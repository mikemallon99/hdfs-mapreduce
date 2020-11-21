from failuredetector import main as failure_detector
from .master_node import MasterNode
from .slave_node import SlaveNode
from maplejuice.maplejuice_worker import MapleJuiceWorker
from maplejuice.maplejuice_master import MapleJuiceMaster
import socket
import logging
import json
import threading
import os, time

dfs_cmds = ["start_sdfs", "master", "put", "get", "delete", "ls", "store", "write_test", "read_test"]
fd_cmds = ["join", "list", "id", "leave", "fail"]
mj_cmds = ["maple", "juice"]

# TODO == need to input nodes as just the hostname, not the hostname+time+port

START_PORT = 12344


def json_to_bytes(msg):
    msg_string = json.dumps(msg)
    return msg_string.encode("UTF-8")


def bytes_to_json(b):
    msg_string = b.decode("UTF-8")
    return json.loads(msg_string)


class NodeManager:
    def __init__(self):
        self.fd_manager = failure_detector
        self.mem_list = failure_detector.mem_list
        self.maplejuice_worker = None
        self.master_manager = None
        self.slave_manager = None
        self.is_slave = True
        self.sdfs_init = False
        self.backup_nodetable = {}
        self.backup_filetable = {}

        self.maplejuice_worker = None
        self.maplejuice_master = None

    def start_thread(self, thread_name):
        if not thread_name:
            logging.error("Invalid thread!")
            return
        requested_thread = None
        if thread_name == "wait_for_sdfs_start":
            requested_thread = threading.Thread(target=self.wait_for_sdfs_start)
        if requested_thread is None:
            logging.error("Invalid thread!")
        requested_thread.start()

    def stop_threads(self):
        if not self.is_slave:
            self.master_manager.stop_master()
            self.maplejuice_master.stop_master()
        else:
            self.slave_manager.stop_slave()
            self.maplejuice_worker.stop_worker()

        logging.info("Stopping all processes from KeyboardInterrupt")

    def process_input(self, command, arguments):
        if command in fd_cmds:
            if not arguments == []:
                logging.warning("Extra arguments for command '" + command + "': " + str(arguments))
            self.fd_manager.handle_user_input(command)
        elif command in mj_cmds:
            if not self.sdfs_init:
                logging.warning("SDFS has not been initialized! \n"
                                "Maplejuice command not executed...")
                return
            message_json = format_mj_msgs(command, arguments)
            if message_json is None:
                logging.info("Maplejuice command not executed...")
            else:
                if self.is_slave:
                    master_addr = self.slave_manager.master_host
                else:
                    master_addr = socket.gethostname()
                self.maplejuice_worker.send_maplejuice_msg(master_addr, message_json)
        elif command in dfs_cmds:
            if command == "start_sdfs":
                if self.sdfs_init:
                    logging.warning("SDFS already started! \n Ignoring command...\n")
                    return
                if self.send_sdfs_start():
                    logging.info("SDFS started!")
                else:
                    logging.info("SDFS not started!")
            elif command == "put":
                self.slave_manager.send_write_request(localfilename=arguments[0], sdfsfilename=arguments[1])
            elif command == "get":
                self.slave_manager.send_read_request(localfilename=arguments[1], sdfsfilename=arguments[0])
            elif command == "ls":
                if not arguments:
                    logging.warning("Need argument for ls command (i.e. the name of the file)")
                    return
                if self.is_slave:
                    self.slave_manager.send_ls_to_master(arguments[0])
                else:
                    self.master_manager.retrieve_file_nodes(arguments[0])
            elif command == "delete":
                self.slave_manager.send_delete_request(sdfsfilename=arguments[0])
            elif command == "store":
                self.slave_manager.send_store_request()
            elif command == "write_test":
                self.run_write_tests()
            elif command == "read_test":
                self.run_read_tests()
        else:
            logging.warning("Unknown command entered\n")

    def start_failure_detector(self, introducer_args, member_args):
        if not introducer_args == []:
            self.fd_manager.start_fd(introducer_args)
        else:
            self.fd_manager.start_fd(member_args)
        self.mem_list.set_callback(self.node_failure_callback)

    def wait_for_sdfs_start(self):
        address = (socket.gethostname(), START_PORT)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(address)
            logging.info("\nWaiting for message to start sdfs...")
            try:
                while not self.sdfs_init:
                    sock.listen()
                    connection, address = sock.accept()
                    with connection:
                        try:
                            data = connection.recv(4096)
                        except ConnectionResetError:
                            logging.error("Client connection error")
                        logging.info("Received request from " + str(address))
                        if not data:
                            logging.warning("No data received, not starting sdfs...")
                            continue
                        message = bytes_to_json(data)
                        if not message['Type'] == "START_SDFS":
                            logging.warning("Invalid message")
                        logging.debug("Message received: " + str(message))
                        self.slave_manager = SlaveNode(message["sender_host"], socket.gethostname())
                        self.slave_manager.set_callback(self.master_backup_callback)
                        self.slave_manager.start_slave()
                        logging.info("Begin slave node setup...")
                        ack = {'Type': "ACK"}
                        connection.sendall(json_to_bytes(ack))
                        self.sdfs_init = True

                        nodes = []
                        node_dict = self.mem_list.get_alive_nodes_not_me(my_id=self.fd_manager.get_id())
                        for node in node_dict.keys():
                            nodes.append(node.split(":")[0])
                        self.maplejuice_worker = MapleJuiceWorker(self.fd_manager.get_id().split(":")[0], nodes)
                        self.maplejuice_worker.set_sdfs_write_callback(self.sdfs_write_callback)
                        self.maplejuice_worker.set_sdfs_read_callback(self.sdfs_read_callback)
                        self.maplejuice_worker.start_worker()
            finally:
                logging.debug("SDFS start socket closed")
                connection.close()

    def send_sdfs_start(self):
        if not self.fd_manager.is_in_group():
            logging.warning("Not in group, must join before starting sdfs")
            return False

        node_dict = self.mem_list.get_alive_nodes_not_me(my_id=self.fd_manager.get_id())
        # Start master node
        self.is_slave = False
        nodes = []
        for node in node_dict.keys():
            nodes.append(node.split(":")[0])

        # Initialize self as master
        self.master_manager = MasterNode(nodes, socket.gethostname())
        self.master_manager.start_master()

        # start maplejuice threads/sockets
        self.maplejuice_master = MapleJuiceMaster(socket.gethostname(), nodes)
        self.maplejuice_master.set_filetable_callback(self.get_filetable_callback)
        self.maplejuice_master.start_master()

        logging.debug(node_dict)
        for node in node_dict:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            address = (node.split(":")[0], START_PORT)
            sock.connect(address)
            try:
                message = {'Type': "START_SDFS", "sender_host": socket.gethostname()}
                sock.sendall(json_to_bytes(message))

                while True:
                    data = sock.recv(4096)
                    if not data:
                        continue
                    else:
                        logging.info("Ack received: " + data.decode("UTF-8"))
                        break
            finally:
                logging.debug("All start messages sent, socket closed")
                self.sdfs_init = True
                sock.close()
        return True

    def node_failure_callback(self, node_id, left=False):
        logging.debug("Node manager callback function!")
        node_id = node_id.split(":")[0]
        if left:
            logging.debug("Node " + str(node_id) + " has left")
        else:
            logging.debug("Failing node: " + str(node_id))
        if self.sdfs_init:
            if not self.is_slave:
                logging.debug(str(self.master_manager.nodetable))
                self.master_manager.node_failure(node_id)
                logging.debug(str(self.master_manager.nodetable))
            elif node_id == self.slave_manager.master_host:
                logging.debug("Master failed!")
                # TODO == call slave function to elect new master
                self.elect_new_master()
        else:
            logging.debug("[Node manager] SDFS not initialized, no need to handle failure")
        return

    def master_backup_callback(self, node_table, file_table):
        ret_msg = "Backup Failed!"
        if not node_table or not file_table:
            logging.warning("Node table and/or file table not received!")
        else:
            self.backup_filetable = file_table
            self.backup_nodetable = node_table
            ret_msg = "Backup Successful!"
        logging.debug("Received node table: " + str(node_table))
        logging.debug("Received file table: " + str(file_table))
        return ret_msg

    def elect_new_master(self):
        logging.debug("Starting process to elect the new master...")
        new_master_id = self.mem_list.get_most_recent_node()
        logging.info(f"Selected node: {new_master_id}")
        if not new_master_id:
            logging.error("Failed to find new master!")
        new_master_ip = new_master_id.split(":")[0]
        if new_master_ip == self.slave_manager.self_host:
            self.slave_manager.stop_slave()
            self.slave_manager = None
            self.is_slave = False
            self.master_manager = MasterNode(self.mem_list.get_alive_nodes_ip_not_me(), socket.gethostname())

            self.master_manager.start_master()
            self.master_manager.set_filenode_tables(self.backup_filetable, self.backup_nodetable)
            logging.info("Changed from slave to Master")
        else:
            self.slave_manager.update_new_master(new_master_ip)
        return

    def sdfs_write_callback(self, filename):
        """
        Send a write request and do not terminate until the request is completed
        """
        self.slave_manager.send_write_request(filename, filename)
        while self.slave_manager.get_writes_queued() > 0:
            continue

    def sdfs_read_callback(self, filename):
        """
        Send a read request and do not terminate until the request is completed
        """
        self.slave_manager.send_read_request(filename, filename)
        while self.slave_manager.get_reads_queued() > 0:
            continue

    def get_filetable_callback(self):
        return self.master_manager.get_filetable()

    def run_write_tests(self):
        for i in range(0, 10):
            # for j in range(0,5):
            os.system(f"dd if=/dev/urandom of=hdfs_files/filename{i} bs=$((1024*1024)) count={str(1 + i * 100)}")
            self.slave_manager.send_write_request(localfilename=f"filename{i}", sdfsfilename=f"file{i}")
            time.sleep(0.5)

    def run_read_tests(self):
        for i in range(0, 10):
            for j in range(0, 5):
                # os.system(f"dd if=/dev/urandom of=hdfs_files/filename{i} bs=$((1024*1024)) count={str(1+i*100)}")
                self.slave_manager.send_read_request(localfilename=f"filename{i}", sdfsfilename=f"file{i}")
                time.sleep(0.5)


def format_mj_msgs(m_type, cli_args):
    req = {'type': m_type}

    # format the maple request message
    if m_type == "maple":
        if len(cli_args) is not 4:
            logging.warning("Invalid number of arguments for "+m_type+" command")
            return None
        req['maple_exe'] = cli_args[0]
        req['num_maples'] = cli_args[1]
        req['file_prefix'] = cli_args[2]
        req['sdfs_src_directory'] = cli_args[3]
    # format the juice request message
    else:
        if len(cli_args) is not 5:
            logging.warning("Invalid number of arguments for " + m_type + " command")
            return None
        req['juice_exe'] = cli_args[0]
        req['num_juices'] = cli_args[1]
        req['file_prefix'] = cli_args[2]
        req['sdfs_dest_filename'] = cli_args[3]
        req['delete_input'] = cli_args[4]

    req['sender_host'] = socket.gethostname()

    return req
