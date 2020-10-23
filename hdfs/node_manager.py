from failuredetector import main as failure_detector
from .master_node import MasterNode
from .slave_node import SlaveNode
import socket
import logging
import json
import threading

fd_cmds = ["join", "list", "id", "leave", "fail"]
dfs_cmds = ["start_sdfs", "master", "put", "get", "delete", "ls"]  # TODO == add more of these

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
        self.master_manager = None
        self.slave_manager = None
        self.is_slave = True
        self.sdfs_init = False

    def start_thread(self, thread_name):
        if not thread_name:
            logging.error("Invalid thread!")
            return
        if thread_name == "wait_for_sdfs_start":
            requested_thread = threading.Thread(target=self.wait_for_sdfs_start)
            requested_thread.start()

    def stop_threads(self):
        if self.master_manager is not None:
            self.master_manager.stop_master()
        if self.slave_manager is not None:
            self.slave_manager.stop_slave()
        logging.info("Stopping all processes from KeyboardInterrupt")

    def process_input(self, command, arguments):
        if command in fd_cmds:
            if not arguments == []:
                logging.warning("Extra arguments for command '"+command+"': "+str(arguments))
            self.fd_manager.handle_user_input(command)
        elif command in dfs_cmds:
            if command == "start_sdfs":
                if self.sdfs_init:
                    logging.warning("SDFS already started! \n Ignoring command...\n")
                    return
                if self.send_sdfs_start():
                    logging.info("SDFS started!")
                else:
                    logging.warning("SDFS not started!")
            if command == "ls":
                if self.is_slave:
                    self.slave_manager.send_ls_to_master(arguments[0])
                else:
                    self.master_manager.retrieve_file_nodes(arguments[0])
            if command == "put":
                self.slave_manager.send_write_request(filename=arguments[0])
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
                        self.slave_manager.start_slave()
                        logging.info("Begin slave node setup...")
                        ack = {'Type': "ACK"}
                        connection.sendall(json_to_bytes(ack))
                        self.sdfs_init = True
            finally:
                logging.debug("Socket closed")
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

        self.master_manager = MasterNode(nodes, socket.gethostname())
        self.master_manager.start_master()
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
        if left:
            logging.debug("Node "+str(node_id)+"has left")
        else:
            logging.debug("Failing node: "+str(node_id))
        if self.sdfs_init:
            if not self.is_slave:
                logging.debug(str(self.master_manager.nodetable))
                self.master_manager.node_failure(node_id)
                logging.debug(str(self.master_manager.nodetable))
            elif node_id == self.slave_manager.master_host:
                logging.debug("Master failed!")
                # TODO == call slave function to elect new master
        else:
            logging.debug("[Node manager] SDFS not initialized, no need to handle failure")
        return
