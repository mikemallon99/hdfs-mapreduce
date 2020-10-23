from failuredetector import main as failure_detector
import socket
import logging
import json
import threading

fd_cmds = ["join", "list", "id", "leave", "fail"]
dfs_cmds = ["start_sdfs", "master"]  # TODO == add more of these

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
                self.send_sdfs_start()
                logging.info("SDFS started!")
        else:
            logging.warning("Unknown command entered\n")

    def start_failure_detector(self, introducer_args, member_args):
        if not introducer_args == []:
            self.fd_manager.start_fd(introducer_args)
        else:
            self.fd_manager.start_fd(member_args)

    def wait_for_sdfs_start(self):
        address = (socket.gethostname(), START_PORT)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
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
                        # TODO == instantiate slave and begin threads
                        # Note: the 'address' of the sender will be the master
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
            return

        node_dict = self.mem_list.get_alive_nodes_not_me(my_id=self.fd_manager.get_id())
        for node in node_dict:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            address = (node.split(":")[0], START_PORT)
            sock.connect(address)
            try:
                message = {'Type': "START_SDFS"}
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
