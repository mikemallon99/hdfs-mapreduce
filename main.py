import argparse
import socket
from enum import Enum
from failuredetector import main as failure_detector
import logging
import threading
from hdfs.master_node import MasterNode

sdfs_init = False

fd_cmds = ["join", "list", "id", "leave", "fail"]
dfs_cmds = ["start_sdfs", "master"]  # TODO == add more of these

START_PORT = 12344

# TODO == Need to ensure that when a node joins the network, and sdfs is on, it will join as slave
# TODO == Need to ensure that when a node leaves the newtwork, and sdfs is on, it will stop its sdfs

class CommandType(Enum):
    LIST = "list"
    JOIN = "join"
    DISP_ID = "id"
    LEAVE = "leave"
    FAIL = "fail"
    START_SDFS = "start_sdfs"
    DISP_MASTER = "master"


def send_start_sdfs(node_list):
    for node in node_list:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = (node, START_PORT)
        sock.connect(address)

        try:
            message = "START_SDFS"  # TODO == determine the message format
            sock.sendall(message.encode("UTF-8"))

            while True:
                data = sock.recv(4096)
                if not data:
                    continue
                else:
                    logging.info("Ack received: "+data.decode("UTF-8"))
                    break
        finally:
            sock.close()


# TODO == move this stuff to master_node.py?
def master_thread():
    slaves_dict = failure_detector.mem_list.get_alive_nodes_not_me(my_id=failure_detector.get_id())
    slaves_list = []
    for node in slaves_dict:
        slaves_list.append(node.split(":")[0])
    print(socket.gethostname())
    masternode = MasterNode(nodes=slaves_list, node_ip=socket.gethostname())
    print(slaves_list)
    send_start_sdfs(slaves_list)
    # TODO == begin master threads


# TODO == move this stuff to slave_node.py?
def wait_for_sdfs_start_thread():
    global sdfs_init
    address = (socket.gethostname(), START_PORT)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(address)
        logging.info("Waiting for message to start sdfs...")
        try:
            while not sdfs_init:
                sock.listen()
                connection, address = sock.accept()
                with connection:
                    try:
                        data = connection.recv(4096)
                    except ConnectionResetError:
                        logging.error("Client connection error")
                    logging.info("Received request from "+str(address)+": {0}".format(data))
                    if not data:
                        logging.warning("No data received, not starting sdfs...")

                    logging.debug("Message received: "+data.decode("UTF-8"))
                    # TODO == instantiate slave and begin threads
                    # Note: the 'address' of the sender will be the master
                    logging.info("Begin slave node setup...")
                    ack = "Received"
                    connection.sendall(ack.encode("UTF-8"))
                    sdfs_init = True
        finally:
            connection.close()


def handle_sdfs_input(cmd, arguments):
    global sdfs_init
    ret_msg = "Invalid Command"
    command = CommandType(cmd)
    if command == CommandType.START_SDFS:
        if sdfs_init:
            logging.warning("SDFS already started!")
        elif failure_detector.is_in_group():
            sdfs_init = True
            # start master node on this machine
            master_thread()
            # TODO == fd adds members using socket.gethostname by default, which is the fa20-...
            logging.info("SDFS started!")
        else:
            logging.warning("Not in group, must join before starting sdfs")
    return ret_msg


def cmd_thread():
    """
    listens for user input and executes corresponding command
    """
    print("Text interface with HDFS system: ")
    host_addr = socket.gethostname()
    while True:
        u_input = input(f"<{host_addr.split('.')[0]} /> ")

        # check if input empty
        if u_input is not "":
            split_args = u_input.split()
        else:
            continue

        # split the input into command + arguments
        cmd = split_args[0]
        optional_args = []
        if len(split_args) > 1:
            optional_args = split_args[1:]

        cmd_ret = None
        # TODO == do something with the return messages or not?
        if cmd in fd_cmds:
            if not optional_args == []:
                logging.warning("Extra arguments for command: "+cmd)
            failure_detector.handle_user_input(cmd)
        elif cmd in dfs_cmds:
            handle_sdfs_input(cmd, optional_args)


def parse_args():
    """
    parse the CLI arguments
    """
    description = '''
                Simple implementation of a Distributed Filesystem
                '''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--start", default=False, action="store_true", help="Starts the introducer node of the network")
    parser.add_argument("--host", help="Introducer's host name, required if this node is not an introducer.")
    parser.add_argument("--port", help="Introducer's port number, required if this node is not an introducer.")

    p_args = parser.parse_args()

    if p_args.start is False:
        if p_args.host is None or p_args.port is None:
            parser.error('Introducer host/port not specified, unable to join network')
    return p_args


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    if args.start is True:
        # start the introducer
        introducer_args = ["--introducer"]
        failure_detector.start_fd(introducer_args)
    else:
        # start the node as a member
        member_args = ["--introducer-host", args.host, "--introducer-port", args.port]
        failure_detector.start_fd(member_args)

    # begin thread to listen for starting sdfs
    start_sdfs_t = threading.Thread(target=wait_for_sdfs_start_thread)
    start_sdfs_t.start()

    # begin listening for commands
    cmd_thread()




