import argparse
import socket
from enum import Enum
from hdfs.node_manager import NodeManager
import logging
import threading

START_PORT = 12344

node_manager = NodeManager()

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


def cmd_thread():
    """
    listens for user input and executes corresponding command
    """
    print("Text interface with HDFS system: ")
    host_addr = socket.gethostname()
    try:
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
            # TODO == do something with return messages or not?
            node_manager.process_input(cmd, optional_args)
    except KeyboardInterrupt:
        node_manager.stop_threads()


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
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    if args.start is True:
        # start the introducer
        introducer_args = ["--introducer"]
        node_manager.start_failure_detector(introducer_args, [])
    else:
        # start the node as a member
        member_args = ["--introducer-host", args.host, "--introducer-port", args.port]
        node_manager.start_failure_detector([], member_args)

    # start a thread to listen for starting sdfs
    node_manager.start_thread("wait_for_sdfs_start")

    # begin listening for commands
    cmd_thread()




