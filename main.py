import argparse
import socket
from enum import Enum
from failuredetector import main as failure_detector
import logging

sdfs_init = False

fd_cmds = ["join", "list", "id", "leave", "fail"]
dfs_cmds = ["start_sdfs", "master"]  # TODO == add more of these


class CommandType(Enum):
    LIST = "list"
    JOIN = "join"
    DISP_ID = "id"
    LEAVE = "leave"
    FAIL = "fail"
    START_SDFS = "start_sdfs"
    DISP_MASTER = "master"


def handle_sdfs_input(command, arguments):
    global sdfs_init
    ret_msg = "Invalid Command"

    if command == CommandType.START_SDFS:
        print("Here")
        if sdfs_init:
            logging.warning("SDFS already started!")
        else:
            sdfs_init = True
            # start master node on this machine
            # send message to all machines to start as slave
            # TODO == fd adds members using socket.gethostname by default, change this?
            print(socket.gethostname())
            logging.info("SDFS started!")
    return ret_msg


def cmd_thread():
    """
    listens for user input and executes corresponding command
    """
    print("Text interface with HDFS system: ")
    host_ip = socket.gethostname()
    while True:
        u_input = input(f"<{host_ip.split('.')[0]} /> ")

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
        print(u_input)
        print(split_args)
        print(cmd)
        print(optional_args)
        # TODO == do something with the return messages or not?
        if cmd in fd_cmds:
            if optional_args is not []:
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

    # begin thread to listen for commands
    cmd_thread()




