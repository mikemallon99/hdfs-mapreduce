import argparse
import socket
from enum import Enum
import sdfs_client
from failuredetector import main as failure_detector


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


def cmd_thread():
    """
    listens for user input and executes corresponding command
    """
    print("Text interface with HDFS system: ")
    host_ip = socket.gethostname()
    while True:
        u_input = input(f"<{host_ip.split('.')[0]} />")
        cmd_ret = None
        if u_input in fd_cmds:
            cmd_ret = failure_detector.handle_user_input(u_input)
        elif u_input in dfs_cmds:
            cmd_ret = sdfs_client.handle_user_input(u_input)
        print(cmd_ret)


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




