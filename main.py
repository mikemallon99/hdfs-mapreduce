import argparse
import socket
from enum import Enum
from failuredetector import main as failure_detector


class CommandType(Enum):
    MEMBERSHIP_LIST = "list"
    DISPLAY_SELF_ID = "id"
    JOIN = "join"
    LEAVE = "leave"
    FAIL = "fail"


def cmd_thread():
    """
    listens for user input and executes corresponding command
    """
    print("Text interface with HDFS system: ")
    host_ip = socket.gethostname()
    while True:
        u_input = input(f"<{host_ip} />")
        try:
            print(u_input)
        except ValueError as e:
            print("Invalid command entered!")



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
    print(args)
    if args.start is True:
        # start the introducer
        introducer_args = ["--introducer"]
        print(introducer_args)
        failure_detector.start_fd(introducer_args)
    else:
        # start the node as a member
        member_args = ["--introducer-host", args.host, "--introducer-port", args.port]
        print(member_args)
        failure_detector.start_fd(member_args)

    # begin thread to listen for commands
    cmd_thread()




