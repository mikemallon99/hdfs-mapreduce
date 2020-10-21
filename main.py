import argparse
import failuredetector.main as failure_detector


def parse_args():
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



