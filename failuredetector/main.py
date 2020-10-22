from enum import Enum
import threading
import argparse
import socket
import signal
import time
import sys
import os
import logging

from .gossip import Gossip
from .all2all import All2All
from .utils import get_host_number_id, get_hostname
from .protocol import ProtocolBase, ProtocolType
from .utils import generate_id, get_hostname
from .messages import parse_and_validate_message
from .membership_list import MembershipList

protocol = None
mem_list = MembershipList()
self_id = None
in_group = False


def listen_thread(server_ip, port, is_introducer):
    """
    Listen for messages from group members. Update membership list accordingly.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((server_ip, int(port)))

    while True:
        data, address = sock.recvfrom(4096)
        request_json = parse_and_validate_message(data)
        if request_json is None:
            # The data received is not valid
            continue

        if is_introducer:
            protocol.process_join_request(address, request_json, mem_list, sock)

        if not in_group:
            continue

        protocol.process_message(address, request_json, mem_list)


def send_thread(
    self_host, self_port, failure_detection_time, dissemination_time, failure_rate
):
    """
    Send messages to group members periodically.
    """
    # "static" sequence number for this function
    send_thread.seqnum = 0

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        while True:
            if in_group:
                protocol.send_message(
                    self_id,
                    mem_list,
                    sock,
                    self_host,
                    self_port,
                    send_thread.seqnum,
                    failure_rate,
                )

            interval = protocol.send_message_interval(
                failure_detection_time, dissemination_time
            )
            time.sleep(interval)
            send_thread.seqnum += 1


class CommandType(Enum):
    SWITCH = "switch"
    MEMBERSHIP_LIST = "list"
    DISPLAY_SELF_ID = "id"
    JOIN = "join"
    LEAVE = "leave"
    FAIL = "fail"


def user_interact_thread(args):
    """
    Take user commands and take action accordingly.
    """
    global protocol, in_group, self_id
    id_num = get_host_number_id(get_hostname())
    while True:
        user_input = input(f"[{id_num}-{protocol.get_type().value}] Enter command: ")
        try:
            command = CommandType(user_input)
            if command == CommandType.SWITCH:
                if protocol.get_type() == ProtocolType.GOSSIP:
                    logging.info("Switching from Gossip to A2A")
                    protocol = All2All
                elif protocol.get_type() == ProtocolType.ALL2ALL:
                    logging.info("Switching from A2A to Gossip")
                    protocol = Gossip
                else:
                    logging.critical("nani????????????")
            elif command == CommandType.MEMBERSHIP_LIST:
                logging.info(mem_list)
            elif command == CommandType.DISPLAY_SELF_ID:
                logging.info(self_id)
            elif command == CommandType.JOIN:
                if in_group:
                    logging.warn("Already in group...")
                else:
                    self_id = generate_id(args.host, args.port)
                    logging.info(
                        f"Joining group at {args.introducer_host}:{args.introducer_port}"
                    )
                    protocol.join_group(
                        mem_list,
                        self_id,
                        args.introducer_host,
                        args.introducer_port,
                        args.host,
                        args.port,
                    )
                    in_group = True
            elif command == CommandType.LEAVE:
                if not in_group:
                    logging.warn("Not in group, cannot leave......")
                elif args.introducer:
                    logging.warn("Introducer cannot leave group :(")
                else:
                    logging.info("Leaving group")
                    in_group = False
                    protocol.leave_group(self_id, mem_list)
                    self_id = None
            elif command == CommandType.FAIL:
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                logging.warn("Unkown command")

        except ValueError as e:
            all_comamnds = ", ".join([c.value for c in CommandType])
            logging.warn(f"Invalid command. Please enter one of: {all_comamnds}")


def update_peer_status_thread(failure_detection_time, dissemination_time):
    """
    Periodically go through membership list and mark nodes that timed-out as failed.
    """
    while True:
        if not in_group:
            continue

        for node_id, row in mem_list:
            if node_id == self_id or not mem_list.is_alive(node_id):
                continue

            last_recv_hb_time = row.timestamp
            delta = time.time() - last_recv_hb_time

            if delta > failure_detection_time:
                mem_list.mark_failed(node_id)

        interval = protocol.timeout_interval(failure_detection_time, dissemination_time)
        time.sleep(interval)


def start_daemon_thread(target_func, *args):
    thread = threading.Thread(target=target_func, args=args)
    thread.daemon = True
    thread.start()
    return thread


def parse_args(args):
    parser = argparse.ArgumentParser(description="Simple implementation of membership list of distributed system that use either gossip or all-to-all heartbeat.")
    parser.add_argument("--host", default=get_hostname(), help="Host name of this node.")
    parser.add_argument("--port", default=3256, help="Port number to host service on.")
    parser.add_argument(
        "--failure-detection-time",
        default=5,
        help="When failure happens, within this time at least one machine detects it.",
    )
    parser.add_argument(
        "--dissemination-time",
        default=6,
        help="When failure/join/leave happens, every node should update their membership list within this time.",
    )

    parser.add_argument(
        "--protocol", default="ALL2ALL", help="One of 'GOSSIP' or 'ALL2ALL'."
    )
    parser.add_argument("--introducer", default=False, action="store_true", help="Whether this node is an introducer.")
    parser.add_argument("--introducer-host", help="Introducer's host name, required if this node is not an introducer.")
    parser.add_argument("--introducer-port", help="Introducer's port number, required if this node is not an introducer.")
    parser.add_argument("--message-failure-rate", default=0, help="Rate of dropping message before sending. A float between 0 and 1. Simulate network package drops.")

    args = parser.parse_args(args)
    if args.introducer is False:
        if args.introducer_host is None or args.introducer_port is None:
            parser.error(
                "--introducer-host/port must be set if the node is not an introducer"
            )

    if not ProtocolType.has_value(args.protocol):
        parser.error(
            f"--protocol must be one of 'GOSSIP' or 'ALL2ALL', received '{args.protocol}'"
        )

    args.port = int(args.port)
    if args.introducer_port is not None:
        args.introducer_port = int(args.introducer_port)

    args.message_failure_rate = float(args.message_failure_rate)

    return args


def handle_user_input(cmd_type):
    """
    Take user commands and take action accordingly.
    """
    global protocol, in_group, self_id

    # Interpret the command
    ret_msg = 'Error, command not found'
    command = cmd_type
    print(command)
    if command == 'switch':
        if protocol.get_type() == ProtocolType.GOSSIP:
            logging.info("Switching from Gossip to A2A")
            ret_msg = "Switching from Gossip to A2A"
            protocol = All2All
        elif protocol.get_type() == ProtocolType.ALL2ALL:
            logging.info("Switching from A2A to Gossip")
            ret_msg = "Switching from A2A to Gossip"
            protocol = Gossip
        else:
            logging.critical("nani????????????")
    elif command == 'list':
        logging.info(mem_list)
        ret_msg = mem_list
    elif command == 'id':
        logging.info(self_id)
        ret_msg = self_id
    elif command == 'leave':
        if not in_group:
            logging.warn("Not in group, cannot leave......")
        else:
            logging.info("Leaving group")
            in_group = False
            protocol.leave_group(self_id, mem_list)
            self_id = None
    elif command == 'fail':
        os.kill(os.getpid(), signal.SIGTERM)
    else:
        logging.warn("Unkown command")

    return ret_msg


def start_fd(args):
    global protocol, in_group, self_id, mem_list
    parsed_args = parse_args(args)

    logging.basicConfig(level=logging.INFO)

    if ProtocolType(parsed_args.protocol) == ProtocolType.GOSSIP:
        protocol = Gossip
    else:
        protocol = All2All

    in_group = parsed_args.introducer
    print(parsed_args.port)
    if parsed_args.introducer:
        self_id = generate_id(parsed_args.host, parsed_args.port)
        mem_list.add_node(self_id, parsed_args.host, parsed_args.port)

    # start threads
    start_daemon_thread(listen_thread, parsed_args.host, parsed_args.port, parsed_args.introducer)
    start_daemon_thread(
        send_thread,
        parsed_args.host,
        parsed_args.port,
        parsed_args.failure_detection_time,
        parsed_args.dissemination_time,
        parsed_args.message_failure_rate,
    )
    start_daemon_thread(
        update_peer_status_thread, parsed_args.failure_detection_time, parsed_args.dissemination_time
    )
    return


if __name__ == "__main__":
    start_fd(sys.argv[1:])
