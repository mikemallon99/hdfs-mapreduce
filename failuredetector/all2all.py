import socket
import random
import logging
import time

from .membership_list import MembershipList, Status
from .messages import create_message, MessageType
from .protocol import ProtocolBase, ProtocolType


class All2All(ProtocolBase):
    @staticmethod
    def get_type():
        return ProtocolType.ALL2ALL

    @staticmethod
    def process_message(address, message_json, mem_list):
        message_type = message_json["type"]

        if message_type == MessageType.HEARTBEAT.value:
            sender_id = message_json["sender_id"]
            seqnum = message_json["seqnum"]
            sender_host = message_json["sender_host"]
            sender_port = message_json["sender_port"]

            if mem_list.contains_node(sender_id):
                # do not revive a failed node
                if mem_list.is_alive(sender_id):
                    mem_list.update_state(sender_id, Status.ALIVE, seqnum, time.time())
            else:
                mem_list.add_node(sender_id, sender_host, sender_port)

        elif message_type == MessageType.LEAVE.value:
            sender_id = message_json["sender_id"]
            mem_list.mark_left(sender_id)

    @staticmethod
    def send_message(sender_id, mem_list, sock, sender_host, sender_port, seqnum, fail_rate=0.0):
        for node_id, row in mem_list:
            if node_id == sender_id or not mem_list.is_alive(node_id):
                continue

            server_addr = (row.host, row.port)
            message = create_message(
                MessageType.HEARTBEAT,
                sender_id=sender_id,
                seqnum=seqnum,
                sender_host=sender_host,
                sender_port=sender_port,
            )

            if random.random() >= fail_rate:
                try:
                    sock.sendto(message, server_addr)
                except socket.gaierror:
                    logging.info(f"ERROR: Sending message to {server_addr}")

    @staticmethod
    def send_message_interval(failure_detection_time, dissemination_time) -> float:
        # Send to all other nodes within half of dissemination_time
        return dissemination_time / 2

    @staticmethod
    def leave_group(sender_id, mem_list):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = create_message(MessageType.LEAVE, sender_id=sender_id)
            for node_id, row in mem_list:
                if node_id == sender_id or not mem_list.is_alive(node_id):
                    continue

                server_addr = (row.host, row.port)
                sock.sendto(message, server_addr)

            mem_list.clear()

    @staticmethod
    def timeout_interval(failure_detection_time, dissemination_time) -> float:
        return failure_detection_time
