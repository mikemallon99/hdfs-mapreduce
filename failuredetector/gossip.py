import socket
import random
import time
import math

from .membership_list import MembershipList, Status
from .messages import create_message, MessageType
from .protocol import ProtocolBase, ProtocolType

NUM_SEND_PER_ROUND = 3
APPROX_NUM_NODE = 10


class Gossip(ProtocolBase):
    @staticmethod
    def get_type():
        return ProtocolType.GOSSIP

    @staticmethod
    def process_message(address, message_json, mem_list):
        if message_json["type"] != MessageType.GOSSIP.value:
            return

        for node_id, props in message_json["mem_list"].items():
            # new node joined
            if not mem_list.contains_node(node_id):
                mem_list.add_node(node_id, props["host"], props["port"])
            # this node left group
            elif props["status"] == Status.LEFT.value and mem_list.is_alive(node_id):
                mem_list.mark_left(node_id)
            # only update alive nodes' info
            elif props["status"] == Status.ALIVE.value and mem_list.is_alive(node_id):
                # only update time when received sequence number is larger
                if props["seqnum"] > mem_list.get_seqnum(node_id):
                    mem_list.update_state(
                        node_id, Status.ALIVE, props["seqnum"], time.time()
                    )

    @staticmethod
    def _update_status_and_send_gossip(sender_id, mem_list, sock, seqnum, status, fail_rate=0.0):
        # update my status and sequence number
        mem_list.update_state(sender_id, status, seqnum, time.time())
        # get all other nodes that are alive
        alive_nodes = list(mem_list.get_alive_nodes_not_me(sender_id).items())
        # choose random ones to GOSSIP to
        if len(alive_nodes) <= NUM_SEND_PER_ROUND:
            rand_nodes = alive_nodes
        else:
            rand_nodes = random.sample(alive_nodes, NUM_SEND_PER_ROUND)

        for _, row in rand_nodes:
            server_addr = (row["host"], row["port"])
            message = create_message(MessageType.GOSSIP, mem_list=mem_list,)
            if random.random() >= fail_rate:
                sock.sendto(message, server_addr)

    @staticmethod
    def send_message(sender_id, mem_list, sock, sender_host, sender_port, seqnum, fail_rate=0.0):
        Gossip._update_status_and_send_gossip(
            sender_id, mem_list, sock, seqnum, Status.ALIVE, fail_rate
        )

    @staticmethod
    def send_message_interval(failure_detection_time, dissemination_time) -> float:
        # approximately disseminate to all other nodes within half of dissemination_time
        return dissemination_time / (2 * math.log2(APPROX_NUM_NODE)) / 2

    @staticmethod
    def leave_group(sender_id, mem_list):
        seqnum = mem_list.get_seqnum(sender_id) + 1
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            Gossip._update_status_and_send_gossip(
                sender_id, mem_list, sock, seqnum, Status.LEFT
            )
        mem_list.clear()

    @staticmethod
    def timeout_interval(failure_detection_time, dissemination_time) -> float:
        # safe half of dissemination_time to disseminate and use the rest for timeout
        if failure_detection_time > dissemination_time / 2:
            return dissemination_time / 2
        else:
            return failure_detection_time
