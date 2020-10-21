from enum import Enum
import time
import socket

from messages import MessageType, create_message, parse_and_validate_message


class ProtocolType(Enum):
    ALL2ALL = "ALL2ALL"
    GOSSIP = "GOSSIP"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ProtocolBase:
    @staticmethod
    def get_type() -> ProtocolType:
        """
        Return the type of the protocol.
        """
        raise Exception("Did not implement get_type")

    @staticmethod
    def process_message(address, message_json, mem_list):
        """
        Process recevied message by updating membership list.
        :param address: address of sender of this message
        :param meesage_json: parsed json (dictionary) object of the received message
        :param mem_list: current membership list
        """
        raise Exception("Did not implement process_message")

    @staticmethod
    def process_join_request(sender_addr, request_json, mem_list, socket):
        """
        If the request is JOIN, add this node to our membership list and send
        the membership list to this node.
        This should only be called if the node is an introducer.

        :param sender_addr: The address of node that sent the JOIN request
        :param request_json: The request json object received from a node that wants to join
        :param mem_list: The current membership list
        :param socket: The socket used to send messages
        """
        if request_json["type"] != MessageType.JOIN.value:
            return

        # Add new node to our membership list
        new_node_id = request_json["sender_id"]
        sender_host = request_json["sender_host"]
        sender_port = request_json["sender_port"]
        mem_list.add_node(new_node_id, sender_host, sender_port)

        # Send JOIN_RES to this node
        response = create_message(MessageType.JOIN_RES, mem_list=mem_list)
        socket.sendto(response, sender_addr)

    @staticmethod
    def send_message(sender_id, mem_list, sock, sender_host, sender_port, seqnum, fail_rate=0.0):
        """
        Send heartbeat message to other members based on protocol.
        :param sender_id: ID of the sender node
        :param sender_host: hostname of the sender node
        :param sender_port: port the sender node is listening on
        :param mem_list: current membership list
        :param sock: currently open UDP socket for writing
        :param seqnum: current sequence number
        """
        raise Exception("Did not implement send_message")

    @staticmethod
    def send_message_interval(failure_detection_time, dissemination_time) -> float:
        """
        Calculate interval to send heartbeat message based on protocol.
        :param failure_detection_time: When failure happens, within this time at least one machine detects it.
        :param dissemination_time: When failure/join/leave happens, every node should update their membership list within this time.
        :return: interval to send message in seconds
        """
        raise Exception("Did not implement send_message_interval")

    @staticmethod
    def join_group(
        mem_list, sender_id, introducer_host, introducer_port, sender_host, sender_port
    ):
        """
        Join the group by contacting the introducer. Update membership list with what introducer provided.
        :param mem_list: empty membership list to fill in
        :param sender_id: ID of the node that is sending the JOIN message
        :param introducer_host: introducer host name
        :param introducer_port: introducer port number
        :param sender_host : hostname of the node that is sending the JOIN message
        :param sender_port: port that the node that is sending the JOIN message
        """
        if introducer_host is None or introducer_port is None:
            raise Exception("Cannot join group without introducer's host and prot")

        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                server_addr = (introducer_host, introducer_port)
                message = create_message(
                    MessageType.JOIN,
                    sender_id=sender_id,
                    sender_host=sender_host,
                    sender_port=sender_port,
                )
                sock.sendto(message, server_addr)

                data, address = sock.recvfrom(4096)
                res_json = parse_and_validate_message(data)

                if (
                    res_json is not None
                    and res_json["type"] == MessageType.JOIN_RES.value
                ):
                    new_mem_list_dict = res_json["mem_list"]
                    mem_list.update_from_dict(new_mem_list_dict)
                    break

            # Sleep a bit and retry joining
            time.sleep(0.5)

    @staticmethod
    def leave_group(sender_id, mem_list):
        """
        Leave the group by sending messages based on protocol. Clear membership list.
        :param sender_id: ID of the node sending the LEAVE message
        :param mem_list: current membership list
        """
        raise Exception("Did not implement leave_group")

    @staticmethod
    def timeout_interval(failure_detection_time, dissemination_time) -> float:
        """
        Calculate interval to go through membership list and mark peers as failed.
        :param failure_detection_time: When failure happens, within this time at least one machine detects it.
        :param dissemination_time: When failure/join/leave happens, every node should update their membership list within this time.
        :return: interval to update peers status in seconds
        """
        raise Exception("Did not implement timeout_interval")
