import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict

MJ_MANAGER_PORT = 12444
MJ_HANDLER_PORT = 12445

class MapleJuiceMaster:
    def __init__(self, node_ip):
        self.acktable = {}
        self.op_queue = []
        self.queue_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.node_ip = node_ip

        self.qman_sock = None
        self.qhan_sock = None

    def enqueue_maple(self, request):
        self.queue_lock.acquire()
        self.op_queue.append(request)
        self.queue_lock.release()

    def enqueue_juice(self, request):
        self.queue_lock.acquire()
        self.op_queue.append(request)
        self.queue_lock.release()

    def mj_manager_thread(self):
        """
        Listen for messages from group members. Update membership list accordingly.
        """
        self.qman_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.qman_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.qman_sock.bind((self.node_ip, MJ_MANAGER_PORT))

        while True:
            data, address = self.qman_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
            if request_json is None:
                # The data received is not valid
                logging.info(f"Recieved a request")
                continue

            # Enqueue the data
            if request_json['type'] == 'maple':
                self.enqueue_maple(request_json)
                logging.info(f"Recieved maple request from {request_json['sender_host']}")
            elif request_json['op'] == 'juice':
                self.enqueue_juice(request_json)
                logging.info(f"Recieved juice request from {request_json['sender_host']}")
            else:
                logging.info(f"Recieved a request from {request_json['sender_host']}


def parse_and_validate_message(byte_data: bytes) -> Optional[Dict]:
    """
    Parse received byte data into json. Check if all required fields are present.
    :return: None if a required field is missing or failed to parse JSON. Otherwise the parsed dict.
    """
    str_data = byte_data.decode("utf-8")
    try:
        dict_data = json.loads(str_data)
    except ValueError:
        logging.warn(f"Failed to decode json: {str_data}")
        return None

    return dict_data