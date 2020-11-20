import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading


CMD_PORT = 12446


class MapleJuiceWorker:
    def __init__(self, node_addr):
        self.cmd_sock = None
        self.node_addr = node_addr
        self.cur_phase = None
        self.fd_lock = threading.Lock()
        self.failed_node = None

    def cmd_listen_thread(self):
        """
                Listen for messages from group members. Update membership list accordingly.
                """
        self.cmd_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.cmd_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cmd_sock.bind((self.node_addr, CMD_PORT))

        while True:
            data, address = self.cmd_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
            if request_json is None:
                # The data received is not valid
                logging.info(f"Recieved a request")
                continue
            cmd_type = request_json['type']
            if cmd_type == "split":
                split_t = threading.Thread(target=self.handle_split_cmd, args=(request_json,))
                split_t.start()
            elif cmd_type == "map":
                map_t = threading.Thread(target=self.handle_map_cmd, args=(request_json,))
                map_t.start()
            elif cmd_type == "combine":
                cmbn_t = threading.Thread(target=self.handle_cmbn_cmd, args=(request_json,))
                cmbn_t.start()
            else:
                logging.warning("MapleJuiceWorker received invalid message: "+str(request_json))

    def handle_split_cmd(self, request_json):
        logging.debug("Received split command: "+str(request_json))
        self.cur_phase = "split"
        while True:
            self.fd_lock.acquire()
            if self.failed_node is not None:
                # TODO == handle rebalancing
                break
            self.failed_node = None
            self.fd_lock.release()
            # TODO == actually process the split
        self.cur_phase = None
        return

    def handle_map_cmd(self, request_json):
        self.cur_phase = "map"
        logging.debug("Received map command: " + str(request_json))
        self.cur_phase = None
        return

    def handle_cmbn_cmd(self, request_json):
        self.cur_phase = "cmbn"
        logging.debug("Received combine command: " + str(request_json))
        self.cur_phase = None
        return

    def node_failure_callback(self, failed_node):
        logging.debug("Failure detected at node "+str(failed_node))
        if self.cur_phase is None or self.cur_phase is not "split":
            logging.debug("Worker not in split phase, ")
            # Node failures during map/combine phase handled by master, only need to do something during split
            return
        else:
            # Acquire lock to notify worker that a node has failed and rebalancing is needed
            self.fd_lock.acquire()
            self.failed_node = failed_node
            self.fd_lock.release()
