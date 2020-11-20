import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading


CMD_PORT = 12446
MJ_MANAGER_PORT = 12444


class MapleJuiceWorker:
    def __init__(self, node_id):
        self.cmd_sock = None
        self.node_id = node_id
        self.cur_phase = None
        self.fd_lock = threading.Lock()
        self.failed_node = None
        self.mjman_socket = None

    def start_worker(self):
        """
        Starts necessary threads and initializes ports for messages
        """
        logging.debug("MapleJuicer worker started!")

        cmd_listen_t = threading.Thread(target=self.cmd_listen_thread)
        cmd_listen_t.start()

        self.mjman_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def stop_worker(self):
        """
        Closes sockets relevant to maplejuice
        """
        if self.mjman_socket is not None:
            self.mjman_socket.close()
        if self.cmd_sock is not None:
            self.cmd_sock.close()
        logging.info("Stopping MJ worker sockets")

    def cmd_listen_thread(self):
        """
        Listen for maplejuice messages, start a thread to handle the message type accordingly
        """

        self.cmd_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.cmd_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cmd_sock.bind((socket.gethostname(), CMD_PORT))

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
        """
        handles all processing for the split phase of the maple command
        """
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
        """
        handles all processing for the map phase of the maple command
        """
        self.cur_phase = "map"
        logging.debug("Received map command: " + str(request_json))
        self.cur_phase = None
        return

    def handle_cmbn_cmd(self, request_json):
        """
        hanldes all processing for the combine phase of the maple command
        """
        self.cur_phase = "cmbn"
        logging.debug("Received combine command: " + str(request_json))
        self.cur_phase = None
        return

    def node_failure_callback(self, failed_node):
        """
        Gets called whenever a node fails
        """
        logging.debug("Failure detected at node "+str(failed_node))
        if self.cur_phase is None or self.cur_phase is not "split":
            logging.debug("Worker not in split phase, do nothing")
            # Node failures during map/combine phase handled by master, only need to do something during split
            return
        else:
            # Acquire lock to notify worker that a node has failed and rebalancing is needed
            self.fd_lock.acquire()
            self.failed_node = failed_node
            self.fd_lock.release()

    def send_maplejuice_msg(self, master_addr, msg_json):
        """
        Sends a maple/juice command to the master for it to be queued
        """
        if msg_json is None:
            logging.warning("MapleJuice command not sent, message body blank!")

        msg_data = json.dumps(msg_json).encode()
        self.mjman_socket.sendto(msg_data, (master_addr, MJ_MANAGER_PORT))
        logging.info(msg_json['type']+" command sent to master to be queued")
        return
