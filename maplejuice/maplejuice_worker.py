import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading

NUM_INPUT_LINES = 25

CMD_PORT = 12446
MJ_MANAGER_PORT = 12444


class MapleJuiceWorker:
    def __init__(self, node_id):
        self.cmd_sock = None
        self.node_id = node_id
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
                logging.warning("MapleJuiceWorker received invalid message: " + str(request_json))

    def handle_split_cmd(self, request_json):
        """
        handles all processing for the split phase of the maple command
        """
        logging.debug("Received split command: " + str(request_json))
        while True:
            # TODO == Process everything
            continue
        return

    def handle_map_cmd(self, request_json):
        """
        handles all processing for the map phase of the maple command
        """
        logging.debug("Received map command: " + str(request_json))
        return

    def handle_cmbn_cmd(self, request_json):
        """
        hanldes all processing for the combine phase of the maple command
        """
        logging.debug("Received combine command: " + str(request_json))
        return

    def send_maplejuice_msg(self, master_addr, msg_json):
        """
        Sends a maple/juice command to the master for it to be queued
        """
        if msg_json is None:
            logging.warning("MapleJuice command not sent, message body blank!")

        msg_data = json.dumps(msg_json).encode()
        self.mjman_socket.sendto(msg_data, (master_addr, MJ_MANAGER_PORT))
        logging.info(msg_json['type'] + " command sent to master to be queued")
        return


def split_files_among_machines(file_list, machine_list):
    """
    Takes a list of files and splits each into 25 line chunks, then assigns each
    chunk to a file corresponding to a machine that will process this input block.
    Each machine receives approximately the same number of blocks (i.e. input lines)
    The names of the blocks follow the convention of machineID_block_i, where i is an integer
    """
    logging.debug(file_list)
    logging.debug(machine_list)

    block_cnt = {}  # tracks the number of blocks assigned to a machine
    line_cnt = {}  # tracks the number of lines in the block file currently being constructed
    cur_block = {}  # maps machine -> the file object associated with the current block being constructed
    block_list = {}  # maps machine -> list of file block inputs assigned to this machine

    for machine in machine_list:
        block_cnt[machine] = 1
        line_cnt[machine] = 0
        file_name = machine + "_block_" + str(block_cnt[machine])+".txt"
        cur_block[machine] = open(file_name, "w")
        block_list[machine] = [file_name]

    idx = 0
    machine_cnt = len(machine_list)
    for in_file in file_list:
        logging.debug(in_file)
        with open(in_file, "r") as fp:
            line = fp.readline()
            while line:
                cur_machine = machine_list[idx]
                # insert the line into the current machines block
                cur_block[cur_machine].write(line)
                line_cnt[cur_machine] = line_cnt[cur_machine] + 1
                # check if this block is full
                if line_cnt[cur_machine] > (NUM_INPUT_LINES-1):
                    # close current block, create new one, and add to list
                    cur_block[cur_machine].close()
                    line_cnt[cur_machine] = 0
                    block_cnt[cur_machine] = block_cnt[cur_machine] + 1
                    new_file_name = cur_machine + "_block_" + str(block_cnt[cur_machine])+".txt"
                    cur_block[cur_machine] = open(new_file_name, "w")
                    block_list[cur_machine].append(new_file_name)

                # move to next machine (idx+1%num_machines)
                idx = idx + 1
                idx = idx % machine_cnt
                line = fp.readline()

    # now close all the open blocks that did not fill to NUM_INPUT_LINES
    for open_block in cur_block.values():
        open_block.close()

    logging.debug(block_cnt)
    logging.debug(line_cnt)
    logging.debug(cur_block)

    return block_list
