import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading
import random

NUM_INPUT_LINES = 25

CMD_PORT = 12446
MJ_MANAGER_PORT = 12444
MJ_HANDLER_PORT = 12445


class MapleJuiceWorker:
    def __init__(self, node_id, nodes):
        self.cmd_sock = None
        self.node_id = node_id
        self.fd_lock = threading.Lock()
        self.failed_node = None
        self.mjman_socket = None
        self.nodes = nodes
        self.nodes.append(self.node_id)

        self.sdfs_write_callback = None
        self.sdfs_read_callback = None
        self.sdfs_delete_callback = None

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
        self.cmd_sock.bind((socket.gethostname(), MJ_HANDLER_PORT))

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
            elif cmd_type == "maple":
                map_t = threading.Thread(target=self.handle_map_cmd, args=(request_json,))
                map_t.start()
            elif cmd_type == "map_combine":
                cmbn_t = threading.Thread(target=self.handle_cmbn_cmd, args=(request_json,))
                cmbn_t.start()
            elif cmd_type == "juice_split":
                juice_split_t = threading.Thread(target=self.handle_juice_split_cmd, args=(request_json,))
                juice_split_t.start()
            elif cmd_type == "juice":
                juice_t = threading.Thread(target=self.handle_juice_cmd, args=(request_json,))
                juice_t.start()
            elif cmd_type == "juice_combine":
                cmbn_juice_t = threading.Thread(target=self.handle_juice_cmbn_cmd, args=(request_json,))
                cmbn_juice_t.start()
            else:
                logging.warning("MapleJuiceWorker received invalid message: " + str(request_json))

    def set_sdfs_write_callback(self, func):
        self.sdfs_write_callback = func

    def set_sdfs_read_callback(self, func):
        self.sdfs_read_callback = func

    def set_sdfs_delete_callback(self, func):
        self.sdfs_delete_callback = func

    def handle_split_cmd(self, request_json):
        """
        handles all processing for the split phase of the maple command
        """
        logging.debug("Received split command: "+str(request_json))
        master_node = request_json['sender_host']
        num_maples = int(request_json['num_maples'])
        file_list = request_json['file_list']

        # Call split function here
        random_nodes = select_random_machines(self.nodes, num_maples)
        split_file_dict = split_files_among_machines(file_list, random_nodes)
        # Add files to sdfs here
        for node in split_file_dict.keys():
            for file in split_file_dict[node]:
                self.sdfs_write_callback(file)

        # Send ack to master
        response = {}
        response['type'] = 'split_ack'
        response['node_ips'] = split_file_dict
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent split_ack to master with list: {split_file_dict}")

        return

    def handle_juice_split_cmd(self, request_json):
        """
        handles all processing for the split phase of the juice command
        """
        logging.debug("Received juice split command: "+str(request_json))
        master_node = request_json['sender_host']
        num_juices = int(request_json['num_juices'])
        file_list = request_json['file_list']

        # Call split function here
        random_nodes = select_random_machines(self.nodes, num_juices)
        # This is a dict containing each machine ip and the files added to it
        split_file_dict = {}

        # Send ack to master
        response = {}
        response['type'] = 'split_ack'
        response['node_ips'] = split_file_dict
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent juice split_ack to master with list: {split_file_dict}")

        return

    def handle_map_cmd(self, request_json):
        """
        handles all processing for the map phase of the maple command
        """
        logging.debug("Received map command: " + str(request_json))

        maple_exe = request_json['maple_exe']
        master_node = request_json['sender_host']
        file_list = request_json['file_list']

        # Pull each file from the sdfs
        for file in file_list:
            self.sdfs_read_callback(file)

        # TODO: Have map function wait until file exists to open it

        # Run map command on each individual file and accumulate its outputs
        key_files = {}
        for file in file_list:
            key_files_dict = {}
            for key in key_files_dict.keys():
                key_files[key] = key_files.get(key, []) + key_files_dict[key]

        response = {}
        response['type'] = 'map_ack'
        response['key_list'] = key_files
        response['file_list'] = file_list
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent map_ack to master with list: {key_files}")

        return

    def handle_juice_cmd(self, request_json):
        """
        handles all processing for the reduce phase of the juice command
        """
        logging.debug("Received juice command: " + str(request_json))

        juice_exe = request_json['juice_exe']
        master_node = request_json['sender_host']
        file_list = request_json['file_list']

        # Pull each file from the sdfs
        for file in file_list:
            self.sdfs_read_callback(file)

        # TODO: Have map function wait until file exists to open it

        # Run map command on each individual file and accumulate its outputs
        key_files = {}
        for file in file_list:
            key_files_dict = {}
            for key in key_files.keys():
                key_files[key] = key_files.get(key, []) + key_files_dict[key]

        response = {}
        response['type'] = 'juice_ack'
        response['key_list'] = key_files
        response['file_list'] = file_list
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent juice_ack to master with list: {key_files}")

        return

    def handle_cmbn_cmd(self, request_json):
        """
        hanldes all processing for the combine phase of the maple command
        """
        logging.debug("Received combine command: " + str(request_json))

        master_node = request_json['sender_host']
        combine_list = request_json['combine_list']


        # Pull each file from the sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_read_callback(file)

        key_files = {}
        # Combine all files with the same key
        for key in combine_list.keys():
            # TODO: Call function here that takes in list of files and a key
            key, file = '', ''
            key_files[key] = file

        # Push all files to the SDFS
        for key in key_files.keys():
            self.sdfs_write_callback(key_files[key])

        # delete all intermediate files from sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_delete_callback(file)

        # Send final ack to the master
        response = {}
        response['type'] = 'combine_ack'
        response['key_files'] = key_files
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent combine_ack to master with list: {key_files}")

        return

    def handle_juice_cmbn_cmd(self, request_json):
        """
        hanldes all processing for the combine phase of the maple command
        """
        logging.debug("Received juice combine command: " + str(request_json))

        master_node = request_json['sender_host']
        combine_list = request_json['combine_list']
        sdfs_dest_filename = request_json['sdfs_dest_filename']
        juice_delete = request_json['delete_input']
        delete_file_list = request_json['file_list']

        # Pull each file from the sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_read_callback(file)

        # Insert function here to combine all files into a single file
        # With name = sdfs_dest_filename

        # Push all files to the SDFS
        self.sdfs_write_callback(sdfs_dest_filename)

        # Delete all intermediate files if delete is true
        if int(juice_delete) == 1:
            for file in delete_file_list:
                self.sdfs_delete_callback(file)

        # Send final ack to the master
        response = {}
        response['type'] = 'combine_ack'
        response['sdfs_dest_filename'] = sdfs_dest_filename
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent juice combine_ack to master with list: {sdfs_dest_filename}")

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

def select_random_machines(machine_list, num_machines):
    """
    Take in a list of machines and select a random set to distribute from
    """
    return random.sample(machine_list, num_machines)

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
        cur_block[machine] = open('hdfs_files/' + file_name, "w")
        block_list[machine] = [file_name]

    idx = 0
    machine_cnt = len(machine_list)
    for in_file in file_list:
        logging.debug(in_file)
        with open('hdfs_files/'+in_file, "r") as fp:
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
                    cur_block[cur_machine] = open('hdfs_files/'+new_file_name, "w")
                    block_list[cur_machine].append(new_file_name)

                # move to next machine (idx+1%num_machines)
                idx = idx + 1
                idx = idx % machine_cnt
                line = fp.readline()

    # now close all the open blocks that did not fill to NUM_INPUT_LINES
    for open_block in cur_block.values():
        open_block.close()

    return block_list
