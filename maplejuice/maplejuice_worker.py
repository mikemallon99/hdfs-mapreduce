import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading
import sys

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

        __import__(request_json[''])
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


def split_input_files(file_list, machine_list):
    """"
    Helper function to distribute inputs evenly across all worker machines. Splits each file into 25 line chunks and
    assigns each to a worker machine.
    """
    block_list = {}  # The function output, maps machine -> list of files assigned to it
    for machine in machine_list:
        block_list[machine] = []

    line_cnt = {}  # tracks the number of lines in the block file currently being constructed
    cur_block = {}  # maps machine -> the file object associated with the current block being constructed

    machine_cnt = len(machine_list)

    idx = 0
    logging.debug(file_list)
    for file_name in file_list:
        logging.debug(file_name)
        with open(file_name, "r") as in_file:
            block_cnt = 1
            line_arr = in_file.readlines()
            for machine in machine_list:
                line_cnt[machine] = 0

            for line in line_arr:
                cur_machine = machine_list[idx]
                # create the block file if its a new block
                if line_cnt[cur_machine] == 0:
                    filename = file_name.split('.')[0] + "_block_" + str(block_cnt) + ".txt"
                    cur_block[cur_machine] = open(filename, "w")
                    block_list[cur_machine].append(filename)
                # add the line to the file
                cur_block[cur_machine].write(line)
                line_cnt[cur_machine] += 1
                # if this block isnt full, continue to add another line
                if line_cnt[cur_machine] == NUM_INPUT_LINES:
                    block_cnt += 1
                    # close this machines input block
                    line_cnt[cur_machine] = 0
                    cur_block[cur_machine].close()
                    idx = idx + 1
                    idx = idx % machine_cnt

            # reset variables to be used for next iteration
            for machine in line_cnt.keys():
                if line_cnt[machine] != 0:
                    cur_block[machine].close()
                line_cnt[machine] = 0

    logging.debug("Split files output")
    for m in block_list.keys():
        msg = m + ": " + str(block_list[m])
        logging.debug(msg)
    return block_list


def run_maple_exe(maple_exe, src_file):
    """
    runs the maple executable on a given file
    returns the list of key, value pairs output by the maple executable
    The return of the maple executable should be in the format [(k1, v1), (k2, v2), ...]
    """
    __import__(maple_exe)
    maple_func = sys.modules[maple_exe]
    key = get_key_from_filename(src_file)
    with open(src_file, "r") as src_fp:
        values = src_fp.readlines()
    key_value_list = maple_func.run(key, values)
    logging.debug(key_value_list)
    return key_value_list


def get_key_from_filename(filename):
    """
    Given the intermediate maple filename, returns the key name
    """
    split_ = filename.split("_")
    last_idx = len(split_) - 1
    key_name_arr = split_[0:last_idx - 1]
    key_name = ""
    for word in key_name_arr:
        key_name = key_name + word + "_"
    key_name = key_name[:-1]
    logging.debug("Key from file [" + filename + "] => " + key_name)
    return key_name


def run_maple_on_files(maple_exe, src_file_list, file_prefix):
    """
    Runs the maple_exe on each file in the src_file_list
    Writes the list of values for each key to a file prefixed by file_prefix
    """
    key_files_dict = {}  # dictionary of keys -> file its stored at
    key_values_dict = {}  # dictionary of keys -> list of values

    # run maple on each input file
    for src_file in src_file_list:
        kv_list = run_maple_exe(maple_exe, src_file)
        for key_value in kv_list:
            key_values_dict.setdefault(key_value[0], []).append(key_value[1])

    # after maple is done, write all keys to a file with the intermediate_filename_prefix
    for key in key_values_dict.keys():
        dest_filename = file_prefix+"_"+str(key)
        with open(dest_filename, "w") as out_file:
            values = key_values_dict[key]
            out_file.write("\n".join(str(value) for value in values))
        key_files_dict.setdefault(key, []).append(dest_filename)

    return key_files_dict
