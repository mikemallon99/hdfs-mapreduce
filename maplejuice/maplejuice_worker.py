import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict
from .maplejuice_master import parse_and_validate_message
import threading
import random
import sys
from os import listdir
from itertools import cycle

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
        prefix = request_json['sdfs_src_prefix']

        # Call split function here
        random_nodes = select_random_machines(self.nodes, num_maples)
        split_file_dict = split_input_files(file_list, random_nodes, prefix)
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
        split_file_dict = split_juice_keys(file_list, random_nodes)

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
        file_prefix = request_json['file_prefix']
        target_id = request_json['target_id']

        # Pull each file from the sdfs
        for file in file_list:
            self.sdfs_read_callback(file)
        self.sdfs_read_callback(maple_exe)

        # TODO: Have map function wait until file exists to open it
        key_files = run_maple_on_files(maple_exe, file_list, file_prefix, target_id)

        # Put all files into the SDFS
        for key in key_files.keys():
            for file in key_files[key]:
                self.sdfs_write_callback(file)

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
        prefix = request_json['sdfs_dest_filename']
        destination_prefix = request_json['sdfs_dest_filename']
        target_id = request_json['target_id']

        # Pull each file from the sdfs
        for file in file_list:
            self.sdfs_read_callback(file)
        self.sdfs_read_callback(juice_exe)

        juice_file = run_juice_on_files(juice_exe, file_list, prefix, destination_prefix, target_id)

        response = {}
        response['type'] = 'juice_ack'
        response['key_list'] = {'juice': [juice_file]}
        response['file_list'] = file_list
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent juice_ack to master with file: {juice_file}")

        return

    def handle_cmbn_cmd(self, request_json):
        """
        hanldes all processing for the combine phase of the maple command
        """
        logging.debug("Received combine command: " + str(request_json))

        master_node = request_json['sender_host']
        combine_list = request_json['combine_list']
        delete_file_list = request_json['file_list']

        # Pull each file from the sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_read_callback(file)

        key_files = combine_key_files(combine_list)

        # Push all files to the SDFS
        for key in key_files.keys():
            self.sdfs_write_callback(key_files[key])

        logging.debug("Combined all files. Deleting intermediate files.")

        # delete all intermediate files from sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_delete_callback(file)

        for file in delete_file_list:
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

        file_list = []

        # Pull each file from the sdfs
        for key in combine_list.keys():
            for file in combine_list[key]:
                self.sdfs_read_callback(file)
                file_list.append(file)

        # Insert function here to combine all files into a single file
        output_filename = combine_juice_output(file_list)

        # Push all files to the SDFS
        self.sdfs_write_callback(sdfs_dest_filename)

        # Delete all intermediate files if delete is true
        if int(juice_delete) == 1:
            for file in delete_file_list:
                self.sdfs_delete_callback(file)
        for file in file_list:
            self.sdfs_delete_callback(file)

        # Send final ack to the master
        response = {}
        response['type'] = 'combine_ack'
        response['sdfs_dest_filename'] = output_filename
        response['sender_host'] = self.node_id
        message_data = json.dumps(response).encode()
        self.cmd_sock.sendto(message_data, (master_node, MJ_HANDLER_PORT))

        logging.debug(f"Sent juice combine_ack to master with list: {output_filename}")

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

def put_all(file_prefix):
    """
    Take a prefix and queue a write command for each file with that prefix
    """
    files = []
    for f in listdir('hdfs_files/'):
        if file_prefix in f:
            files.append(f)
    return files

def select_random_machines(machine_list, num_machines):
    """
    Take in a list of machines and select a random set to distribute from
    """
    return random.sample(machine_list, num_machines)

def split_input_files(file_list, machine_list, sdfs_src_prefix):
    # TODO == This needs access to the source prefix
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
        # with open(file_name, "r") as in_file:
        with open("hdfs_files/" + file_name, "r") as in_file:
            block_cnt = 1
            line_arr = in_file.readlines()
            for machine in machine_list:
                line_cnt[machine] = 0

            for line in line_arr:
                cur_machine = machine_list[idx]
                # create the block file if its a new block
                if line_cnt[cur_machine] == 0:
                    filename = file_name + "_block_" + str(block_cnt)
                    cur_block[cur_machine] = open("hdfs_files/" + filename, "w")
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

# New map format compatible
def run_maple_exe(maple_exe, src_file):
    """
    runs the maple executable on a given file
    returns the list of key, value pairs output by the maple executable
    The return of the maple executable should be in the format [(k1, v1), (k2, v2), ...]
    """
    module_name = 'hdfs_files.' + maple_exe.split(".")[0]
    __import__(module_name)
    maple_module = sys.modules[module_name]
    with open('hdfs_files/'+src_file, "r") as src_fp:
        lines = src_fp.readlines()
    kv_in_list = maple_module.map_format(lines)
    kv_out_list = maple_module.maple(kv_in_list)
    return kv_out_list

# TODO == not needed given our new map format
def get_key_from_in_filename(filename):
    """
    Given the intermediate maple filename, returns the key name
    filename format = [key_name]_block_[block#]
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

# New map format compatible
def get_prefix_from_out_filename(filename):
    """
    Given the filename of the emitted (key, values) from maple, this function extracts the filename
    of the maple destination file
    filename format = [prefix]_[key]_[machine_id]
    """
    split_name = filename.split("_")
    dest_file_prfx = ""
    for word in split_name:
        if "fa20-cs425" in word:
            break
        dest_file_prfx += word
        dest_file_prfx += "_"

    dest_file_prfx = dest_file_prfx[:-1]
    logging.debug("Destination file: "+dest_file_prfx)
    return dest_file_prfx

# New map format compatible
def run_maple_on_files(maple_exe, src_file_list, file_prefix, machine_id):
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
        dest_filename = file_prefix+"_"+str(key)+"_"+machine_id
        with open('hdfs_files/'+dest_filename, "w") as out_file:
            values = key_values_dict[key]
            out_file.write("\n".join(str(value) for value in values))
        key_files_dict.setdefault(key, []).append(dest_filename)

    return key_files_dict

# New map format compatible
def combine_key_files(key_map):
    """
    Takes the key_map input [dict: key -> list of files storing key values] and combines all files
    holding the values into one file per key
    """
    output_files = {}
    for key in key_map.keys():
        dest_filename = get_prefix_from_out_filename(key_map[key][0])
        output_files[key] = dest_filename
        with open('hdfs_files/'+dest_filename, "w") as dest_file:
            for i, value_filename in enumerate(key_map[key]):
                with open('hdfs_files/'+value_filename, "r") as value_file:
                    if i:
                        dest_file.write("\n")
                    values = value_file.readlines()
                    dest_file.writelines(values)
    return output_files


def split_juice_keys(key_files, machines):
    file_dict = {}
    machine_list_cycle = cycle(machines)
    for k_filename in key_files:
        cur_machine = next(machine_list_cycle)
        file_dict.setdefault(cur_machine, []).append(k_filename)

    return file_dict


def get_key_from_juice_input(juice_in_file, intermediate_prefix):
    key_name = juice_in_file.replace(intermediate_prefix+"_", "")
    return key_name


def run_juice_on_files(juice_exe, src_file_list, int_prefix, dest_prefix, machine_id):
    juice_output = []  # list of key, value pairs output from the juice executable

    # run juice on each key file in the list, append each output to a list
    for juice_file in src_file_list:
        key_name = get_key_from_juice_input(juice_file, int_prefix)
        kv_pair = run_juice_exe(juice_exe, juice_file, key_name)
        juice_output.append(kv_pair)

    # writes all the outputs to the destination file of this machine
    outfile_name = dest_prefix + "_" + machine_id
    with open('hdfs_files/'+outfile_name, "w") as outfile:
        outfile.writelines("\n".join(str(kv_pair[0])+"|"+str(kv_pair[1]) for kv_pair in juice_output))

    return outfile_name


def run_juice_exe(juice_exe, key_file, key_value):
    __import__(juice_exe)
    juice_module = sys.modules[juice_exe]
    with open('hdfs_files/'+key_file, "r") as value_f:
        values = value_f.readlines()
    juice_out = juice_module.juice(key_value, values)
    return juice_out


def get_dest_prefix(juice_outfile):
    split_name = juice_outfile.split("_")
    split_name.pop()  # remove the machine id from the name
    dest_name = "_".join(str(word) for word in split_name)
    return dest_name


def combine_juice_output(file_list):
    sample_file = file_list[0]
    dest_file_name = get_dest_prefix(sample_file)
    final_output = []
    for file in file_list:
        with open('hdfs_files/'+file, "r") as fp:
            out_list = fp.readlines()
        for kv in out_list:
            if kv[-1] == '\n':
                kv = kv[:-1]
            final_output.append(kv)
            logging.debug(kv)

    logging.debug(final_output)

    with open('hdfs_files/'+dest_file_name, "w") as dest_file:
        dest_file.write("\n".join(str(kv) for kv in final_output))

    return dest_file_name
