import threading
import logging
import socket
import json
from datetime import datetime
from typing import Optional, Dict

MJ_MANAGER_PORT = 12444
MJ_HANDLER_PORT = 12445


class MapleJuiceMaster:
    def __init__(self, node_ip, nodes):
        self.acktable = {}
        self.op_queue = []
        self.queue_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.node_ip = node_ip
        self.nodes = nodes
        self.work_table = {}
        self.work_lock = threading.Lock()

        self.node_key_table = {}
        self.node_key_table_lock = threading.Lock()

        self.cur_application = ''
        self.cur_prefix = ''

        self.mj_man_sock = None
        self.mj_listener_sock = None

        self.file_table_callback = None

        # Populate the work table & ack table
        for node in self.nodes:
            self.work_table[node] = []
            self.acktable[node] = 0

    def start_master(self):
        """
        Starts necessary threads and initializes ports
        """
        logging.debug("Starting MapleJuice master.")

        mj_manager_thread = threading.Thread(target=self.mj_manager_thread)
        mj_manager_thread.start()

        mj_handler_thread = threading.Thread(target=self.queue_handler_thread)
        mj_handler_thread.start()

        listener_thread = threading.Thread(target=self.listener_thread)
        listener_thread.start()

    def stop_master(self):
        self.mj_man_sock.close()
        self.mj_listener_sock.close()

    def set_filetable_callback(self, func):
        self.file_table_callback = func

    def update_node_list(self, nodes):
        """
        Node manager should call this to update the membership list
        """
        self.nodes = nodes

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
        self.mj_man_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.mj_man_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.mj_man_sock.bind((self.node_ip, MJ_MANAGER_PORT))

        while True:
            data, address = self.mj_man_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
            if request_json is None:
                # The data received is not valid
                logging.info(f"Recieved a request")
                continue

            # Enqueue the data
            if request_json['type'] == 'maple':
                self.enqueue_maple(request_json)
                logging.info(f"Recieved maple request from {request_json['sender_host']}")
            elif request_json['type'] == 'juice':
                self.enqueue_juice(request_json)
                logging.info(f"Recieved juice request from {request_json['sender_host']}")
            else:
                logging.info(f"Recieved a request from {request_json['sender_host']}")

    def queue_handler_thread(self):
        """
        Continuously completes tasks inputted into the operation queue
        Does not attempt the next task until the last task is totally finished
        """
        while True:
            # Check for an update in the queue
            queue_update = False
            request = {}
            self.queue_lock.acquire()
            if len(self.op_queue) > 0:
                queue_update = True
                request = self.op_queue[0]
                self.op_queue.pop(0)
            self.queue_lock.release()

            if queue_update:
                # Handle the request
                if request['type'] == 'maple':
                    logging.info(f"Handling maple request from {request['sender_host']}")
                    self.handle_maple(request, self.mj_listener_sock)
                elif request['type'] == 'juice':
                    logging.info(f"Handling juice request from {request['sender_host']}")
                    self.handle_juice(request, self.mj_listener_sock)

    def handle_juice(self, request, sock):
        """
        Process a juice request
        """
        self.cur_application = request['juice_exe']
        num_juices = request['num_juices']
        self.cur_prefix = request['file_prefix']
        sdfs_dest_filename = request['sdfs_dest_filename']
        self.target_node = request['sender_host']
        delete_input = request['delete_input']

        # Search sdfs for the files
        file_list = []
        file_table_copy = self.file_table_callback()
        for filename in file_table_copy.keys():
            if self.cur_prefix in filename:
                file_list.append(filename)

        # Send filenames to requester
        response = dict.copy(request)
        response['type'] = 'juice_split'
        response['file_list'] = file_list
        response['num_juices'] = num_juices
        response['sender_host'] = self.node_ip

        # Increment ack table for requesting nodes
        self.ack_lock.acquire()
        self.acktable[self.target_node] += 1
        self.ack_lock.release()
        logging.info(f"Sending juice split data to {self.target_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))

        # Wait for the split acknowledgement message
        start_time = datetime.now()
        counts = 0
        nodes = [self.target_node]
        while not (len(nodes) == 0 or counts >= 3) and self.target_node in self.work_table.keys():
            nodes = self.validate_acks()
            if (datetime.now() - start_time).total_seconds() > 120:
                # redo sends
                logging.info(f"Trying again")
                sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))
                start_time = datetime.now()
                counts += 1
                if counts == 3:
                    logging.info("Juice Split failed")

        # Once we have recieved the split, we can add the worker nodes to the work
        # table and inform them of the operations they must perform

        self.work_lock.acquire()
        logging.info(f"Sending juice work data to {self.work_table}")
        for node in self.work_table.keys():
            self.send_juice_message(node, self.work_table[node], sock)
        self.work_lock.release()

        # After sending the messages, wait for all of the ack bits
        while True:
            if len(self.validate_acks()) == 0:
                break

        # Send a message back to the requester telling it to combine all the files
        self.ack_lock.acquire()
        self.acktable[self.target_node] += 1
        self.ack_lock.release()

        response = {}
        response['sender_host'] = self.node_ip
        response['type'] = 'juice_combine'
        response['combine_list'] = self.node_key_table
        response['sdfs_dest_filename'] = sdfs_dest_filename
        response['delete_input'] = delete_input
        response['file_list'] = file_list

        logging.info(f"Sending combine request to {self.target_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))

        # Wait for the final ack indicating the files have been combined
        while True:
            if len(self.validate_acks()) == 0:
                break

        logging.info(f"Combine ack received, juice request completed.")

    def handle_maple(self, request, sock):
        """
        Process a maple request
        """
        # First is the split phase
        # Send a message to the requester telling it which files to split
        self.cur_application = request['maple_exe']
        num_maples = request['num_maples']
        self.cur_prefix = request['file_prefix']
        sdfs_src_directory = request['sdfs_src_directory']
        self.target_node = request['sender_host']

        # Search sdfs for the files
        file_list = []
        file_table_copy = self.file_table_callback()
        for filename in file_table_copy.keys():
            if sdfs_src_directory in filename:
                file_list.append(filename)

        # Send filenames to requester
        response = dict.copy(request)
        response['type'] = 'split'
        response['file_list'] = file_list
        response['num_maples'] = num_maples
        response['sender_host'] = self.node_ip

        # Increment ack table for requesting nodes
        self.ack_lock.acquire()
        self.acktable[self.target_node] += 1
        self.ack_lock.release()
        logging.info(f"Sending split data to {self.target_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))

        # Wait for the split acknowledgement message
        start_time = datetime.now()
        counts = 0
        nodes = [self.target_node]
        while not (len(nodes) == 0 or counts >= 3) and self.target_node in self.work_table.keys():
            nodes = self.validate_acks()
            if (datetime.now() - start_time).total_seconds() > 120:
                # redo sends
                logging.info(f"Trying again")
                sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))
                start_time = datetime.now()
                counts += 1
                if counts == 3:
                    logging.info("Split failed")

        # Once we have recieved the split, we can add the worker nodes to the work
        # table and inform them of the operations they must perform
        self.node_key_table_lock.acquire()
        self.node_key_table.clear()
        self.node_key_table_lock.release()

        self.all_intermediate_blocks = []

        self.work_lock.acquire()
        logging.info(f"Sending work data to {self.work_table}")
        for node in self.work_table.keys():
            self.all_intermediate_blocks += self.work_table[node]
            self.send_maple_message(node, node, self.work_table[node], sock)
        self.work_lock.release()

        # After sending the messages, wait for all of the ack bits
        while True:
            if len(self.validate_acks()) == 0:
                break

        # Send a message back to the requester telling it to combine all the files
        self.ack_lock.acquire()
        self.acktable[self.target_node] += 1
        self.ack_lock.release()

        response = {}
        response['sender_host'] = self.node_ip
        response['type'] = 'map_combine'
        response['combine_list'] = self.node_key_table
        response['file_list'] = self.all_intermediate_blocks

        logging.info(f"Sending combine request to {self.target_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (self.target_node, MJ_HANDLER_PORT))

        # Wait for the final ack indicating the files have been combined
        while True:
            if len(self.validate_acks()) == 0:
                break

        logging.info(f"Combine ack received, maple request completed.")

    def send_juice_message(self, node, files, sock):
        """
        Send a message to a worker telling it to process files
        Additionally, increment the ack table for that node
        """
        if len(files) == 0:
            return

        response = {}
        response['sender_host'] = self.node_ip
        response['type'] = 'juice'
        response['juice_exe'] = self.cur_application
        response['file_list'] = files
        response['file_prefix'] = self.cur_prefix
        self.ack_lock.acquire()
        self.acktable[node] += 1
        self.ack_lock.release()

        # Double check to make sure node is alive
        work_table_copy = self.work_table.copy()
        if node not in work_table_copy.keys():
            node = work_table_copy.keys()[0]

        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (node, MJ_HANDLER_PORT))

    def send_maple_message(self, target_node, send_node, files, sock):
        """
        Send a message to a worker telling it to process files
        Additionally, increment the ack table for that node
        """
        if len(files) == 0:
            return

        response = {}
        response['sender_host'] = self.node_ip
        response['type'] = 'maple'
        response['maple_exe'] = self.cur_application
        response['file_list'] = files
        response['file_prefix'] = self.cur_prefix
        response['target_node'] = target_node
        self.ack_lock.acquire()
        self.acktable[node] += 1
        self.ack_lock.release()

        # Double check to make sure node is alive
        work_table_copy = self.work_table.copy()
        if node not in work_table_copy.keys():
            node = work_table_copy.keys()[0]

        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (send_node, MJ_HANDLER_PORT))

    def validate_acks(self):
        """
        Check for all outstanding acks
        """
        self.ack_lock.acquire()
        node_acks = self.acktable.copy()
        self.ack_lock.release()
        outstanding_nodes = []
        for node in node_acks.keys():
            if node_acks[node] > 0:
                outstanding_nodes.append(node)

        return outstanding_nodes

    def listener_thread(self):
        """
        Listen for messages being sent to the queue handler
        """
        self.mj_listener_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.mj_listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.mj_listener_sock.bind((self.node_ip, MJ_HANDLER_PORT))

        while True:
            data, address = self.mj_listener_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)

            if request_json['type'] == 'split_ack':
                split_ack_thread = threading.Thread(target=self.split_ack, args=(request_json,))
                split_ack_thread.start()
            elif request_json['type'] == 'map_ack':
                map_ack_thread = threading.Thread(target=self.map_ack, args=(request_json,))
                map_ack_thread.start()
            elif request_json['type'] == 'combine_ack':
                combine_ack_thread = threading.Thread(target=self.decrement_ack, args=(request_json['sender_host'],))
                combine_ack_thread.start()
            elif request_json['type'] == "juice_ack":
                juice_ack_thread = threading.Thread(target=self.juice_ack, args=(request_json,))
                juice_ack_thread.start()

    def split_ack(self, request):
        """
        Use this function to populate the work table upon recieving a split ack
        """
        self.work_lock.acquire()
        for node in request['node_ips'].keys():
            self.work_table[node] = request['node_ips'][node]
        self.work_lock.release()
        self.decrement_ack(request['sender_host'])

    def map_ack(self, request):
        """
        Use this function to populate the node file table with the produced files
        """
        # The node key table maps a key to all the files associated with that key
        self.node_key_table_lock.acquire()
        for key in request['key_list'].keys():
            self.node_key_table[key] = self.node_key_table.get(key, []) + request['key_list'][key]
        self.node_key_table_lock.release()
        self.work_lock.acquire()
        for file in request['file_list']:
            self.work_table[request['sender_host']].remove(file)
        self.work_lock.release()
        self.decrement_ack(request['sender_host'])

    def juice_ack(self, request):
        """
        Use this function to populate the node file table with the produced juice file
        """
        self.node_key_table_lock.acquire()
        for key in request['key_list'].keys():
            self.node_key_table[key] = self.node_key_table.get(key, []) + request['key_list']['key']
        self.node_key_table_lock.release()
        self.work_lock.acquire()
        # Remove all files corresponding to request from work table
        for file in request['file_list']:
            self.work_table[request['sender_host']].remove(file)
        self.work_lock.release()
        self.decrement_ack(request['sender_host'])

    def decrement_ack(self, node):
        """
        Use this function to decrement the acktable for a receiving node without having to wait
        """
        self.ack_lock.acquire()
        if self.acktable[node] > 0:
            self.acktable[node] -= 1
        self.ack_lock.release()

    def node_failure(self, node):
        """
        Check to see if the node is being used as the target node or if it has
        any work scheduled to it and transfer to a different machine
        """
        logging.info(f"Removing {node} from work table")
        # Get outstanding work
        self.work_lock.acquire()
        work = self.work_table.pop(node)
        # Reallocate work to first node and send message
        # TODO: do this better
        new_node = self.work_table.keys()[0]
        self.work_table[new_node] = self.work_table.get(new_node, []) + work
        self.work_lock.release()

        # Send work message to the new node
        self.send_maple_message(node, new_node, work, self.mj_listener_sock)

        # Check if this node is the target
        if self.target_node == node:
            self.target_node = new_node


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
