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
        self.file_table = None
        self.work_table = {}
        self.work_lock = threading.Lock()

        self.cur_application = ''
        self.cur_prefix = ''

        self.mj_man_sock = None
        self.mj_listener_sock = None

        # Populate the work table
        for node in self.nodes:
            self.work_table[node] = []

    def update_file_table(self, file_table):
        self.file_table = file_table

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
                logging.info(f"Recieved a request from {request_json['sender_host']}

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
                    self.handle_maple(request, self.list_sock)
                elif request['type'] == 'juice':
                    logging.info(f"Handling juice request from {request['sender_host']}")
                    self.handle_juice(request, self.list_sock)

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
        request_node = request['sender_host']

        # Search sdfs for the files
        file_list = []
        for filename in self.file_table.keys():
            if sdfs_src_directory in filename:
                file_list.append(filename)

        # Send filenames to requester
        response = dict.copy(request)
        response['type'] = 'split'
        response['file_list'] = file_list
        response['sender_host'] = self.node_ip

        # Increment ack table for requesting nodes
        self.ack_lock.acquire()
        self.acktable[request_node] += 1
        self.ack_lock.release()
        logging.info(f"Sending split data to {request_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (request_node, MJ_HANDLER_PORT))

        # Wait for the split acknowledgement message
        start_time = datetime.now()
        counts = 0
        nodes = [request_node]
        while not (len(nodes) == 0 or counts >= 3) and request_node in self.work_table.keys():
            nodes = self.validate_acks()
            if (datetime.now() - start_time).total_seconds() > 120:
                # redo sends
                logging.info(f"Trying again")
                sock.sendto(message_data, (request_node, MJ_HANDLER_PORT))
                start_time = datetime.now()
                counts += 1
                if counts == 3:
                    logging.info("Split failed")

        # Once we have recieved the split, we can add the worker nodes to the work
        # table and inform them of the operations they must perform
        self.work_lock.acquire()
        for node in self.work_table.keys():
            self.send_maple_message(node, self.work_table[node], sock)
        self.work_lock.release()

        # After sending the messages, wait for all of the ack bits
        start_time = datetime.now()
        counts = 0
        while True:
            if self.validate_acks() == 0:
                break

    def send_maple_message(self, node, files, sock):
        """
        Send a message to a worker telling it to process files
        Additionally, increment the ack table for that node
        """
        response = {}
        response['sender_host'] = self.node_ip
        response['type'] = 'maple'
        response['maple_exe'] = self.cur_application
        response['file_list'] = files
        response['file_prefix'] = self.cur_prefix
        self.ack_lock.acquire()
        self.acktable[node] += 1
        self.ack_lock.release()

        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (node, MJ_HANDLER_PORT))

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
                decrement_ack_thread = threading.Thread(target=self.decrement_ack, args=(request_json['sender_host'],))
                decrement_ack_thread.start()

    def split_ack(self, request):
        """
        Use this function to populate the work table upon recieving a split ack
        """
        self.work_lock.acquire()
        for node in request['node_ips'].keys():
            self.work_table[node] = request['node_ips'][node]
        self.work_lock.release()
        self.decrement_ack(request['sender_host'])

    def reallocate_work(self, node):
        """
        Check to see if a failed node has any work scheduled onto it, and
        fix the work table and ack table to compensate
        """

    def decrement_ack(self, node):
        """
        Use this function to decrement the acktable for a receiving node without having to wait
        """
        self.ack_lock.acquire()
        if self.acktable[node] > 0:
            self.acktable[node] -= 1
        self.ack_lock.release()

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