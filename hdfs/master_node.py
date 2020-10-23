import threading
import logging
import socket
import json
from typing import Optional, Dict

QMANAGER_PORT = 12345
QHANDLER_PORT = 12346

ack_available = threading.Event()

class MasterNode:
    def __init__(self, nodes, node_ip):
        self.nodetable = {}  # TODO == easier to make this a list?
        self.filetable = {}
        self.acktable = {}
        self.op_queue = []
        self.queue_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.node_ip = node_ip

        # Populate the node table with each slave
        for node in nodes:
            self.nodetable[node] = []
            self.acktable[node] = 0

    def start_master(self):
        queue_manager = threading.Thread(target=self.queue_manager_thread)
        queue_handler = threading.Thread(target=self.queue_handler_thread)
        listener = threading.Thread(target=self.listener_thread)

        # Start all threads
        queue_manager.start()
        queue_handler.start()
        listener.start()

    def enqueue_read(self, request):
        """
        Safely enqueue a read operation. Attempt to combine reads of the same file
        """
        self.queue_lock.acquire()
        add_flag = True
        # Check if the file is already being requested
        for i in range(0, len(self.op_queue)):
            entry = self.op_queue[i]
            if entry['filename'] == request['filename'] and entry['op'] == 'read':
                entry['addr'].append(request['addr'])
                add_flag = False
                break
        # Otherwise pin it to the end of the queue
        if add_flag:
            self.op_queue.append(request)
        self.queue_lock.release()

    def enqueue_write(self, request):
        """
        Safely enqueue a write operation at the end of the queue
        """
        self.queue_lock.acquire()
        self.op_queue.append(request)
        self.queue_lock.release()

    def queue_manager_thread(self):
        """
        Listen for messages from group members. Update membership list accordingly.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.node_ip, QMANAGER_PORT))

        while True:
            data, address = sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
            if request_json is None:
                # The data received is not valid
                continue

            # Enqueue the data
            if request_json['op'] == 'read':
                self.enqueue_read(request_json)
            elif request_json['op'] == 'write':
                self.enqueue_write(request_json)

    def listener_thread(self):
        """
        Listen for messages being sent to the queue handler
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.node_ip, QHANDLER_PORT))

        while True:
            data, address = sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)

            if request_json['ack'] == True:
                self.decrement_ack(data['sender_host'])

    def decrement_ack(self, node):
        """
        Use this function to decrement the acktable for a receiving node without having to wait
        """
        self.ack_lock.acquire()
        if self.acktable[node] > 0:
            self.acktable[node] -= 1
        self.ack_lock.release()

    def queue_handler_thread(self):
        """
        Continuously completes tasks inputted into the operation queue
        Does not attempt the next task until the last task is totally finished
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
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

                # Handle the request
                if request['op'] == 'read':
                    logging.info(f"Handling read request from {request['addr']}")
                    self.handle_read(request, sock)
                elif request['op'] == 'write':
                    logging.info(f"Handling write request from {request['addr']}")
                    self.handle_write(request, sock)

    def handle_write(self, request, sock):
        """
        Handle a write operation from the queue.
        The objective of the operation is to refer the request machine to 4 machines that can hold the file,
        and the write operation will go to the machine. Once the file has been written, the machines
        being written to will return with a acknowledgement.
        """
        # Find a machine that has space for the file
        request_nodes = request['addr']
        filename = request['filename']
        file_nodes = []

        # First, check if the file is in the network
        if filename in self.filetable.keys():
            file_nodes = self.filetable.get(filename)
        # Otherwise find nodes with free space
        else:
            sortednodetable = sorted(self.nodetable, key=lambda key: len(self.nodetable[key]))
            file_nodes = sortednodetable[:4]

        # Add each write node to the ack table
        for node in file_nodes:
            self.ack_lock.acquire()
            self.acktable[node] += 1
            self.ack_lock.release()

        # Direct the requester to each node
        response = dict.copy(request)
        response['addr'] = file_nodes
        logging.info(f"Sending write nodes to {request_nodes}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (request_nodes[0], QHANDLER_PORT))

        # Wait for the acknowledgement message
        valid = False
        while not valid:
            valid = self.validate_acks(request_nodes)

        logging.info("All ACKs recieved, write successful")


    def handle_read(self, request, sock):
        """
        Handle a read operation left in the queue.
        The objective is to find a machine holding the file, and tell the machine
        to send the requested file to the machine asking for it. Then, wait for the process
        to finish.
        """
        # Find a machine holding the file
        request_nodes = request['addr']
        filename = request['filename']
        file_node = None

        if filename in self.filetable.keys():
            filetable_entry = self.filetable.get(filename)
            if len(filetable_entry) > 0:
                file_node = filetable_entry[0]
        if file_node is None:
            # Inform machines that the file does not exist
            # TODO: Implement this
            pass

        # Increment ack table for requesting nodes
        for node in request_nodes:
            self.ack_lock.acquire()
            self.acktable[node] += 1
            self.ack_lock.release()

        # Inform the file node of the nodes requesting the file
        logging.info(f"Sending read request to {file_node}")
        message_data = json.dumps(request).encode()
        sock.sendto(message_data, (file_node, QHANDLER_PORT))

        # Wait for the acknowledgement message
        valid = False
        while not valid:
            valid = self.validate_acks(request_nodes)

        logging.info("All ACKs recieved, read successful")

    def validate_acks(self, nodes):
        """
        Check to make sure we can stop waiting for nodes.
        Valid if a machine is failed or if all acks are recieved
        """
        valid = True
        for node in nodes:
            # Keep waiting if there are still needed acks
            self.ack_lock.acquire()
            node_acks = self.acktable[node]
            self.ack_lock.release()
            if node_acks > 0:
                valid = False
            if node not in self.nodetable.keys():
                valid = True
                logging.info(f"Node {node} not detected in node table")
                break

        return valid


def recv_ack_thread(sock):
    """
    Start this as a thread so a message can be timed out in the case of a failure
    """
    data, address = sock.recvfrom(4096)
    request_json = parse_and_validate_message(data)
    return request_json

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
