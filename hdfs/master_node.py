import threading
import logging
import socket
import json
from typing import Optional, Dict

QMANAGER_PORT = 12345
QHANDLER_PORT = 12346
LS_PORT = 12348

ack_available = threading.Event()


class MasterNode:
    def __init__(self, nodes, node_ip):
        self.nodetable = {}
        self.filetable = {}
        self.acktable = {}
        self.op_queue = []
        self.queue_lock = threading.Lock()
        self.ack_lock = threading.Lock()
        self.node_ip = node_ip

        self.qman_sock = None
        self.qhan_sock = None
        self.list_sock = None

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

        logging.info(f"Starting master thread: {self.node_ip}")

    def stop_master(self):
        self.qman_sock.close()
        self.list_sock.close()
        self.qhan_sock.close()

        logging.info("Stopping master sockets")

    def node_failure(self, node):
        """
        This is called by the node manager when it detects that a node fails in the membership list
        This will take the node out from the node table and queue all of its files to be written
        """
        # Remove node from nodetable and from all filetable entries
        node_files = self.nodetable.pop(node, [])
        for file in node_files:
            try:
                self.filetable[file].remove(node)
            except ValueError:
                logging.error("Value error")
                continue

        # Add new writes to queue for file

    def enqueue_read(self, request):
        """
        Safely enqueue a read operation. Attempt to combine reads of the same file
        """
        self.queue_lock.acquire()
        add_flag = True
        # Check if the file is already being requested
        # TODO: Stop once theres a write of the same file in the queue
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

    def enqueue_ls(self, request):
        """
        safely enqueue an ls operation at the end of the queue
        """
        self.queue_lock.acquire()
        self.op_queue.append(request)
        self.queue_lock.release()

    def retrieve_file_nodes(self, filename):
        filenodes = []
        if filename in self.filetable.keys():
            filenodes = self.filetable[filename]
        return filenodes

    def queue_manager_thread(self):
        """
        Listen for messages from group members. Update membership list accordingly.
        """
        self.qman_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.qman_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.qman_sock.bind((self.node_ip, QMANAGER_PORT))

        while True:
            data, address = self.qman_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
            if request_json is None:
                # The data received is not valid
                logging.info(f"Recieved a request")
                continue

            # Enqueue the data
            if request_json['op'] == 'read':
                self.enqueue_read(request_json)
                logging.info(f"Recieved read request from {request_json['sender_host']}")
            elif request_json['op'] == 'write':
                self.enqueue_write(request_json)
                logging.info(f"Recieved write request from {request_json['sender_host']}")
            elif request_json['op'] == 'ls':
                self.enqueue_ls(request_json)
                logging.info(f"Recieved ls request from {request_json['sender_host']}")
            else:
                logging.info(f"Recieved a request from {request_json['sender_host']}")

    def listener_thread(self):
        """
        Listen for messages being sent to the queue handler
        """
        self.list_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.list_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.list_sock.bind((self.node_ip, QHANDLER_PORT))

        while True:
            data, address = self.list_sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)

            if request_json['op'] == 'ack':
                self.decrement_ack(request_json['sender_host'])

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
                if request['op'] == 'read':
                    logging.info(f"Handling read request from {request['sender_host']}")
                    self.handle_read(request, self.list_sock)
                elif request['op'] == 'write':
                    logging.info(f"Handling write request from {request['sender_host']}")
                    self.handle_write(request, self.qhan_sock)
                elif request['op'] == 'ls':
                    logging.info(f"Handling ls request from {request['sender_host']}")
                    self.handle_ls(request)

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
        sortednodetable = sorted(self.nodetable, key=lambda key: len(self.nodetable[key]))
        if filename in self.filetable.keys():
            file_nodes = self.filetable.get(filename)
            # Then, see if there are 4 replicas
            while len(file_nodes) < 4 and len(file_nodes) < len(self.nodetable.keys()) - len(request_nodes):
                # Find the node with the most space that doesnt have the file
                for node in sortednodetable:
                    if filename not in self.nodetable[node] and node not in request_nodes:
                        # Add file to tables
                        file_nodes.append(node)
                        self.filetable[filename].append(node)
                        self.nodetable[node].append(filename)
                        break
        # Otherwise find nodes with free space
        else:
            #file_nodes = sortednodetable[:5]
            # Add file to filetable
            self.filetable[filename] = []
            # Fix node and file tables
            counter = 0
            for node in sortednodetable:
                if node not in request_nodes:
                    file_nodes.append(node)
                    self.nodetable[node].append(filename)
                    self.filetable[filename].append(node)
                    counter += 1
                if counter >= 4:
                    break

        # Add each write node to the ack table
        for node in file_nodes:
            self.ack_lock.acquire()
            self.acktable[node] += 1
            self.ack_lock.release()

        # Direct the requester to each node
        response = dict.copy(request)
        response['addr'] = file_nodes
        logging.info(f"Sending Nodes:{file_nodes} to {request_nodes}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (request_nodes[0], QHANDLER_PORT))

        # Wait for the acknowledgement message
        valid = False
        while not valid:
            valid = self.validate_acks(request_nodes)

        # TODO: Send out node/file tables somewhere

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
        response = dict.copy(request)
        response['sender_host'] = self.node_ip
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (file_node, QHANDLER_PORT))

        # Wait for the acknowledgement message
        valid = False
        while not valid:
            valid = self.validate_acks(request_nodes)

        logging.info("All ACKs recieved, read successful")

    def handle_ls(self, request):
        ls_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        filename = request['filename']
        file_list = self.retrieve_file_nodes(filename)
        message = {}
        message['op'] = 'disp_ls'
        message['filelist'] = file_list
        message['filename'] = filename
        bytes_sent = ls_sock.sendto(json.dumps(message).encode(), (request['sender_host'], QHANDLER_PORT))
        if not bytes_sent == len(json.dumps(message)):
            logging.error("LS message not sent!")

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
