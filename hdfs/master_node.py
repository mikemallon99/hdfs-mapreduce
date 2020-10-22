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
        self.nodetable = {}
        self.filetable = {}
        self.op_queue = []
        self.queue_lock = threading.Lock()
        self.node_ip = node_ip

        # Populate the node table with each slave
        for node in nodes:
            self.nodetable[node] = []

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
                #self.queue_handler(request, sock)

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


        # Inform the file node of the nodes requesting the file
        logging.info(f"Sending read request to {file_node}")
        message_data = json.dumps(request).encode()
        sock.sendto(message_data, file_node)

        # Wait for the acknowledgement message
        sock.settimeout(60)
        try:
            data, address = sock.recvfrom(4096)
            request_json = parse_and_validate_message(data)
        except socket.timeout:
            logging.info(f"Timeout waiting for acknowledgement from {request_nodes}")



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
