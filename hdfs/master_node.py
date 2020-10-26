import threading
import logging
import socket
import json
from datetime import datetime
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

    def set_filenode_tables(self, ftable, ntable):
        """
        Generate the masternodes filetable from an input
        """
        self.filetable = ftable
        self.nodetable = ntable

        # Remove self from nodetable and from all filetable entries
        node_files = self.nodetable.pop(self.node_ip, [])
        for file in node_files:
            try:
                self.filetable[file].remove(self.node_ip)
            except ValueError:
                logging.error("Value error")
                continue
            # Add write to queue
            request = {}
            request['op'] = 'write'
            request['sender_host'] = self.node_ip  # TODO == why need both?
            request['addr'] = [self.filetable[file][0]]
            request['filename'] = file
            request['localfilename'] = file

            self.enqueue_write_front(request)

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
            # Add write to queue
            request = {}
            request['op'] = 'write'
            request['sender_host'] = self.node_ip  # TODO == why need both?
            request['addr'] = [self.filetable[file][0]]
            request['filename'] = file
            request['localfilename'] = file

            self.enqueue_write_front(request)

    def enqueue_read(self, request):
        """
        Safely enqueue a read operation. Attempt to combine reads of the same file
        """
        self.queue_lock.acquire()
        add_flag = True
        # Check if the file is already being requested
        for i in range(0, len(self.op_queue)):
            entry = self.op_queue[i]
            # Stop if entry is writing the file
            if entry['filename'] == request['filename'] and entry['op'] == 'write':
                self.op_queue.append(request)
                break
            if entry['filename'] == request['filename'] and entry['op'] == 'read':
                for addr in request['addr']:
                    entry['addr'].append(addr)
                add_flag = False
                break
        # Otherwise pin it to the end of the queue
        if add_flag:
            self.op_queue.append(request)
        self.queue_lock.release()

    def enqueue_write_front(self, request):
        """
        Safely enqueue a write operation at the end of the queue
        """
        self.queue_lock.acquire()
        self.op_queue.insert(0, request)
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

    def enqueue_delete(self, request):
        """
        safely enqueue an delete operation at the end of the queue
        """
        self.queue_lock.acquire()
        self.op_queue.append(request)
        self.queue_lock.release()

    def enqueue_store(self, request):
        """
        safely enqueue an delete operation at the end of the queue
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
            elif request_json['op'] == 'delete':
                self.enqueue_ls(request_json)
                logging.info(f"Recieved delete request from {request_json['sender_host']}")
            elif request_json['op'] == 'store':
                self.enqueue_ls(request_json)
                logging.info(f"Recieved store request from {request_json['sender_host']}")
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
                decrement_ack_thread = threading.Thread(target=self.decrement_ack, args=(request_json['sender_host'],))
                decrement_ack_thread.start()

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
                    self.handle_write(request, self.list_sock)
                elif request['op'] == 'ls':
                    logging.info(f"Handling ls request from {request['sender_host']}")
                    self.handle_ls(request, self.list_sock)
                elif request['op'] == 'delete':
                    logging.info(f"Handling delete request from {request['sender_host']}")
                    self.handle_delete(request, self.list_sock)
                elif request['op'] == 'store':
                    logging.info(f"Handling delete request from {request['sender_host']}")
                    self.handle_store(request, self.list_sock)

    def handle_store(self, request, sock):
        """
        Handle a store operation from the queue.
        The store function will return all files being stored
        at the requesting machine.
        """
        request_node = request['sender_host']
        # Retrieve all files from nodetable
        files = self.nodetable[request_node].copy()

        # Formulate response
        response = dict.copy(request)
        response['filelist'] = files
        response['sender_host'] = self.node_ip

        logging.info(f"Sending node data to {request_node}")
        message_data = json.dumps(response).encode()
        sock.sendto(message_data, (request_node, QHANDLER_PORT))

    def handle_delete(self, request, sock):
        """
        Handle a delete operation from the queue.
        The delete function will remove a file from the file table and
        delete all of its entries from the nodetable
        """
        filename = request['filename']

        # Get the nodes that contain the file
        nodes = self.filetable.pop(filename)
        # Remove the filename from all the nodes in the nodetable
        for node in nodes:
            self.nodetable[node].remove(filename)

        # TODO -- Add send function here
        logging.info(f"Successfully deleted {filename} from the file system.")

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
        request['timestamp'] = datetime.now().isoformat()
        file_nodes = []

        # First, check if the file is in the network
        sortednodetable = sorted(self.nodetable, key=lambda key: len(self.nodetable[key]))
        if filename in self.filetable.keys():
            file_nodes = self.filetable.get(filename).copy()
            # Then, see if there are 4 replicas
            while len(file_nodes) < 4 and len(file_nodes) < len(self.nodetable.keys()):
                # Find the node with the most space that doesnt have the file
                for node in sortednodetable:
                    if node not in self.filetable[filename]:
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
        start_time = datetime.now()
        nodes = file_nodes.copy()
        counts = 0
        while not (len(nodes) == 0 or counts >= 3):
            nodes = self.validate_acks(file_nodes)
            if (datetime.now() - start_time).total_seconds() > 120:
                # redo sends
                logging.info(f"Trying again")
                sock.sendto(message_data, (request_nodes[0], QHANDLER_PORT))
                start_time = datetime.now()
                counts += 1
                if counts == 3:
                    logging.info("Write failed")



        # TODO: Send out node/file tables somewhere
        self.send_backup_information(sock)

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
        request['timestamp'] = datetime.now().isoformat()
        file_node = None

        if filename in self.filetable.keys():
            filetable_entry = self.filetable.get(filename)
            if len(filetable_entry) > 0:
                file_node = filetable_entry[0]
        if file_node is None:
            # Inform machines that the file does not exist
            # TODO: Implement this
            logging.info(f"Sending non-exist message to {request_nodes[0]}")
            response = dict.copy(request)
            response['sender_host'] = self.node_ip
            response['op'] = 'failure'
            message_data = json.dumps(response).encode()
            sock.sendto(message_data, (request_nodes[0], QHANDLER_PORT))
            return

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
        nodes = request_nodes.copy()
        start_time = datetime.now()
        counts = 0
        while not (len(nodes) == 0 or counts >= 3):
            nodes = self.validate_acks(request_nodes)
            if (datetime.now() - start_time).total_seconds() > 120:
                # redo sends
                logging.info(f"Trying again")
                sock.sendto(message_data, (file_node, QHANDLER_PORT))
                start_time = datetime.now()
                counts += 1
                if counts == 3:
                    logging.info("Read failed")

        logging.info("All ACKs recieved, read successful")

    def handle_ls(self, request, sock):
        filename = request['filename']
        file_list = self.retrieve_file_nodes(filename)
        message = {}
        message['op'] = 'disp_ls'
        message['filelist'] = file_list
        message['filename'] = filename
        bytes_sent = sock.sendto(json.dumps(message).encode(), (request['sender_host'], QHANDLER_PORT))
        if not bytes_sent == len(json.dumps(message)):
            logging.error("LS message not sent!")

    def validate_acks(self, nodes):
        """
        Check to make sure we can stop waiting for nodes.
        Valid if a machine is failed or if all acks are recieved
        """
        ack_nodes = []
        for node in nodes:
            # Keep waiting if there are still needed acks
            self.ack_lock.acquire()
            node_acks = self.acktable[node]
            self.ack_lock.release()
            if node_acks > 0:
                ack_nodes.append(node)
            if node not in self.nodetable.keys():
                self.ack_lock.acquire()
                self.ack_table[node] = 0
                self.ack_lock.release()
                logging.info(f"Node {node} not detected in node table")
                break

        return ack_nodes

    def send_backup_information(self, sock):
        message = {}
        message['op'] = 'backup_master'
        message['filetable'] = self.filetable
        message['nodetable'] = self.nodetable
        message_data = json.dumps(message).encode()
        for node in self.nodetable.keys():
            if node == self.node_ip:
                #logging.debug("Skipping dissemination to master")
                continue
            sock.sendto(message_data, (node, QHANDLER_PORT))
        logging.debug("Master backup info sent!")
        return


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
