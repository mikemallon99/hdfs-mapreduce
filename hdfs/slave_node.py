from .master_node import parse_and_validate_message
import json
import logging
import socket
import threading
import os

QMANAGER_PORT = 12345
QHANDLER_PORT = 12346
TCP_PORT = 12347

class SlaveNode():
    def __init__(self, master_host, self_host):
        self.nodetable = {}
        self.filetable = {}
        self.master_host = master_host
        self.self_host = self_host
        self.tcp_socket = None
        self.udp_socket = None
        self.qman_socket = None

    def start_slave(self):
        tcp_thread = threading.Thread(target=self.listener_thread_TCP)
        udp_thread = threading.Thread(target=self.listener_thread_UDP)
        tcp_thread.start()
        udp_thread.start()

        self.qman_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.qman_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.qman_socket.bind((self.self_host, QMANAGER_PORT))

        logging.info("Started slave listeners")

    def stop_slave(self):
        self.tcp_socket.close()
        self.udp_socket.close()
        self.qman_socket.close()
        logging.info("Stopping slave sockets")

    def send_write_request(self, filename):
        """
        Send a write request to the master queue manager
        """
        request = {}
        request['op'] = 'write'
        request['sender_host'] = self.self_host
        request['addr'] = [self.self_host]
        request['filename'] = filename

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Write to {self.master_host} queued")

    def handle_write_request(self, request):
        """
        Write file the target machines
        """
        target_nodes = request['addr']
        filename = request['filename']
        filesize = os.path.getsize("hdfs_files/" + filename)

        tcp_socket_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket_send.bind((socket.gethostname(), TCP_PORT))

        for target_node in target_nodes:
            logging.info(f"Attempting to connect to:{target_node}")
            c = tcp_socket_send.connect((target_node, TCP_PORT))
            # Transfer the file to the request machine
            c.send(f"{filename}|{filesize}")
            with open("hdfs_files/"+filename, "rb") as f:
                while True:
                    bytes_read = f.read(4096)
                    if not bytes_read:
                        break
                    c.sendall(bytes_read)
            c.close()

        tcp_socket_send.close()

        logging.info(f"Successfully wrote file: {filename} to nodes: {target_nodes}")

    def send_read_request(self, filename):
        """
        Send a read request to the master queue manager and wait for the response
        """
        request = {}
        request['op'] = 'read'
        request['sender_host'] = self.self_host
        request['addr'] = [self.self_host]
        request['filename'] = filename

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Read to {self.master_host} queued")

    def handle_read_request(self, request):
        """
        Send a file over to whatever machines are requesting it
        """
        request_nodes = request['addr']
        filename = request['filename']
        filesize = os.path.getsize("hdfs_files/"+filename)

        tcp_socket_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #tcp_socket_send.bind((socket.gethostname(), TCP_PORT))

        for request_node in request_nodes:
            c = tcp_socket_send.connect((request_node, TCP_PORT))
            c.send(f"{filename}|{filesize}")
            # Transfer the file to the request machine
            with open("hdfs_files/"+filename, "rb") as f:
                while True:
                    bytes_read = f.read(4096)
                    if not bytes_read:
                        break
                    c.sendall(bytes_read)
            c.close()
        tcp_socket_send.close()

        logging.info(f"Successfully sent file: {filename} to node: {request_nodes}")

    def handle_file_transfer(self, c):
        """
        This thread is started whenever this machine is being sent a file.
        Send an acknowledge message to the master node after the file is recieved
        """
        # Get file information
        received = c.recv(4096).decode()
        filename, filesize = received.split("|")
        filename = os.path.basename(filename)
        filesize = int(filesize)

        # Retrieve file
        with open("hdfs_files/"+filename, "wb") as f:
            while True:
                bytes_read = f.read(4096)
                if not bytes_read:
                    break
                f.write(bytes_read)
        c.close()

        # Send acknowledgement to the master node
        self.send_ack_message()
        logging.info(f"Write to {filename} complete")

    def send_ack_message(self):
        """
        Sends a generic ack message to the master node
        """
        request = {}
        request['op'] = 'ack'
        request['sender_host'] = self.self_host

        # Send acknowledgement
        message_data = json.dumps(request).encode()
        self.udp_socket.send(message_data, (self.master_host, QHANDLER_PORT))

    def listener_thread_TCP(self):
        """
        This thread is where all file transfers will occur
        """
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((socket.gethostname(), TCP_PORT))
        self.tcp_socket.listen(8)

        while True:
            c, addr = self.tcp_socket.accept()
            logging.info(f"Recieved TCP connection from {addr}")
            # Handle recieiving files here
            file_transfer_thread = threading.Thread(target=self.handle_file_transfer, args=(c,))
            file_transfer_thread.start()

    def listener_thread_UDP(self):
        """
        Listen for messages being sent to the queue handler
        """
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind((self.self_host, QHANDLER_PORT))

        while True:
            data, address = self.udp_socket.recvfrom(4096)
            request_json = parse_and_validate_message(data)

            if request_json['op'] == 'read':
                read_thread = threading.Thread(target=self.handle_read_request, args=(request_json,))
                read_thread.start()
            elif request_json['op'] == 'write':
                write_thread = threading.Thread(target=self.handle_write_request, args=(request_json,))
                write_thread.start()