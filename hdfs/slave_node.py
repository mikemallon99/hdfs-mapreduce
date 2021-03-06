from .master_node import parse_and_validate_message
from datetime import datetime
import json
import logging
import socket
import threading
import os

QMANAGER_PORT = 12345
QHANDLER_PORT = 12346
TCP_PORT = 12347
LS_PORT = 12348

class SlaveNode():
    def __init__(self, master_host, self_host):
        self.nodetable = {}
        self.filetable = {}
        self.master_host = master_host
        self.self_host = self_host
        self.tcp_socket = None
        self.udp_socket = None
        self.qman_socket = None
        self.master_backup_callback = None
        self.ack_counter = 0

        self.writes_queued = 0
        self.writes_queued_lock = threading.Lock()
        self.reads_queued = 0
        self.reads_queued_lock = threading.Lock()

    def start_slave(self):
        tcp_thread = threading.Thread(target=self.listener_thread_TCP)
        udp_thread = threading.Thread(target=self.listener_thread_UDP)
        tcp_thread.start()
        udp_thread.start()

        self.qman_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.qman_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.qman_socket.bind((self.self_host, QMANAGER_PORT))

        # self.f = open("read_times.txt", "a")

        logging.info("Started slave listeners")

    def stop_slave(self):
        self.tcp_socket.close()
        self.udp_socket.close()
        self.qman_socket.close()
        # self.f.close()
        logging.info("Stopping slave sockets")

    def update_new_master(self, new_master_host):
        self.master_host = new_master_host

    def get_writes_queued(self):
        return self.writes_queued

    def get_reads_queued(self):
        return self.reads_queued

    def send_ls_to_master(self, filename):
        """
        Sends a ls request to the master queue manager
        """
        request = {'op': 'ls', 'sender_host': self.self_host, 'filename': filename}
        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.debug("ls successfully queued")

    def send_delete_request(self, sdfsfilename):
        """
        Send a delete request to the master queue manager
        """
        request = {}
        request['op'] = 'delete'
        request['sender_host'] = self.self_host
        request['addr'] = [self.self_host]
        request['filename'] = sdfsfilename

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Delete to {self.master_host} queued")

    def send_store_request(self):
        """
        Send a delete request to the master queue manager
        """
        request = {}
        request['op'] = 'store'
        request['sender_host'] = self.self_host

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Store to {self.master_host} queued")
        
    def set_callback(self, func):
        self.master_backup_callback = func

    def send_write_request(self, localfilename, sdfsfilename):
        """
        Send a write request to the master queue manager
        """
        request = {}
        request['op'] = 'write'
        request['sender_host'] = self.self_host  # TODO == why need both?
        request['addr'] = [self.self_host]
        request['filename'] = sdfsfilename
        request['localfilename'] = localfilename
        request['timestamp'] = datetime.now().isoformat()

        try:
            filesize = os.path.getsize("hdfs_files/" + localfilename)
        except FileNotFoundError:
            logging.info(f"File {localfilename} not found")
            return

        # Add to write queue counter
        self.writes_queued_lock.acquire()
        self.writes_queued += 1
        self.writes_queued_lock.release()

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Write to {self.master_host} queued")

    def handle_write_request(self, request):
        """
        Write file the target machines
        """
        target_nodes = request['addr']
        localfilename = request['localfilename']
        sdfsfilename = request['filename']
        try:
            filesize = os.path.getsize("hdfs_files/" + localfilename)
        except FileNotFoundError:
            logging.info(f"File {localfilename} not found")
            return

        for target_node in target_nodes:
            tcp_socket_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            logging.info(f"Attempting to connect to:{target_node}")
            tcp_socket_send.connect((target_node, TCP_PORT))
            # Transfer the file to the request machine
            tcp_socket_send.send(f"{sdfsfilename}|{filesize}".ljust(4096).encode())
            with open("hdfs_files/"+localfilename, "rb") as f:
                while True:
                    bytes_read = f.read(4096)
                    if not bytes_read:
                        break
                    tcp_socket_send.sendall(bytes_read)
            tcp_socket_send.close()

        logging.info(f"Successfully wrote file: {localfilename} to nodes: {target_nodes}")
        og_time = request['timestamp']
        date_time_obj = datetime.strptime(og_time, '%Y-%m-%dT%H:%M:%S.%f')
        time_delta = datetime.now() - date_time_obj
        logging.info(f"Upload time: {time_delta}")

        # Remove from write queue counter
        self.writes_queued_lock.acquire()
        self.writes_queued -= 1
        self.writes_queued_lock.release()

        # Delete this after
        # self.f.write(f"{time_delta}\n")

    def send_read_request(self, localfilename, sdfsfilename):
        """
        Send a read request to the master queue manager and wait for the response
        """
        request = {}
        request['op'] = 'read'
        request['sender_host'] = self.self_host
        request['addr'] = [self.self_host]
        request['filename'] = sdfsfilename
        request['localfilename'] = localfilename

        # Add to read queue counter
        self.reads_queued_lock.acquire()
        self.reads_queued += 1
        self.reads_queued_lock.release()

        message_data = json.dumps(request).encode()
        self.qman_socket.sendto(message_data, (self.master_host, QMANAGER_PORT))
        logging.info(f"Read to {self.master_host} queued")

    def handle_read_request(self, request):
        """
        Send a file over to whatever machines are requesting it
        """
        request_nodes = request['addr']
        sdfsfilename = request['filename']
        localfilename = request['localfilename']
        try:
            filesize = os.path.getsize("hdfs_files/"+sdfsfilename)
        except FileNotFoundError:
            logging.info(f"File {sdfsfilename} does not exist")
            return

        for request_node in request_nodes:
            tcp_socket_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket_send.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_socket_send.connect((request_node, TCP_PORT))
            tcp_socket_send.send(f"{localfilename}|{filesize}".ljust(4096).encode())
            # Transfer the file to the request machine
            with open("hdfs_files/"+sdfsfilename, "rb") as f:
                while True:
                    bytes_read = f.read(4096)
                    if not bytes_read:
                        break
                    tcp_socket_send.sendall(bytes_read)
            tcp_socket_send.close()

        logging.info(f"Successfully sent file: {sdfsfilename} to node: {request_nodes}")


    def handle_file_transfer(self, c):
        """
        This thread is started whenever this machine is being sent a file.
        Send an acknowledge message to the master node after the file is recieved
        """
        # Get file information
        received = c.recv(4096).decode()
        filename, filesize= received.split("|")
        filename = os.path.basename(filename)
        filesize = int(filesize)

        timestamp1 = datetime.now()
        # Retrieve file
        with open("hdfs_files/"+filename, "wb") as f:
            while True:
                bytes_read = c.recv(4096)
                if not bytes_read:
                    break
                f.write(bytes_read)
        c.close()

        # Send acknowledgement to the master node
        self.send_ack_message()
        logging.info(f"Write to {filename} complete")

        time_delta = datetime.now() - timestamp1
        logging.info(f"Download time: {time_delta}")

        # Remove from read queue counter
        self.reads_queued_lock.acquire()
        self.reads_queued -= 1
        self.reads_queued_lock.release()

        # Delete this after
        # self.f.write(f"{time_delta}\n")

    def handle_store_response(self, request):
        file_list = request['filelist']
        if not file_list:
            logging.info("No files assigned to this node found in SDFS!")
            return
        logging.info("Files located at this node:")
        for file in file_list:
            logging.info(file)

    def handle_ls_response(self, request):
        file_list = request['filelist']
        if not file_list:
            logging.info("File not found in SDFS!")
            return
        logging.info("Found the file " + request['filename'] + " at nodes:")
        for file in file_list:
            logging.info(file)

    def send_ack_message(self):
        """
        Sends a generic ack message to the master node
        """
        request = {}
        request['op'] = 'ack'
        request['sender_host'] = self.self_host

        # Send acknowledgement
        message_data = json.dumps(request).encode()
        self.udp_socket.sendto(message_data, (self.master_host, QHANDLER_PORT))

    def listener_thread_TCP(self):
        """
        This thread is where all file transfers will occur
        """
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((socket.gethostname(), TCP_PORT))
        self.tcp_socket.listen(8)

        while True:
            try:
                c, addr = self.tcp_socket.accept()
                logging.info(f"Recieved TCP connection from {addr}")
                # Handle recieiving files here
                file_transfer_thread = threading.Thread(target=self.handle_file_transfer, args=(c,))
                file_transfer_thread.start()
            except OSError:
                logging.debug("Recieved message but socket is closed")
                break


    def listener_thread_UDP(self):
        """
        Listen for messages being sent to the queue handler
        """
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind((self.self_host, QHANDLER_PORT))

        while True:
            data, address = self.udp_socket.recvfrom(65536)
            request_json = parse_and_validate_message(data)

            if request_json['op'] == 'read':
                read_thread = threading.Thread(target=self.handle_read_request, args=(request_json,))
                read_thread.start()
            elif request_json['op'] == 'write':
                write_thread = threading.Thread(target=self.handle_write_request, args=(request_json,))
                write_thread.start()
            elif request_json['op'] == 'disp_ls':
                ls_thread = threading.Thread(target=self.handle_ls_response, args=(request_json,))
                ls_thread.start()
            elif request_json['op'] == 'store':
                ls_thread = threading.Thread(target=self.handle_store_response, args=(request_json,))
                ls_thread.start()
            elif request_json['op'] == 'backup_master':
                ret = self.master_backup_callback(request_json['nodetable'], request_json['filetable'])
                logging.debug(ret)
            elif request_json['op'] == 'failure':
                logging.info("File does not exist in SDFS")
