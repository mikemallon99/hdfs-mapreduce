import re
import time
import copy
import threading
import logging
from enum import Enum
from .utils import get_display_id, log_to_file, get_hostname
from typing import Iterator, Tuple


class Status(Enum):
    ALIVE = "ALIVE"
    FAILED = "FAILED"
    LEFT = "LEFT"


class Row:
    def __init__(
        self, host: str, port: int, seqnum: int = 0, status: Status = Status.ALIVE
    ):
        """
        :param host: Hostname of the node
        :param port: Port that the node is listening on
        """
        self.host = host
        self.port = port
        self.seqnum = seqnum
        self.status = status
        self.timestamp = time.time()

    @staticmethod
    def from_dict(d) -> "Row":
        rv = Row(
            host=d["host"],
            port=d["port"],
            seqnum=d["seqnum"],
            status=Status[d["status"]],
        )
        return rv

    def to_dict(self) -> dict:
        rv = copy.deepcopy(self.__dict__)
        rv[
            "status"
        ] = self.status.value  # Turn status into a string (so it can be serialized)
        del rv["timestamp"]  # Timestamp is local per machine, so don't send it.
        return rv

    def __repr__(self) -> str:
        # TODO: Make this prettier
        # return f"(Host: {self.host}; Port: {self.port}; Seqnum: {self.seqnum}; TS: {self.timestamp}; Status: {self.status})"
        return f"{self.status.value}, Seqnum: {self.seqnum}"


class MembershipList:
    def __init__(self):
        self.nodes = {}
        self.lock = threading.Lock()
        self.failure_callback = None

    def set_callback(self, func):
        self.failure_callback = func

    def add_node(self, id: str, host: str, port: int):
        """
        :param id: Id of the node being added
        :param id: Hostname of the node being added
        :param id: Port that the node is listening on
        """
        with self.lock:
            if id in self.nodes:
                raise Exception(f"{id} already in Membership List!")
            self.nodes[id] = Row(host, port)
        if get_hostname() in id:
            log_to_file(f"Node (me) {id} joined the group.")
        else:
            log_to_file(f"Node {id} joined the group.")

    def clear(self):
        with self.lock:
            self.nodes.clear()

    def update_state(
        self, id: str, status: Status, seqnum: int, timestamp: float = 0.0
    ):
        """
        :param id: Id of the node to update
        :param status: New status of node <id>, should be from the Status enum
        :param seqnum: New sequence number of node <id>
        :param timestamp: New timestamp of node <id>. Defaults to the current time (in sec).
        """
        if timestamp == 0.0:
            timestamp = time.time()

        with self.lock:
            self.nodes[id].seqnum = seqnum
            self.nodes[id].timestamp = timestamp
            self.nodes[id].status = status

    def update_from_dict(self, other_mem_list_dict: dict):
        """
        :param other_mem_list: Other membership list to copy values over from
        """
        for id, row_dict in other_mem_list_dict.items():
            with self.lock:
                self.nodes[id] = Row.from_dict(row_dict)

    def mark_left(self, id: str):
        with self.lock:
            self.nodes[id].status = Status.LEFT
        self.failure_callback(id, True)
        if get_hostname() in id:
            log_to_file(f"Node (me) {id} left the group.")
        else:
            log_to_file(f"Node {id} left the group.")

    def mark_failed(self, id: str):
        with self.lock:
            self.nodes[id].status = Status.FAILED
        self.failure_callback(id)
        log_to_file(f"Node {id} failed.")

    def contains_node(self, id: str) -> bool:
        with self.lock:
            return id in self.nodes

    def is_alive(self, id: str):
        with self.lock:
            return self.nodes[id].status == Status.ALIVE

    def get_seqnum(self, id: str) -> int:
        with self.lock:
            return self.nodes[id].seqnum

    def to_dict(self) -> dict:
        with self.lock:
            result = {}
            for id, row in self.nodes.items():
                result[id] = row.to_dict()
            return result

    def get_alive_nodes_not_me(self, my_id) -> dict:
        with self.lock:
            result = {}
            for id, row in self.nodes.items():
                if row.status == Status.ALIVE and id != my_id:
                    result[id] = row.to_dict()
            return result

    def get_alive_nodes_ip_not_me(self) -> list:
        with self.lock:
            result = []
            for id, row in self.nodes.items():
                logging.info(f"Iterating on id: {id}")
                if row.status == Status.ALIVE:
                    result.append(id.split(":")[0])
            return result

    def get_most_recent_node(self) -> dict:
        logging.info(f"Iterating on id before lock")
        with self.lock:
            ret_node = {}
            logging.info(f"Iterating on id after lock")
            for id, row in self.nodes.items():
                logging.info(f"Iterating on id")
                if row.status == Status.ALIVE:
                    ret_node[id] = row.to_dict()
            return ret_node

    def __getitem__(self, idx) -> Row:
        with self.lock:
            return self.nodes[idx]

    def __repr__(self) -> str:
        with self.lock:
            ret = []
            for node_id, row in self.nodes.items():
                node_id = get_display_id(node_id)
                ret.append(f"{node_id}: {{{row}}}")
            return "\n".join(ret)

    def __iter__(self) -> Iterator[Tuple[str, Row]]:
        with self.lock:
            dict_copy = copy.deepcopy(self.nodes)
        for id, row in dict_copy.items():
            yield id, row
