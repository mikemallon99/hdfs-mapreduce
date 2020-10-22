import re
import socket
from datetime import datetime

LOG_FILE_NAME = "distributed.log"

def generate_id(host: str, port: int) -> str:
    now = datetime.now().strftime("%H:%M:%S")
    node_id = f"{host}:{port}-{now}"
    return node_id


def get_hostname() -> str:
    rv = socket.gethostname()
    # Super stupid fix to make testing on my PC work
    if rv == "pop-os":
        rv = "localhost"
    return rv


def get_host_number_id(node_id) -> str:
    res = re.match(r"fa20-cs425-g\d\d-(\d+).cs.illinois.edu", node_id)
    if res:
        return f"{res.groups()[0]}"
    return node_id

def get_display_id(node_id) -> str:
    res = re.match(r"fa20-cs425-g\d\d-(\d+).cs.illinois.edu:\d+-(\d\d:\d\d:\d\d)", node_id)
    if res:
        id_num = res.groups()[0]
        timestamp = res.groups()[1]
        return f"{id_num} ({timestamp})"
    return node_id

def log_to_file(content):
    with open(LOG_FILE_NAME, "a+") as file:
        now = datetime.now().strftime("%c")
        file.write(f"{now}: {content}\n")