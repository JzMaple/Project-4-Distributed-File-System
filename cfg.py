import threading
from enum import Enum


class Config(object):
    # DFS Meta Data
    CHUNK_SIZE = 2 * 1024 * 1024
    NUM_DATA_SERVER = 4
    NUM_REPLICATION = 4
    CHUNK_PATTERN = '%s-part-%s'

    # Name Node Meta Data
    NAME_NODE_META_PATH = './dfs/name/meta.pkl'

    # Data Node
    DATA_NODE_DIR = './dfs/data-%s'
    LS_PATTERN = '%s\t%20s\t%10s'

    # Operations
    operation_names = ('upload', 'read', 'fetch', 'quit', 'mkdir', "ls", "help")
    COMMAND = Enum('COMMAND', operation_names)

    def __init__(self):
        self.server_chunk_map = {}  # datanode -> chunks
        self.read_chunk = None
        self.read_offset = None
        self.read_count = None

        self.cmd_flag = False
        self.cmd_type = None
        self.file_id = None
        self.file_dir = None  # read or fetch using dir/filename
        self.file_path = None  # local source path

        self.save_path = None  # put2 save using dir on DFS
        self.fetch_path = None  # download on local
        self.fetch_servers = []
        self.fetch_chunks = None

        # events
        self.name_event = threading.Event()
        self.ls_event = threading.Event()
        self.read_event = threading.Event()
        self.mkdir_event = threading.Event()

        self.data_events = [threading.Event() for _ in range(self.NUM_DATA_SERVER)]
        self.main_events = [threading.Event() for _ in range(self.NUM_DATA_SERVER)]


