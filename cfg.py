import threading


class Config(object):
    CHUNK_SIZE = 2 * 1024 * 1024
    NUM_DATA_SERVER = 4
    NUM_REPLICATION = 4

    server_chunk_map = {}  # datanode -> chunks
    read_chunk = None
    read_offset = None
    read_count = None

    cmd_flag = False
    cmd_type = None
    file_id = None
    file_dir = None  # read or fetch using dir/filename
    file_path = None  # local source path

    save_path = None  # put2 save using dir on DFS
    fetch_path = None  # download on local
    fetch_servers = []
    fetch_chunks = None

    # events
    name_event = threading.Event()
    ls_event = threading.Event()
    read_event = threading.Event()
    mkdir_event = threading.Event()

    data_events = [threading.Event() for _ in range(NUM_DATA_SERVER)]
    main_events = [threading.Event() for _ in range(NUM_DATA_SERVER)]
