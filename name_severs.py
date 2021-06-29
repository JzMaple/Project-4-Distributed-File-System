import math
import random
import os
import pickle
import threading

from file_system import FileTree


class NameSever(threading.Thread):
    """
    Name Server，handle instructions and manage data servers
    Client can use `ls, read, fetch` cmds.
    """

    def __init__(self, name, cfg):
        super(NameSever, self).__init__(name=name)
        self.cfg = cfg  # global parameters
        self.metas = None
        self.id_chunk_map = None  # file id -> chunk, eg. {0: ['0-part-0'], 1: ['1-part-0']}
        self.id_file_map = None  # file id -> name, eg. {0: ('README.md', 1395), 1: ('mini_dfs.py', 14603)}
        self.chunk_server_map = None  # chunk -> data servers, eg. {'0-part-0': [0, 1, 2], '1-part-0': [0, 1, 2]}
        self.last_file_id = -1  # eg. 1
        self.last_data_server_id = -1  # eg. 2
        self.tree = None  # dir tree
        self.load_meta()

    def run(self):
        cfg = self.cfg
        while True:
            # waiting for cmds
            cfg.name_event.wait()
            print("name sever start")
            if cfg.cmd_flag:
                if cfg.cmd_type == self.cfg.COMMAND.upload:
                    self.upload()
                elif cfg.cmd_type == self.cfg.COMMAND.read:
                    self.read()
                elif cfg.cmd_type == self.cfg.COMMAND.fetch:
                    self.fetch()
                elif cfg.cmd_type == self.cfg.COMMAND.ls:
                    self.ls()
                elif cfg.cmd_type == self.cfg.COMMAND.mkdir:
                    print("dir", cfg.file_dir)
                    self.tree.insert(cfg.file_dir)
                    self.cfg.mkdir_event.set()
                else:
                    pass
            print("name sever sleep")
            cfg.name_event.clear()

    def load_meta(self):
        """load Name Node Meta Data"""

        if not os.path.isfile(self.cfg.NAME_NODE_META_PATH):
            self.metas = {
                'id_chunk_map': {},
                'id_file_map': {},
                'chunk_server_map': {},
                'last_file_id': -1,
                'last_data_server_id': -1,
                'tree': FileTree()
            }
        else:
            with open(self.cfg.NAME_NODE_META_PATH, 'rb') as f:
                self.metas = pickle.load(f)
        self.id_chunk_map = self.metas['id_chunk_map']
        self.id_file_map = self.metas['id_file_map']
        self.chunk_server_map = self.metas['chunk_server_map']
        self.last_file_id = self.metas['last_file_id']
        self.last_data_server_id = self.metas['last_data_server_id']
        self.tree = self.metas['tree']

    def update_meta(self):
        """update Name Node Meta Data after upload"""

        with open(self.cfg.NAME_NODE_META_PATH, 'wb') as f:
            self.metas['last_file_id'] = self.last_file_id
            self.metas['last_data_server_id'] = self.last_data_server_id
            pickle.dump(self.metas, f)

    def ls(self):
        """ls print meta data info"""
        print('total', len(self.id_file_map))
        print("file dir tree:")
        self.tree.view(self.id_file_map)
        for file_id, (file_name, file_len) in self.id_file_map.items():
            print(self.cfg.LS_PATTERN % (file_id, file_name, file_len))
        self.cfg.ls_event.set()

    def upload(self):
        """split input file into chunk, then sent to different chunks"""

        in_path = self.cfg.file_path
        print("in_path", in_path)

        file_name = in_path.split('/')[-1]
        self.last_file_id += 1
        print("file_name", file_name)

        # update file tree
        if self.cfg.cmd_type == self.cfg.COMMAND.upload:
            dir = self.cfg.save_path
            if dir[0] == '/':
                dir = dir[1:]
            if dir[-1] == '/':
                dir = dir[:-1]
            self.tree.insert(dir, self.last_file_id)
            # self.tree.add(self.last_file_id)

        server_id = (self.last_data_server_id + 1) % self.cfg.NUM_REPLICATION
        print("server_id", server_id)

        file_length = os.path.getsize(in_path)
        chunks = int(math.ceil(float(file_length) / self.cfg.CHUNK_SIZE))
        print("chunks", chunks)

        # generate chunk, add into <id, chunk> mapping
        self.id_chunk_map[self.last_file_id] = [self.cfg.CHUNK_PATTERN % (self.last_file_id, i) for i in range(chunks)]
        self.id_file_map[self.last_file_id] = (file_name, file_length)

        for i, chunk in enumerate(self.id_chunk_map[self.last_file_id]):
            self.chunk_server_map[chunk] = []

            # copy to 4 data nodes
            for j in range(self.cfg.NUM_REPLICATION):
                assign_server = (server_id + j) % self.cfg.NUM_DATA_SERVER
                print("assign_server", assign_server)
                self.chunk_server_map[chunk].append(assign_server)

                # add chunk-server info to global variable
                size_in_chunk = self.cfg.CHUNK_SIZE if i < chunks - 1 else file_length % self.cfg.CHUNK_SIZE
                if assign_server not in self.cfg.server_chunk_map:
                    self.cfg.server_chunk_map[assign_server] = []
                self.cfg.server_chunk_map[assign_server].append((chunk, self.cfg.CHUNK_SIZE * i, size_in_chunk))

            server_id = (server_id + self.cfg.NUM_REPLICATION) % self.cfg.NUM_DATA_SERVER

        self.last_data_server_id = (server_id - 1) % self.cfg.NUM_DATA_SERVER
        self.update_meta()

        self.cfg.file_id = self.last_file_id
        for data_event in self.cfg.data_events:
            data_event.set()

    def read(self):
        """assign read mission to each data node"""

        cfg = self.cfg
        file_id = cfg.file_id

        # find file_id
        try:
            file_id = int(file_id)
        except ValueError:
            file_id = file_id
        if type(file_id) == str:
            file_dir = file_id
            file_id = self.tree.get_id_by_path(file_dir, self.id_file_map)
            cfg.file_id = file_id
            if file_id < 0:
                print('No such file:', file_dir)
                cfg.read_event.set()
                return False

        if cfg.read_offset is not None:
            read_offset = cfg.read_offset
        else:
            read_offset = 0
        if cfg.read_count is not None:
            read_count = cfg.read_count
        else:
            read_count = self.id_file_map[file_id][1] - read_offset

        if file_id not in self.id_file_map:
            print('No such file with id =', file_id)
            cfg.read_event.set()
        elif read_offset < 0:
            print('Read offset cannot less than 0')
            cfg.read_event.set()
        elif (read_offset + read_count) > self.id_file_map[file_id][1]:
            print('The expected reading exceeds the file, file size:', self.id_file_map[file_id][1])
            cfg.read_event.set()
        else:
            start_chunk = int(math.floor(read_offset / self.cfg.CHUNK_SIZE))
            space_left_in_chunk = (start_chunk + 1) * self.cfg.CHUNK_SIZE - read_offset

            if space_left_in_chunk < read_count:
                print('Cannot read across chunks')
                cfg.read_event.set()
            else:
                # randomly select a data server to read chunk
                read_server_candidates = self.chunk_server_map[self.cfg.CHUNK_PATTERN % (file_id, start_chunk)]
                read_server_id = random.choice(read_server_candidates)
                cfg.read_chunk = self.cfg.CHUNK_PATTERN % (file_id, start_chunk)
                cfg.read_offset = read_offset - start_chunk * self.cfg.CHUNK_SIZE
                cfg.data_events[read_server_id].set()
                print('Read file successfully!')
                return True

        return False

    def fetch(self):
        """assign download mission"""

        cfg = self.cfg
        file_id = cfg.file_id

        # find file_id
        if file_id is None:
            assert cfg.file_path is not None
            file_dir = cfg.file_path
            file_id = self.tree.get_id_by_path(file_dir, self.id_file_map)
            cfg.file_id = file_id
            if file_id < 0:
                cfg.fetch_chunks = -1
                print('No such file:', file_dir)
                for data_event in cfg.data_events:
                    data_event.set()
                return None
        else:
            try:
                file_id = int(file_id)
            except ValueError:
                file_id = file_id

        if file_id not in self.id_file_map:
            cfg.fetch_chunks = -1
            print('No such file with id =', file_id)
        else:
            file_chunks = self.id_chunk_map[file_id]
            # print(self.id_chunk_map)
            cfg.fetch_chunks = len(file_chunks)
            # get file's data server
            for chunk in file_chunks:
                cfg.fetch_servers.append(self.chunk_server_map[chunk][0])
            for data_event in cfg.data_events:
                data_event.set()
            return True

        for data_event in cfg.data_events:
            data_event.set()
        # print(cfg.fetch_chunks)
        return None
