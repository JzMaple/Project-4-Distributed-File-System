import os
import threading


class DataSever(threading.Thread):
    """Data Server: execute self.COMMAND from nameserver."""

    def __init__(self, server_id, cfg):
        super(DataSever, self).__init__(name='DataServer%s' % (server_id,))
        self.config = cfg
        self._server_id = server_id

    def run(self):
        config = self.config
        while True:
            config.data_events[self._server_id].wait()
            if config.cmd_flag:
                if config.cmd_type == self.config.COMMAND.upload and self._server_id in config.server_chunk_map:
                    self.save()
                elif config.cmd_type == self.config.COMMAND.read:
                    self.read()
                else:
                    pass
            config.data_events[self._server_id].clear()
            config.main_events[self._server_id].set()

    def save(self):
        """Data Sever save file"""
        data_node_dir = self.config.DATA_NODE_DIR % (self._server_id,)
        with open(self.config.file_path, 'r') as f_in:
            for chunk, offset, count in self.config.server_chunk_map[self._server_id]:
                f_in.seek(offset, 0)
                content = f_in.read(count)

                with open(data_node_dir + os.path.sep + chunk, 'w') as f_out:
                    f_out.write(content)
                    f_out.flush()

    def read(self):
        """read chunk according to offset and count"""

        read_path = (self.config.DATA_NODE_DIR % (self._server_id,)) + os.path.sep + self.config.read_chunk

        with open(read_path, 'r') as f_in:
            f_in.seek(self.config.read_offset)
            content = f_in.read(self.config.read_count)
            print(content)
        self.config.read_event.set()
