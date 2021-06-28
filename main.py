import os
import sys

from cfg import Config
from cmd import CMD
from utils import start_stop_info


def run():
    cmd = CMD()
    cfg = Config()
    start_stop_info('Start', cfg)

    # make dfs dir
    if not os.path.isdir("dfs"):
        os.makedirs("dfs")
        for i in range(4):
            os.makedirs("dfs/data-%d" % i)
        os.makedirs("dfs/name")

    # # start name and data servers
    # name_server = NameNode('NameServer', cfg)
    # name_server.start()
    #
    # data_servers = [DataNode(s_id, cfg) for s_id in range(NUM_DATA_SERVER)]
    # for server in data_servers:
    #     server.start()

    while True:
        cmd_str = input()
        cmd_str = cmd_str.split(" ")
        cmd_str = [_.split("=") for _ in cmd_str]
        cmd_str = [_ for cmd_list in cmd_str for _ in cmd_list]
        cfg.cmd_flag = cmd.process(cmd_str, cfg)

        if cfg.cmd_flag:
            if cfg.cmd_type == cmd.COMMAND.quit:
                start_stop_info('Start', cfg)
                sys.exit(0)

            if cfg.cmd_type == cmd.COMMAND.upload:
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].wait()
                print('Put succeed! File ID is %d' % (cfg.file_id,))
                cfg.server_chunk_map.clear()
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].clear()
            elif cfg.cmd_type == cmd.COMMAND.mkdir:
                cfg.mkdir_event.wait()
                cfg.mkdir_event.clear()
            elif cfg.cmd_type == cmd.COMMAND.read:
                cfg.read_event.wait()
                cfg.read_event.clear()
            elif cfg.cmd_type in [cmd.COMMAND.ls, cmd.COMMAND.Ls]:
                cfg.ls_event.wait()
                cfg.ls_event.clear()
            elif cfg.cmd_type in [cmd.COMMAND.fetch, cmd.COMMAND.Fetch]:
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].wait()
                if cfg.fetch_chunks > 0:
                    f_fetch = open(cfg.file_path, mode='wb')
                    for i in range(cfg.fetch_chunks):
                        server_id = cfg.fetch_servers[i]
                        chunk_file_path = "dfs/data-" + str(server_id) + "/" + str(cfg.file_id) + '-part-' + str(i)
                        chunk_file = open(chunk_file_path, "rb")
                        f_fetch.write(chunk_file.read())
                        chunk_file.close()
                    f_fetch.close()
                    print('Finished download!')
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].clear()
            else:
                pass


if __name__ == '__main__':
    run()
