import os
import sys

from cfg import Config
from utils import start_stop_info, cmd_process
from data_severs import DataSever
from name_severs import NameSever


def run():
    cfg = Config()
    start_stop_info('Start', cfg)

    # make dfs dir
    if not os.path.isdir("dfs"):
        os.makedirs("dfs")
        for i in range(4):
            os.makedirs("dfs/data-%d" % i)
        os.makedirs("dfs/name")

    # start name and data servers
    name_server = NameSever('NameServer', cfg)
    name_server.start()

    data_servers = [DataSever(s_id, cfg) for s_id in range(cfg.NUM_DATA_SERVER)]
    for server in data_servers:
        server.start()

    while True:
        print("**************************************************************")
        print("Usage: upload|read|fetch|quit|mkdir")
        print("Please input your command:")
        cmd_str = input()
        cmd_str = cmd_str.split(" ")
        cmd_str = [_.split("=") for _ in cmd_str]
        cmd_str = [_ for cmd_list in cmd_str for _ in cmd_list]
        cfg.cmd_flag = cmd_process(cmd_str, cfg)
        cfg.name_event.set()

        if cfg.cmd_flag:
            if cfg.cmd_type == cfg.COMMAND.quit:
                start_stop_info('Stop', cfg)
                print("Bye: Exiting miniDFS...")
                os._exit(0)
                sys.exit(0)

            if cfg.cmd_type == cfg.COMMAND.upload:
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].wait()
                print('Upload succeed! File ID is %d' % (cfg.file_id,))
                cfg.server_chunk_map.clear()
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].clear()
            elif cfg.cmd_type == cfg.COMMAND.mkdir:
                cfg.mkdir_event.wait()
                cfg.mkdir_event.clear()
            elif cfg.cmd_type == cfg.COMMAND.read:
                cfg.read_event.wait()
                cfg.read_event.clear()
            elif cfg.cmd_type == cfg.COMMAND.ls:
                cfg.ls_event.wait()
                cfg.ls_event.clear()
            elif cfg.cmd_type == cfg.COMMAND.fetch:
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].wait()
                if cfg.fetch_chunks > 0:
                    try:
                        f_fetch = open(cfg.save_path, mode='wb')
                        for i in range(cfg.fetch_chunks):
                            server_id = cfg.fetch_servers[i]
                            chunk_file_path = "dfs/data-" + str(server_id) + "/" + str(cfg.file_id) + '-part-' + str(i)
                            chunk_file = open(chunk_file_path, "rb")
                            f_fetch.write(chunk_file.read())
                            chunk_file.close()
                        f_fetch.close()
                        print('Finished download!')
                    except Exception as e:
                        print(e)
                for i in range(cfg.NUM_DATA_SERVER):
                    cfg.main_events[i].clear()


if __name__ == '__main__':
    run()
