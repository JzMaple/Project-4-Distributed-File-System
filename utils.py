import os


def cmd_process(inputs, cfg):
    flag = True
    try:
        print("inputs", inputs)
        if inputs[0] == "upload":
            # upload --src=* --save=*
            assert len(inputs) == 5
            cfg.cmd_type = cfg.COMMAND.upload
            cfg.file_path = None
            cfg.save_path = None
            for i in range((len(inputs) - 1) // 2):
                command = inputs[i * 2 + 1]
                param = inputs[i * 2 + 2]
                if command == "--src":
                    cfg.file_path = param
                    if not os.path.exists(cfg.file_path):
                        print("No such source file. Please give a right source file path")
                        raise FileExistsError
                else:
                    assert command == "--save"
                    cfg.save_path = param
        elif inputs[0] == "read":
            # read --id=* --offset=* --size=*(optional)
            assert len(inputs) == 3 or len(inputs) == 5 or len(inputs) == 7
            cfg.cmd_type = cfg.COMMAND.read
            cfg.file_id = None
            cfg.read_offset = None
            cfg.read_count = None
            for i in range((len(inputs) - 1) // 2):
                command = inputs[i * 2 + 1]
                param = inputs[i * 2 + 2]
                if command == "--id":
                    cfg.file_id = param
                elif command == "--offset":
                    cfg.read_offset = int(param)
                else:
                    assert command == "--size"
                    cfg.read_count = int(param)
            assert cfg.file_id is not None
        elif inputs[0] == "fetch":
            # fetch --save=* --file_id=* --file_path=*
            assert len(inputs) == 5 or len(inputs) == 7
            cfg.cmd_type = cfg.COMMAND.fetch
            cfg.file_id = None
            cfg.file_path = None
            cfg.save_path = None
            for i in range((len(inputs) - 1) // 2):
                command = inputs[i * 2 + 1]
                param = inputs[i * 2 + 2]
                if command == "--file_path":
                    cfg.file_path = param
                elif command == "--save":
                    cfg.save_path = param
                else:
                    assert command == "--file_id"
                    cfg.file_id = param
            assert cfg.file_path is not None or cfg.file_id is not None
        elif inputs[0] == "quit":
            assert len(inputs) == 1
            cfg.cmd_type = cfg.COMMAND.quit
        elif inputs[0] == "mkdir":
            # mkdir --dir=*
            assert len(inputs) == 3
            cfg.cmd_type = cfg.COMMAND.mkdir
            assert inputs[1] == "--dir"
            cfg.file_dir = inputs[2]
        elif inputs[0] == "ls":
            assert len(inputs) == 1
            cfg.cmd_type = cfg.COMMAND.ls
        elif inputs[0] == "help":
            assert len(inputs) == 1
            cfg.cmd_type = cfg.COMMAND.help
            print("Usage:")
            print("upload file: upload --src=src_path --save=save_path")
            print("read file: read --id=file_id --offset=file_offset(default=0) --size=file_size(default=all)")
            print("fetch file: --save=save_path --file_id=file_id_to_fetch --file_path=file_id_to_fetch")
            print("make dir: mkdir --dir=dir_path")
            print("list all files: ls")
            print("quit system: quit")
            print("usage help: help")
    except Exception as _:
        print("Inputs are not valid. Please use 'help' for command usage information")
        flag = False

    return flag


def start_stop_info(operation, cfg):
    print(operation, 'Name Server')
    for i in range(cfg.NUM_DATA_SERVER):
        print(operation, 'Data Server' + str(i))
