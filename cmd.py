import os
from enum import Enum


def cmd_process(inputs, cfg):
    flag = True
    print("Usage: upload|read|fetch|quit|mkdir")
    try:
        if inputs[0] == "upload":
            # upload --src=* --save=*
            assert len(inputs) == 5
            cfg.cmd_type = cfg.COMMAND.upload
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
            assert len(inputs) == 5 or len(inputs) == 7
            cfg.cmd_type = cfg.COMMAND.read
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
        elif inputs[0] == "fetch":
            # fetch --path=* --id=*
            assert len(inputs) == 5
            cfg.cmd_type = cfg.COMMAND.fetch
            for i in range((len(inputs) - 1) // 2):
                command = inputs[i * 2 + 1]
                param = inputs[i * 2 + 2]
                if command == "--path":
                    cfg.file_path = param
                    if not os.path.exists(cfg.file_path):
                        print("No such file. Please give a right file path to fetch")
                        raise FileExistsError
                else:
                    assert command == "--id"
                    cfg.file_id = param
        elif inputs[0] == "quit":
            assert len(inputs) == 0
            cfg.cmd_type = cfg.COMMAND.quit
        elif inputs[0] == "mkdir":
            # mkdir --dir=*
            assert len(inputs) == 3
            cfg.cmd_type = cfg.COMMAND.mkdir
            assert inputs[1] == "--dir"
            cfg.file_dir = inputs[2]
        elif inputs[0] == "help":
            assert len(inputs) == 0
            cfg.cmd_type = cfg.COMMAND.help
    except Exception as _:
        print("Inputs are not valid. Please use 'help' for commond usage information")
        flag = False

    return flag
