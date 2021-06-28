def start_stop_info(operation, cfg):
    print(operation, 'Name Server')
    for i in range(cfg.NUM_DATA_SERVER):
        print(operation, 'Data Server' + str(i))