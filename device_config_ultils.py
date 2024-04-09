import json
import config

def get_local_device_config():
    device_config = list()
    with open('dev_config.json', 'r', encoding='utf-8') as file:
        device_config = json.load(file)
    return device_config

def write_device_config_list_to_local_file(local_device_config):
    with open(config.device_config_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(local_device_config, indent=2))