import yaml

CONFIG_PATH = "config.yaml"


def get_config():
    config = parse_config_file()
    return config


def parse_config_file():
    with open(CONFIG_PATH, "r") as config_file:
        return yaml.load(config_file, Loader=yaml.FullLoader)
