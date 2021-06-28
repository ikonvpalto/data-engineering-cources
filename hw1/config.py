import yaml
import sys
from datetime import datetime
from date_utils import is_valid_date, parse_date

CONFIG_PATH = "config.yaml"


def get_config():
    config = parse_config_file()
    dates_of_interest = get_dates_of_interest()
    config["dates_of_interest"] = {"start": dates_of_interest[0], "end": dates_of_interest[1]}
    return config


def parse_config_file():
    with open(CONFIG_PATH, "r") as config_file:
        return yaml.load(config_file, Loader=yaml.FullLoader)


def get_dates_of_interest():
    start_date = datetime.today()
    if len(sys.argv) >= 2 and is_valid_date(sys.argv[1]):
        start_date = parse_date(sys.argv[1])

    end_date = start_date
    if len(sys.argv) >= 3 and is_valid_date(sys.argv[2]) and parse_date(sys.argv[2]) > end_date:
        end_date = parse_date(sys.argv[2])

    return start_date, end_date
