import json
from os import path, makedirs
from date_utils import dump_date

file_name = "out_of_stock.json"


def store_out_of_stock_info(config, out_of_stock_info, date):
    directory_path = path.join(config["data_directory"], dump_date(date))
    makedirs(directory_path, exist_ok=True)

    file_path = path.join(directory_path, file_name)
    with open(file_path, "w") as data_file:
        json.dump(out_of_stock_info, data_file)
