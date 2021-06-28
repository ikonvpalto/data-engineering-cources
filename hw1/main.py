from stock_api import authorize, get_out_of_stock_info
from date_utils import get_date_range
from config import get_config
from store import store_out_of_stock_info


def main():
    config = get_config()
    authorize_data = authorize(config)

    for date in get_date_range(config["dates_of_interest"]["start"], config["dates_of_interest"]["end"]):
        out_of_stock_info = get_out_of_stock_info(config, authorize_data, date)
        store_out_of_stock_info(config, out_of_stock_info, date)


if __name__ == "__main__":
    main()
