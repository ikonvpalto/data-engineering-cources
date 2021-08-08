from stock_api import authorize, get_out_of_stock_info
from date_utils import get_date_range, DATE_FORMAT
from config import get_config
from store import store_out_of_stock_info
import click


@click.command()
@click.argument("start_date", type=click.DateTime())
@click.argument("end_date", type=click.DateTime())
def main(start_date, end_date):
    config = get_config()
    authorize_data = authorize(config)

    for date in get_date_range(start_date, end_date):
        out_of_stock_info = get_out_of_stock_info(config, authorize_data, date)
        store_out_of_stock_info(config, out_of_stock_info, date)


if __name__ == "__main__":
    main()
