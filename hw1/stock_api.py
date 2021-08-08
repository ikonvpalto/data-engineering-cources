import json
import requests
from date_utils import dump_date


def authorize(config):
    endpoint = "auth"

    url = prepare_url(config, endpoint)
    headers = prepare_headers()
    data = json.dumps(config["auth_data"])

    response = requests.post(url, headers=headers, data=data, timeout=config["endpoints"][endpoint]["timeout"])
    response.raise_for_status()

    return "JWT " + response.json()["access_token"]


def get_out_of_stock_info(config, authorize_data, date):
    endpoint = "out_of_stock"

    url = prepare_url(config, endpoint)
    headers = prepare_headers(authorize_data)
    data = json.dumps({"date": dump_date(date)})

    response = requests.get(url, headers=headers, data=data, timeout=config["endpoints"][endpoint]["timeout"])
    response.raise_for_status()

    return response.json()


def prepare_url(config, endpoint):
    return config["url"] + config["endpoints"][endpoint]["address"]


def prepare_headers(authorization_data=None):
    headers = {"content-type": "application/json"}
    if authorization_data is not None:
        headers["Authorization"] = authorization_data
    return headers
