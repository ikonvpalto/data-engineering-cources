import json
import requests
from date_utils import dump_date


def authorize(config):
    url = prepare_url(config, "auth")
    headers = prepare_headers()
    data = json.dumps(config["auth_data"])

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()

    return "JWT " + response.json()["access_token"]


def get_out_of_stock_info(config, authorize_data, date):
    url = prepare_url(config, "out_of_stock")
    headers = prepare_headers(authorize_data)
    data = json.dumps({"date": dump_date(date)})

    response = requests.get(url, headers=headers, data=data)
    response.raise_for_status()

    return response.json()


def prepare_url(config, endpoint):
    return config["url"] + config["endpoints"][endpoint]


def prepare_headers(authorization_data=None):
    headers = {"content-type": "application/json"}
    if authorization_data is not None:
        headers["Authorization"] = authorization_data
    return headers
