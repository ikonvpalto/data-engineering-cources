from datetime import datetime, timedelta

DATE_FORMAT = "%Y-%m-%d"


def get_date_range(start_date, end_date):
    days_between = (end_date - start_date).days + 1
    return [start_date + timedelta(days=days) for days in range(0, days_between)]


def dump_date(date_string):
    return datetime.strftime(date_string, DATE_FORMAT)
