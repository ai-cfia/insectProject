from datetime import datetime, timedelta

from pydantic import validate_call


def get_yesterday():
    return (datetime.now() - timedelta(days=1)).date()


@validate_call
def get_recent_dates(n_days: int):
    yesterday = get_yesterday()
    return [yesterday - timedelta(days=i) for i in reversed(range(n_days))]


if __name__ == "__main__":
    # run with `python -m src.dates`
    print("Yesterday's date:", get_yesterday())
