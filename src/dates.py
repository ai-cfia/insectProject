from datetime import datetime, timedelta


def get_yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


if __name__ == "__main__":
    print("Yesterday's date:", get_yesterday())
