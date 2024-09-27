import json
import os

import faker
from faker.providers import DynamicProvider
from datetime import datetime , timedelta


def get_periods_list(year, month, day):
    start_of_day = datetime(year, month, day)
    for i in range(24):
        start_of_hour = start_of_day + timedelta(hours=i)
        end_of_hour = start_of_hour + timedelta(hours=1) - timedelta(milliseconds=1)
        start_ts = int(start_of_hour.timestamp() * 1000)
        end_ts = int(end_of_hour.timestamp() * 1000)
        name = f"{start_of_hour.strftime('%Y%m%d%H')}0000"
        yield start_ts, end_ts, name



symbol_provider = DynamicProvider(
    provider_name='symbol',
    elements=['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'FB', 'NVDA', 'INTC', 'CSCO', 'ADBE']
)

fake = faker.Faker()
fake.add_provider(symbol_provider)

if __name__ == "__main__":
    out_put_path = "dummy-data"
    date = ["21-09-2024", "22-09-2024", "23-09-2024"]
    for d in date:
        if not os.path.exists(f"{out_put_path}/{d}"):
            os.makedirs(f"{out_put_path}/{d}")
        day, month, year = map(int, d.split("-"))
        for period in get_periods_list(year, month, day):
            start_ts, end_ts, name = period
            with open(f"{out_put_path}/{d}/{name}.ndjson", 'w') as f:
                cur_ts = start_ts
                while cur_ts < end_ts:
                    data = {
                        "symbol": fake.symbol(),
                        "price": fake.random_int(100, 1000),
                        "volume": fake.random_int(1000, 10000),
                        "timestamp": cur_ts
                    }
                    f.write(json.dumps(data) + "\n")
                    cur_ts += fake.random_int(500, 2000)