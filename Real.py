import pandas as pd
import requests
import time

pair = "KRW-BTC"
depth = 5

url = f"https://api.upbit.com/v1/orderbook?markets={pair}&orderbook_units={depth}"

df = pd.DataFrame(columns=["timestamp", "price", "quantity", "type"])

total_seconds = 48 * 60 * 60  # 48 hours

interval_seconds = 1

start_time = time.time()
while time.time() < start_time + total_seconds:
    response = requests.get(url)
    data = response.json()[0]["orderbook_units"]
    timestamp = int(time.time() - start_time)
    
    for i in range(depth):
        bid_price = float(data[i]["bid_price"])
        bid_quantity = float(data[i]["bid_size"])
        ask_price = float(data[i]["ask_price"])
        ask_quantity = float(data[i]["ask_size"])
        row = [timestamp, bid_price, bid_quantity, 0]
        df.loc[len(df)] = row
        row = [timestamp, ask_price, ask_quantity, 1]
        df.loc[len(df)] = row
    
    time.sleep(interval_seconds)

df.to_csv(f"{pair}_orderbook.csv", index=False)
