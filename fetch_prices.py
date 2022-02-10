import csv
from binance.client import Client

# key reader
with open("keys.txt", "r") as file:
    lines = file.readlines()
    lines = [line.split("\"")[1] for line in lines]
    api_key, secret_api_key = lines[0], lines[1]

client = Client(api_key, secret_api_key)  # create a client instance


def get_price(pair):
    # get 1minute ohlcv(open high low close values) values with unix timestamps since 01 Jan 2021
    return client.get_historical_klines(pair, Client.KLINE_INTERVAL_1MINUTE, "01 Jan 2021")


def save_prices(filename, pair):
    price_data = get_price(pair)  # get price data of bitcoin/usd_tether pair
    with open(f"{filename}.csv", "w", newline="") as csv_file:  # save all data to a csv file
        line_writer = csv.writer(csv_file, delimiter=",")
        for data in price_data:
            line_writer.writerow(data)
    csv_file.close()


currency_pairs = ["BTCUSDT", "DOGEUSDT"]  # Other example coin pairs with usdt: DOGEUSDT, ETHUSDTH, ...

for p in currency_pairs:  # iterate over pairs and save their data to csv files
    pair_name = p[:-4].lower() + "_" + p[-4:].lower()
    save_prices(f"{pair_name}_prices", p)
