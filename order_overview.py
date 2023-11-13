from pybit.unified_trading import HTTP
import time
import pandas as pd
import datetime as dt
import json
from pathlib import Path
import cli_inputs


def get_credentials(account):
    root = Path(".")
    file_path = f"{root}/credentials.json"

    with open(file_path) as file:

        file = file.read()
        credentials = json.loads(file)

        api_key = credentials[account]["bybit_api_key"]
        api_secret = credentials[account]["bybit_secret_key"]

    return api_key, api_secret


def auth(api_key, api_secret):
    bybit_client = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)

    return bybit_client


def get_all_spot_positions(client):
    balances = client.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]["coin"]

    spot_positions = []

    for balance in balances:
        coin = balance["coin"]
        usd_value = round(float(balance["usdValue"]))
        coins = float(balance["walletBalance"])

        if usd_value > 10:
            spot_positions.append([coin, usd_value, coins])

    if spot_positions:
        print("\n")
        print("Current spot positions")
        positions_df = pd.DataFrame(spot_positions, columns=["coin", "usdValue", "coinAmount"])
        positions_df["coinAmount"] = positions_df["coinAmount"].astype(str)
        print(positions_df.to_markdown(floatfmt=''))
        print("\n")
    else:
        print("No spot positions")


def get_open_orders(client, spot:bool):
    """
    returns all sitting orders and their avg price

    :param client:
    :param spot:
    :return:
    """

    if spot:
        category = "spot"
    else:
        category = "linear"

    if category == "linear":
        settleCoin = "USDT"

    next_page_cursor = ""
    order_data_list = []
    while True:
        if category == "spot":
            orders = client.get_open_orders(category=category, limit=50, cursor=next_page_cursor)["result"]
        elif category == "linear":
            orders = client.get_open_orders(category=category, settleCoin="USDT" ,limit=50, cursor=next_page_cursor)["result"]

        next_page_cursor = orders["nextPageCursor"]
        order_data = orders["list"]
        order_data_list.append(order_data)
        if not order_data:
            break

    orders_by_ticker = {}
    for order_list in order_data_list:
        for order in order_list:
            if order["symbol"] not in orders_by_ticker.keys():
                orders_by_ticker[order["symbol"]] = {"Buy": [], "Sell": []}
                orders_by_ticker[order["symbol"]] = {"Buy": [], "Sell": []}

            if order["orderType"] == "Limit":
                orders_by_ticker[order["symbol"]][order["side"]].append(order)

    if orders_by_ticker:
        open = []
        for key in orders_by_ticker.keys():
            for side in orders_by_ticker[key].keys():
                ticker_orders = orders_by_ticker[key][side]
                price_times_qty = 0
                qty_sum = 0
                coin_qty_sum = 0
                if ticker_orders:
                    for order in ticker_orders:
                        usd_val_open = round(float(order["leavesValue"]), 1)
                        price = float(order["price"])
                        coin_qty_open = float(order["leavesQty"])

                        price_times_qty += usd_val_open * price
                        qty_sum += usd_val_open
                        coin_qty_sum += coin_qty_open

                    weighted_avg_price = price_times_qty / qty_sum
                    # print(f"{key} {side} side filled: {qty_sum} USDT at: {weighted_avg_price}")
                    open.append([key, side, weighted_avg_price, coin_qty_sum, weighted_avg_price * coin_qty_sum])
                else:
                    # print(f"{key} no {side} side orders filled")
                    pass

        filled_df = pd.DataFrame(open, columns=["ticker", "side", "avg_price", "unfilled qty[coins]", "unfilled usdt value"])
        filled_df.set_index("ticker", inplace=True)
        print(f"Open limit orders || category: {category}")
        print(filled_df.to_markdown())

    else:
        print(f"no Limit orders for {category} category")


def get_filled_orders_by_hours(client, hours_back:int, spot:bool):
    """
    returns filled orders with respective avg filled price

    :param hours_back:
    :param spot: True: return spot filled orders, False: returns derivs filled orders
    :return:
    """


    end = dt.datetime.now()
    start = end - dt.timedelta(hours=hours_back)

    end_ts = int(end.timestamp()*1000)
    start_ts = int(start.timestamp()*1000)

    if spot:
        category = "spot"
    else:
        category = "linear"

    next_page_cursor = ""

    order_data_list = []
    while True:
        orders = client.get_order_history(category=category,startTime=start_ts, endTime=end_ts, limit=50, cursor=next_page_cursor)["result"]

        next_page_cursor = orders["nextPageCursor"]
        order_data = orders["list"]
        order_data_list.append(order_data)
        if not order_data:
            break

    orders_by_ticker = {}
    for order_list in order_data_list:
        for order in order_list:
            if order["symbol"] not in orders_by_ticker.keys():
                orders_by_ticker[order["symbol"]] = {"Buy": [], "Sell": []}
                orders_by_ticker[order["symbol"]] = {"Buy": [], "Sell": []}

            orders_by_ticker[order["symbol"]][order["side"]].append(order)

    if orders_by_ticker:
        filled = []
        for key in orders_by_ticker.keys():
            for side in orders_by_ticker[key].keys():
                ticker_orders = orders_by_ticker[key][side]
                price_times_qty = 0
                qty_sum = 0
                if ticker_orders:
                    for order in ticker_orders:

                        usd_val_filled = round(float(order["cumExecValue"]),1)
                        if usd_val_filled > 1:
                            filled_avg_price = float(order["avgPrice"])

                            price_times_qty += usd_val_filled * filled_avg_price
                            qty_sum += usd_val_filled

                    if qty_sum > 0:
                        weighted_avg_price = price_times_qty / qty_sum
                        # print(f"{key} {side} side filled: {qty_sum} USDT at: {weighted_avg_price}")
                        filled.append([key, side, weighted_avg_price, qty_sum])
                else:
                    # print(f"{key} no {side} side orders filled")
                    pass


        filled_df = pd.DataFrame(filled, columns=["ticker", "side", "avg_filled_prc", "filled_usdt_qty"])
        filled_df.set_index("ticker", inplace=True)
        print(f"Filled qty's in last {hours_back} hours || category: {category}")
        print(filled_df.to_markdown())

    else:
        print(f"no executed order for {category} category")


def view_filled_orders(client):
    lookback_window = cli_inputs.select_lookback_window()

    category = input("Select spot/futures [1-spot, 2-futures] >>> ")
    category = int(category)
    if category == 1:
        spot = True
    elif category == 2:
        spot = False

    print("\n")
    get_filled_orders_by_hours(client=client, hours_back=lookback_window, spot=spot)


def view_open_orders(client):
    category = input("Select spot/futures [1-spot, 2-futures] >>> ")
    category = int(category)
    if category == 1:
        spot = True
    elif category == 2:
        spot = False
    print("\n")
    get_open_orders(client, spot=spot)


def orderOverview_bybit_personal():
    api_key, api_secret = get_credentials(account="personal")
    client = auth(api_key, api_secret)

    exit = False
    while not exit:
        print("\n")
        print("What do you want to do:"
              "\n 1 >> display spot positions"
              "\n 2 >> view filled orders"
              "\n 3 >> view open limit orders"
              "\n 0 >> exit ")
        mode = int(input("input number >>> "))
        if mode == 0:
            exit = True
            print(f"Bybit Futures >> IC_personal account - closing")
        elif mode == 1:
            print("\n")
            get_all_spot_positions(client)
        elif mode == 2:
            print("\n")
            view_filled_orders(client)
        elif mode == 3:
            print("\n")
            view_open_orders(client)


def main():
    exit = False
    while not exit:
        print("\n")
        print("Select account:"
              "\n 1 >> Bybit - personal"
              "\n 0 >> exit terminal")

        mode = int(input("input number >>> "))
        if mode == 0:
            exit = True
            print("\n")
            print("Terminal closing")
        elif mode == 1:
            print("\n")
            orderOverview_bybit_personal()

if __name__ == "__main__":
    main()