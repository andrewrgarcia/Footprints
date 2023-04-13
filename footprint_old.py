import pandas as pd
import numpy as np
# import plotly.graph_objects as go
# import polars as pl
from numba import njit


np.set_printoptions(suppress=True, formatter={'float_kind': '{:.2f}'.format})
pd.set_option('display.float_format', '{:.2f}'.format)
# -
data = pd.read_csv('btc.csv')


req_cols = ['open time', 'close time', 'duration', 'open', 'high', 'low', 'close',
            'volume', 'trade count', 'delta', 'delta %', 'candle buy volume', 'candle buy count', 'candle sell volume',
            'candle sell count', 'price', 'buy vol', 'buy count', 'sell vol', 'sell count', 'price delta',
            'price delta %', 'price vol', 'price trade count']


def price_calc(price_buy_vol, price_sell_vol, price_buy_count, price_sell_count):
	price_delta = price_buy_vol - price_sell_vol
	price_volume = price_buy_vol + price_sell_vol
	price_delta_percent = price_delta / price_volume
	price_trade_count = price_buy_count + price_sell_count
	return price_delta, price_volume, price_delta_percent, price_trade_count


data.sort_values(by=['timestamp'], ignore_index=True, inplace=True)
data.rename(columns={'size': 'btc_size', 'foreignNotional': 'usd_size'}, inplace=True)
# data.timestamp = pd.to_datetime(data.timestamp, unit="s")
# data = data.append(pd.Series(), ignore_index=True)
data_len = data.shape[0]


foot_dtype = np.dtype([
        ('open time', np.float_),
        ('close time', np.float_),
        ('duration', np.float_),
        ('open', np.float_),
        ('high', np.float_),
        ('low', np.float_),
        ('close', np.float_),
        ('volume', np.float_),
        ('trade count', np.float_),
        ('delta', np.float_),
        ('delta %', np.float_),
        ('candle buy volume', np.float_),
        ('candle buy count', np.float_),
        ('candle sell volume', np.float_),
        ('candle sell count', np.float_),
        ('price', np.float_),
        ('buy vol', np.float_),
        ('buy count', np.float_),
        ('sell vol', np.float_),
        ('sell count', np.float_),
        ('price delta', np.float_),
        ('price delta %', np.float_),
        ('price vol', np.float_),
        ('price trade count', np.float_),
    ], align=True)
timestamp = data.timestamp.values
side = np.where(data.side == 'Buy', 1, 0)
price = data.price.values
usd_size = data.usd_size.values
btc_size = data.btc_size.values


def user_vol_size(split_me):
    vol_size = split_me.split('-')
    if vol_size[1] == 'm':
        return float(vol_size[0]) * 1000000
    elif vol_size[1] == 'k':
        return float(vol_size[0]) * 1000
    


@njit
def Convert_tick_data_volume_candles(vol_size):
    global timestamp, side, price, usd_size
    i = 0
    num_row = 0
    real_start = 0
    real_end = 0
    candle_count = open_row = 0
    price_sell_vol = 0
    price_sell_count = 0
    candle_volume = 0
    price_buy_vol = 0
    price_buy_count = 0
    temp = np.empty((0, 3))
    imb_candles = np.full((1000000, 24), np.nan)

    while i < data_len:
        while vol_size > candle_volume and i <= data_len - 1:
            candle_volume += usd_size[i]
            i += 1

        candle_open_time = np.min(timestamp[open_row: i])
        candle_close_time = np.max(timestamp[open_row: i])
        candle_open = price[open_row]
        candle_high = np.max(price[open_row: i])
        candle_low = np.min(price[open_row: i])
        candle_close = price[i - 1]
        candle_duration = candle_close_time - candle_open_time
        candle_buy_vol = 0
        candle_buy_count = 0
        candle_sell_vol = 0
        candle_sell_count = 0
        candle_trade_count = 0

        original_vol_candle = np.column_stack(
            (side[open_row:i], price[open_row:i],
             usd_size[open_row:i]))

        uni_prices_in_og_vol_can = np.sort(
            np.unique(original_vol_candle[:, 1]))

        len_uni_prices = uni_prices_in_og_vol_can.size

        real_prices = np.arange(
            uni_prices_in_og_vol_can[0], uni_prices_in_og_vol_can[-1] + .5, .5)

        len_real_prices = real_prices.size

        ss = 0

        for j in range(0, len_uni_prices):
            grouped_by_uni_prices = original_vol_candle[original_vol_candle[:, 1]
                                                        == uni_prices_in_og_vol_can[j]]
            uni_sides_in_group_p = np.sort(
                np.unique(grouped_by_uni_prices[:, 0]))

            while ss < len_real_prices:

                # checks to see if the price is a slippage price or a price in the candle
                if real_prices[ss] == uni_prices_in_og_vol_can[j]:

                    # checks if side is a sell
                    if uni_sides_in_group_p[0] == 0:
                        # sum up the amoung of sells and sell volume
                        price_sell_vol = np.sum(
                            grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
                        price_sell_count = (
                            grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[0]
                        try:
                            # checks if there are any buys ... if so sum them ... if set buy info to 0
                            var = uni_sides_in_group_p[1]
                            price_buy_vol = np.sum(
                                grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[1]][:, -1])
                            price_buy_count = (grouped_by_uni_prices[
                                grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]
                            ).shape[1]
                        except:
                            price_buy_vol = 0
                            price_buy_count = 0

                        price_delta = price_buy_vol - price_sell_vol
                        price_volume = price_buy_vol + price_sell_vol
                        price_trade_count = price_buy_count + price_sell_count
                        price_delta_percent = price_delta / price_volume
                        # price stuff
                        candle_trade_count += price_trade_count
                        candle_sell_vol += price_sell_vol
                        candle_sell_count += price_sell_count
                        candle_buy_vol += price_buy_vol
                        candle_buy_count += price_buy_count

                        # append to the candle
                        imb_candles[num_row, 0] = candle_open_time
                        imb_candles[num_row, 1] = candle_close_time
                        imb_candles[num_row, 2] = candle_duration
                        imb_candles[num_row, 3] = candle_open
                        imb_candles[num_row, 4] = candle_high
                        imb_candles[num_row, 5] = candle_low
                        imb_candles[num_row, 6] = candle_close
                        imb_candles[num_row, 7] = candle_volume
                        imb_candles[num_row, 15] = real_prices[ss]
                        imb_candles[num_row, 16] = price_buy_vol
                        imb_candles[num_row, 17] = price_buy_count
                        imb_candles[num_row, 18] = price_sell_vol
                        imb_candles[num_row, 19] = price_sell_count
                        imb_candles[num_row, 20] = price_delta
                        imb_candles[num_row, 21] = price_delta_percent
                        imb_candles[num_row, 22] = price_volume
                        imb_candles[num_row, 23] = price_trade_count
                        # print(f'hello {num_row}' )
                        # print(imb_candles[num_row,:])
                        ss += 1
                        num_row += 1
                        break

                    # First element is a buy
                    else:
                        # sum up all the buy volume and count
                        price_buy_vol = np.sum(
                            grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
                        price_buy_count = (
                            grouped_by_uni_prices[grouped_by_uni_prices[:, 0] ==
                                                  uni_sides_in_group_p[0]]).shape[0]
                        try:
                            # checks 2nd element for any sells ... if so sum them ... if not pass
                            var = uni_sides_in_group_p[1]
                            price_sell_vol = np.sum(
                                grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[1]][:, -1])
                            price_sell_count = (grouped_by_uni_prices[
                                grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]
                            ).shape[1]
                        except:
                            price_sell_count = 0
                            price_sell_vol = 0

                        # price info
                        price_delta = price_buy_vol - price_sell_vol
                        price_volume = price_buy_vol + price_sell_vol
                        price_trade_count = price_buy_count + price_sell_count
                        price_delta_percent = price_delta / price_volume

                        # adding to candle info
                        candle_trade_count += price_trade_count
                        candle_sell_vol += price_sell_vol
                        candle_sell_count += price_sell_count
                        candle_buy_vol += price_buy_vol
                        candle_buy_count += price_buy_count

                        # append to the candle
                        imb_candles[num_row, 0] = candle_open_time
                        imb_candles[num_row, 1] = candle_close_time
                        imb_candles[num_row, 2] = candle_duration
                        imb_candles[num_row, 3] = candle_open
                        imb_candles[num_row, 4] = candle_high
                        imb_candles[num_row, 5] = candle_low
                        imb_candles[num_row, 6] = candle_close
                        imb_candles[num_row, 7] = candle_volume
                        imb_candles[num_row, 15] = real_prices[ss]
                        imb_candles[num_row, 16] = price_buy_vol
                        imb_candles[num_row, 17] = price_buy_count
                        imb_candles[num_row, 18] = price_sell_vol
                        imb_candles[num_row, 19] = price_sell_count
                        imb_candles[num_row, 20] = price_delta
                        imb_candles[num_row, 21] = price_delta_percent
                        imb_candles[num_row, 22] = price_volume
                        imb_candles[num_row, 23] = price_trade_count
                        # print(f'hello {num_row}' )
                        # print(imb_candles[num_row,:])
                        ss += 1
                        num_row += 1
                        break

                else:
                    # append to the candle
                    imb_candles[num_row, 0] = candle_open_time
                    imb_candles[num_row, 1] = candle_close_time
                    imb_candles[num_row, 2] = candle_duration
                    imb_candles[num_row, 3] = candle_open
                    imb_candles[num_row, 4] = candle_high
                    imb_candles[num_row, 5] = candle_low
                    imb_candles[num_row, 6] = candle_close
                    imb_candles[num_row, 7] = candle_volume
                    imb_candles[num_row, 15] = real_prices[ss]
                    imb_candles[num_row, 16] = 0
                    imb_candles[num_row, 17] = 0
                    imb_candles[num_row, 18] = 0
                    imb_candles[num_row, 19] = 0
                    imb_candles[num_row, 20] = 0
                    imb_candles[num_row, 21] = 0
                    imb_candles[num_row, 22] = 0
                    imb_candles[num_row, 23] = 0
                    # print(f'hello {num_row}' )
                    # print(imb_candles[num_row,:])
                    ss += 1
                    num_row += 1

        candle_delta = candle_buy_vol - candle_sell_vol
        candle_delta_percent = candle_delta / candle_volume

        real_end += len_real_prices
        imb_candles[real_start:real_end, 8] = candle_trade_count
        imb_candles[real_start:real_end, 9] = candle_delta
        imb_candles[real_start:real_end, 10] = candle_delta_percent
        imb_candles[real_start:real_end, 11] = candle_buy_vol
        imb_candles[real_start:real_end, 12] = candle_buy_count
        imb_candles[real_start:real_end, 13] = candle_sell_vol
        imb_candles[real_start:real_end, 14] = candle_sell_count

        candle_volume = 0
        open_row = i
        real_start = real_end

    return imb_candles

from timethis import timethis

with timethis("with @njit"):
    pd.DataFrame(Convert_tick_data_volume_candles(user_vol_size('10-m')), columns=req_cols).dropna().to_csv('footprint_test_numba.csv')
# Convert_tick_data_volume_candles(user_vol_size('10-m'))