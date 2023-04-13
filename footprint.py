import pandas as pd
import numpy as np
from timethis import timethis

# import plotly.graph_objects as go
# import polars as pl
# from numba import njit


def user_vol_size(split_me):
    vol_size = split_me.split('-')
    if vol_size[1] == 'm':
        return float(vol_size[0]) * 1000000
    elif vol_size[1] == 'k':
        return float(vol_size[0]) * 1000

class Printer:
    def __init__(self):
        '''
        A class that defines methods for updating and aggregating data for footprint candlestick chart generation.

        Attributes
        -----------
        ss : int
            The row index for a particular candlestick chart.
        num_row : int
            The total number of rows in a candlestick chart.
        candle_open_time : int
            The opening time for a particular candlestick.
        candle_close_time : int
            The closing time for a particular candlestick.
        candle_duration : int
            The duration of a particular candlestick.
        candle_open : float
            The opening price for a particular candlestick.
        candle_high : float
            The highest price for a particular candlestick.
        candle_low : float
            The lowest price for a particular candlestick.
        candle_close : float
            The closing price for a particular candlestick.
        candle_volume : float
            The total volume for a particular candlestick.
        real_prices : float
            The prices for a particular candlestick chart.
        price_buy_vol : float
            The total buy volume for a particular candlestick.
        price_buy_count : int
            The total buy count for a particular candlestick.
        price_sell_vol : float
            The total sell volume for a particular candlestick.
        price_sell_count : int
            The total sell count for a particular candlestick.
        price_delta : float
            The difference between buy and sell volume for a particular candlestick.
        price_delta_percent : float
            The percentage difference between buy and sell volume for a particular candlestick.
        price_volume : float
            The total volume for a particular candlestick.
        price_trade_count : int
            The total trade count for a particular candlestick.
        candle_trade_count : int
            The total trade count for a particular candlestick chart.
        candle_sell_vol : float
            The total sell volume for a particular candlestick chart.
        candle_sell_count : int
            The total sell count for a particular candlestick chart.
        candle_buy_vol : float
            The total buy volume for a particular candlestick chart.
        candle_buy_count : int
            The total buy count for a particular candlestick chart.

        '''
        self.ss = 0
        self.num_row = 0
        
        self.candle_open_time = 0
        self.candle_close_time = 0
        self.candle_duration = 0
        self.candle_open = 0
        self.candle_high = 0
        self.candle_low = 0
        self.candle_close = 0
        self.candle_volume = 0
        self.real_prices = 0
        self.price_buy_vol = 0
        self.price_buy_count = 0
        self.price_sell_vol = 0
        self.price_sell_count = 0
        self.price_delta = 0
        self.price_delta_percent = 0
        self.price_volume = 0
        self.price_trade_count = 0

        self.candle_trade_count = 0
        self.candle_sell_vol = 0
        self.candle_sell_count = 0
        self.candle_buy_vol = 0
        self.candle_buy_count = 0

    def update_price_data(self):
        '''A method that updates price data (Formerly known as `price_calc`)'''
        
        self.price_delta = self.price_buy_vol - self.price_sell_vol
        self.price_volume = self.price_buy_vol + self.price_sell_vol
        self.price_delta_percent = self.price_delta / self.price_volume
        self.price_trade_count = self.price_buy_count + self.price_sell_count

    def append_to_candle(self,candles):
        '''
        A function that appends the features to a given candle and updates the value for key 15 based on the updated_ss value.

        Parameters
        ------------------
        candles: ndarray
            A numpy array representing the candles
        '''        
        # Define a dictionary of features
        features={
            '0': self.candle_open_time,
            '1': self.candle_close_time,
            '2': self.candle_duration,
            '3': self.candle_open,
            '4': self.candle_high,
            '5': self.candle_low,
            '6': self.candle_close,
            '7': self.candle_volume,
            '15': self.real_prices[self.ss],
            '16': self.price_buy_vol,
            '17': self.price_buy_count,
            '18': self.price_sell_vol,
            '19': self.price_sell_count,
            '20': self.price_delta,
            '21': self.price_delta_percent,
            '22': self.price_volume,
            '23': self.price_trade_count
        }

        for i in features.keys():
            candles[self.num_row, int(i)] = features[i]

        return candles

    def aggregate_volumes(self, candles, grouped_by_uni_prices, uni_sides_in_group_p, side=0):
        """
        This function aggregates volumes from the buy and sell sides of an order book, depending on the direction of the side.
        It is used to generate footprint candle charts.

        Parameters
        -------------------
        candles: ndarray
            A numpy array representing the candles
        grouped_by_uni_prices : ndarray
            Contains grouped prices.
        uni_sides_in_group_p : list  
            A list of unique sides in the grouped prices array.
        side : int
            The side direction. 0 represents 'bid' and 1 represents 'ask'. Defaults to 0.
        """          

        if side == 0:
            # Sum up the amount of sells and sell volume
            self.price_sell_vol = np.sum(grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
            self.price_sell_count = (grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[0]
            
            try:
                # If there are any buys , sum them, otherwise set buy info to 0
                self.price_buy_vol = np.sum(grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[1]][:, -1])
                self.price_buy_count = (grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[1]
            except:
                self.price_buy_vol = 0
                self.price_buy_count = 0
        else: 
            # Sum up the amount of buys and buy volume
            self.price_buy_vol = np.sum(grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
            self.price_buy_count = (grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[0]

            try:
                # If there are any sells , sum them, otherwise set sell info to 0
                self.price_sell_vol = np.sum(grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[1]][:, -1])
                self.price_sell_count = (grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[1]
            except:
                self.price_sell_count = 0
                self.price_sell_vol = 0

        # Update price data
        self.update_price_data()

        # Aggregate volumes and counts
        self.candle_trade_count += self.price_trade_count
        self.candle_sell_vol += self.price_sell_vol
        self.candle_sell_count += self.price_sell_count
        self.candle_buy_vol += self.price_buy_vol
        self.candle_buy_count += self.price_buy_count

        # Append to candlestick data
        candles = self.append_to_candle(candles)

        # Increment counters
        self.ss += 1
        self.num_row += 1

        return candles


    def Convert_tick_data_volume_candles(self, data, vol_size):

        data.sort_values(by=['timestamp'], ignore_index=True,  inplace=True)
        data.rename(columns={'size': 'btc_size', 'foreignNotional': 'usd_size'}, inplace=True)
        # data.timestamp = pd.to_datetime(data.timestamp, unit="s")
        # data = data.append(pd.Series(), ignore_index=True)
        data_len = data.shape[0]

        # global timestamp, side, price, usd_size
        timestamp = data.timestamp.values
        side = np.where(data.side == 'Buy', 1, 0)
        price = data.price.values
        usd_size = data.usd_size.values
        btc_size = data.btc_size.values

        # initialize variables
        i = 0
        real_start = 0
        real_end = 0
        candle_count = open_row = 0
        temp = np.empty((0, 3))
        imb_candles = np.full((1000000, 24), np.nan)

        # iterate through tick data
        while i < data_len:
            # accumulate tick data until it reaches the given volume size
            while vol_size > self.candle_volume and i <= data_len - 1:
                self.candle_volume += usd_size[i]
                i += 1

            # calculate candle features based on accumulated tick data
            self.candle_open_time = np.min(timestamp[open_row: i])
            self.candle_close_time = np.max(timestamp[open_row: i])
            self.candle_open = price[open_row]
            self.candle_high = np.max(price[open_row: i])
            self.candle_low = np.min(price[open_row: i])
            self.candle_close = price[i - 1]
            self.candle_duration = self.candle_close_time - self.candle_open_time
            self.candle_buy_vol = 0
            self.candle_buy_count = 0
            self.candle_sell_vol = 0
            self.candle_sell_count = 0
            self.candle_trade_count = 0

            # Combine the side, price, and USD size columns into a single array
            original_vol_candle = np.column_stack(
                (side[open_row:i], price[open_row:i],
                usd_size[open_row:i]))

            # Find the unique prices in the original volume candle and sort them in ascending order
            uni_prices_in_og_vol_can = np.sort(
                np.unique(original_vol_candle[:, 1]))

            # Count the number of unique prices
            len_uni_prices = uni_prices_in_og_vol_can.size

            # Create an array of real prices, starting from the smallest unique price in the original volume candle and ending at the largest unique price plus 0.5
            self.real_prices = np.arange(
                uni_prices_in_og_vol_can[0], uni_prices_in_og_vol_can[-1] + .5, .5)

            len_real_prices = self.real_prices.size

            self.ss = 0
            
            for j in range(0, len_uni_prices):
                grouped_by_uni_prices = original_vol_candle[original_vol_candle[:, 1]
                                                            == uni_prices_in_og_vol_can[j]]
                uni_sides_in_group_p = np.sort(
                    np.unique(grouped_by_uni_prices[:, 0]))

                while self.ss < len_real_prices:

                    # checks to see if the price is a slippage price or a price in the candle
                    if self.real_prices[self.ss] == uni_prices_in_og_vol_can[j]:

                        # Check if side is a sell
                        if uni_sides_in_group_p[0] == 0:
                            imb_candles = self.aggregate_volumes(imb_candles, grouped_by_uni_prices, uni_sides_in_group_p, side=0)
                            break

                        else:
                            # First element is a buy
                            imb_candles = self.aggregate_volumes(imb_candles, grouped_by_uni_prices, uni_sides_in_group_p, side=1)
                            break

                    else:
                        imb_candles = self.append_to_candle(imb_candles)
                        # print(f'hello {self.num_row}' )
                        # print(imb_candles[self.num_row,:])
                        self.ss += 1
                        self.num_row += 1

            candle_delta = self.candle_buy_vol - self.candle_sell_vol
            candle_delta_percent = candle_delta / self.candle_volume

            real_end += len_real_prices
            imb_candles[real_start:real_end, 8] = self.candle_trade_count
            imb_candles[real_start:real_end, 9] = candle_delta
            imb_candles[real_start:real_end, 10] = candle_delta_percent
            imb_candles[real_start:real_end, 11] = self.candle_buy_vol
            imb_candles[real_start:real_end, 12] = self.candle_buy_count
            imb_candles[real_start:real_end, 13] = self.candle_sell_vol
            imb_candles[real_start:real_end, 14] = self.candle_sell_count

            self.candle_volume = 0
            open_row = i
            real_start = real_end

        return imb_candles



np.set_printoptions(suppress=True, formatter={'float_kind': '{:.2f}'.format})
pd.set_option('display.float_format', '{:.2f}'.format)
# -
data = pd.read_csv('btc.csv')


req_cols = ['open time', 'close time', 'duration', 'open', 'high', 'low', 'close',
            'volume', 'trade count', 'delta', 'delta %', 'candle buy volume', 'candle buy count', 'candle sell volume',
            'candle sell count', 'price', 'buy vol', 'buy count', 'sell vol', 'sell count', 'price delta',
            'price delta %', 'price vol', 'price trade count']



with timethis("without @njit"):

    handler = Printer()
    pd.DataFrame(handler.Convert_tick_data_volume_candles(data, user_vol_size('10-m')), columns=req_cols).dropna().to_csv('footprint_test_numpy.csv')
