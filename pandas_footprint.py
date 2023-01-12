# +
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import polars as pl

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


# +
def Convert_tick_data_volume_candles(data: pd.DataFrame, user_defined_volume: str):
	"""
    This function accepts a dataframe of raw tick data for one day,
    return a pd.Dataframe of multiple volume candles.

    Params
    ======
    df_tick_source  : pd.DataFrame
    user_defined_volume        : str, in dd-c format (Eg: 30-m)

    Returns
    =======
    output_df       : pd.DataFrame
    """
	i = 0
	real_start = 0
	real_end = 0
	candle_count = open_row = 0
	price_sell_vol = 0
	price_sell_count = 0
	candle_volume = 0
	price_buy_vol = 0
	price_buy_count = 0
	imb_candles = []
	temp = np.empty((0, 3))
	
	data.sort_values(by=['timestamp'], ignore_index=True, inplace=True)
	data.rename(columns={'size': 'btc_size', 'foreignNotional': 'usd_size'}, inplace=True)
	# data.timestamp = pd.to_datetime(data.timestamp, unit="s")
	# data = data.append(pd.Series(), ignore_index=True)
	data_len = data.shape[0]
	
	timestamp = data.timestamp.values
	side = np.where(data.side == 'Buy', 1, 0)
	price = data.price.values
	usd_size = data.usd_size.values
	btc_size = data.btc_size.values
	
	vol_size = user_defined_volume.split('-')
	if vol_size[1] == 'm':
		vol_size = float(vol_size[0]) * 1000000
	elif vol_size[1] == 'k':
		vol_size = float(vol_size[0]) * 1000
	
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
		
		temp = np.column_stack(
				(side[open_row:i], price[open_row:i],
				 usd_size[open_row:i]))
		original_vol_candle = temp[np.lexsort((temp[:, 2], temp[:, 0], temp[:, 1]))]
		temp = np.empty((0, 3))
		# print(list(original_vol_candle))
		
		uni_prices_in_og_vol_can = np.unique(original_vol_candle[:, 1])
		
		len_uni_prices = uni_prices_in_og_vol_can.size
		
		real_prices = np.arange(uni_prices_in_og_vol_can[0], uni_prices_in_og_vol_can[-1], .5)
		
		len_real_prices = real_prices.size
		
		ss = 0
		for j in range(0, len_uni_prices):
			grouped_by_uni_prices = original_vol_candle[original_vol_candle[:, 1] == uni_prices_in_og_vol_can[j]]
			uni_sides_in_group_p = np.unique(grouped_by_uni_prices[:, 0])
			
			while ss < len_real_prices:
				price_buy_vol = price_buy_count = price_sell_vol = price_sell_count = \
					price_delta = price_delta_percent = price_volume = price_trade_count = 0
				
				if real_prices[ss] == uni_prices_in_og_vol_can[j]:
					if uni_sides_in_group_p[0] == 0:
						price_sell_vol = np.sum(
								grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
						price_sell_count = (
								grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]).shape[0]
						try:
							var = uni_sides_in_group_p[1]
							price_buy_vol = np.sum(grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[1]][:, -1])
							price_buy_count = (grouped_by_uni_prices[
								grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]
							).shape[1]
							candle_buy_vol += price_buy_vol
							candle_buy_count += price_buy_count
						
						except:
							pass
						
						price_delta, price_volume, price_delta_percent, price_trade_count = \
							price_calc(price_buy_vol, price_sell_vol, price_buy_count, price_sell_count)
						
						candle_trade_count += price_trade_count
						candle_sell_vol += price_sell_vol
						candle_sell_count += price_sell_count
					
					
					# if the first side is a buy then do this and the sell vol has to be zero
					else:
						price_buy_vol = np.sum(
								grouped_by_uni_prices[grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]][:, -1])
						price_buy_count = (
								grouped_by_uni_prices[grouped_by_uni_prices[:, 0] ==
								                      uni_sides_in_group_p[0]]).shape[0]
						# price stuff
						price_delta, price_volume, price_delta_percent, price_trade_count = \
							price_calc(price_buy_vol, price_sell_vol, price_buy_count, price_sell_count)
						
						candle_trade_count += price_trade_count
						candle_buy_vol += price_buy_vol
						candle_buy_count += price_buy_count
					
					# append to the candle
					imb_candles.append(
							[
									candle_open_time, candle_close_time, candle_duration, candle_open, candle_high, candle_low, candle_close, candle_volume,
									0, 0, 0, 0, 0, 0, 0,
									# 8 candle_trade_count, 9 candle_delta , 10 candle_delta_percent, 11 candle_buy_vol, 12 candle_buy_count, 13 candle_sell_vol, 14 candle_sell_count
									
									# for loop stuff
									real_prices[ss], price_buy_vol, price_buy_count, price_sell_vol, price_sell_count, price_delta, price_delta_percent, price_volume, price_trade_count])
					ss += 1
					break
				else:
					# append to the candle
					imb_candles.append(
							[
									candle_open_time, candle_close_time, candle_duration, candle_open, candle_high, candle_low, candle_close, candle_volume,
									0, 0, 0, 0, 0, 0, 0,
									# 8 candle_trade_count, 9 candle_delta , 10 candle_delta_percent, 11 candle_buy_vol, 12 candle_buy_count, 13 candle_sell_vol, 14 candle_sell_count
									
									# for loop stuff
									real_prices[ss], price_buy_vol, price_buy_count, price_sell_vol, price_sell_count, price_delta, price_delta_percent, price_volume, price_trade_count])
					ss += 1
		
		candle_delta = candle_buy_vol - candle_sell_vol
		candle_delta_percent = candle_delta / candle_volume
		
		real_end += len_real_prices
		imb_candles = np.array(imb_candles)
		imb_candles[real_start:real_end, 8] = candle_trade_count
		imb_candles[real_start:real_end, 9] = candle_delta
		imb_candles[real_start:real_end, 10] = candle_delta_percent
		imb_candles[real_start:real_end, 11] = candle_buy_vol
		imb_candles[real_start:real_end, 12] = candle_buy_count
		imb_candles[real_start:real_end, 13] = candle_sell_vol
		imb_candles[real_start:real_end, 14] = candle_sell_count
		
		imb_candles = imb_candles.tolist()
		
		candle_volume = 0
		open_row = i
		real_start = real_end
	
	df_test = pd.DataFrame(imb_candles, columns=req_cols)
	return df_test


print(Convert_tick_data_volume_candles(data, '100-m').head(50))