# +
import pandas as pd
import numpy as np

np.set_printoptions(suppress=True, formatter={'float_kind': '{:.2f}'.format})
pd.set_option('display.float_format', '{:.2f}'.format)

data = pd.read_csv('btc.csv')
data.sort_values(by=['timestamp'], ignore_index=True, inplace=True)
data.rename(columns={'size': 'btc_size', 'foreignNotional': 'usd_size'}, inplace=True)
data.timestamp = pd.to_datetime(data.timestamp, unit="s")
# data = data.append(pd.Series(), ignore_index=True)
data_len = data.shape[0]
# data

# +
timestamp = data.timestamp.values
side = np.where(data.side == 'Buy', 1, 0)
price = data.price.values
usd_size = data.usd_size.values
btc_size = data.btc_size.values

vol_size = '400-m'
vol_size = vol_size.split('-')
if vol_size[1] == 'm':
	vol_size = float(vol_size[0]) * 1000000
elif vol_size[1] == 'k':
	vol_size = float(vol_size[0]) * 1000

i = 1
x = 0
open_row = 0
sell_vol_total = 0
sell_count = 0
candle_volume = 0
buy_vol_total = 0
buy_count = 0
imb_candles = np.empty((0, 17))
temp = np.empty((0, 3))

# +

while x < data_len:
	print(x)
	while vol_size > candle_volume and  i <= data_len - 1:
		candle_volume += usd_size[x]
		i += 1
	
	candle_open_time = np.min(timestamp[open_row: i])
	candle_close_time = np.max(timestamp[open_row: i])
	candle_open = price[open_row]
	candle_high = np.max(price[open_row: i])
	candle_low = np.min(price[open_row: i])
	candle_close = price[i - 1]
	candle_duration = candle_close_time - candle_open_time
	
	temp = np.column_stack(
			(side[open_row:i], price[open_row:i],
			 usd_size[open_row:i]))
	original_vol_candle = temp[np.lexsort((temp[:, 2], temp[:, 0], temp[:, 1]))]
	temp = np.empty((0, 3))
	list(original_vol_candle)
	
	
	uni_prices_in_og_vol_can = np.sort(np.unique(original_vol_candle[:, 1]))
	
	len_uni_prices = uni_prices_in_og_vol_can.size
	
	real_prices = np.arange(uni_prices_in_og_vol_can[0], uni_prices_in_og_vol_can[-1] + .5, .5)
	
	len_real_prices = real_prices.size
	
	# ss = 0
	
	for j in range(0, len_uni_prices):
		grouped_by_uni_prices = original_vol_candle[original_vol_candle[:, 1] ==
		                                            uni_prices_in_og_vol_can[j]]
		uni_sides_in_group_p = np.sort(np.unique(grouped_by_uni_prices[:, 0]))
		# for s in range(0 + ss, len_real_prices):
		ss = 0
		while ss <= len_real_prices:
			if real_prices[ss] == uni_prices_in_og_vol_can[j]:
				if uni_sides_in_group_p[0] == 0:
					sell_vol_total = np.sum(
							grouped_by_uni_prices[
								grouped_by_uni_prices[:,
								0] == uni_sides_in_group_p[0]][:,
							-1])
					sell_count = (
							grouped_by_uni_prices[grouped_by_uni_prices[:, 0] ==
							                      uni_sides_in_group_p[0]]).shape[0]
					
					try:
						var = uni_sides_in_group_p[1]
						buy_vol_total = np.sum(
								grouped_by_uni_prices[grouped_by_uni_prices[:, 0] ==
								                      uni_sides_in_group_p[1]][:, -1])
						buy_count = (grouped_by_uni_prices[
							grouped_by_uni_prices[:, 0] == uni_sides_in_group_p[0]]
						).shape[1]
						price_delta = buy_vol_total - sell_vol_total
						price_volume = buy_vol_total + sell_vol_total
						price_delta_percent = price_delta / price_volume
						price_trade_count = buy_count + sell_count
					
					except:
						buy_vol_total = 0
						buy_count = 0
						price_delta = buy_vol_total - sell_vol_total
						price_volume = buy_vol_total + sell_vol_total
						price_delta_percent = price_delta / price_volume
						price_trade_count = buy_count + sell_count
				
				# if the first side is a buy then do this and the sell vol has to be zero
				else:
					buy_vol_total = np.sum(
							grouped_by_uni_prices[
								grouped_by_uni_prices[:,
								0] == uni_sides_in_group_p[0]][:,
							-1])
					buy_count = (
							grouped_by_uni_prices[grouped_by_uni_prices[:, 0] ==
							                      uni_sides_in_group_p[0]]).shape[0]
					
					# sell vol has to be zero
					sell_vol_total = 0
					sell_count = 0
					
					# price stuff
					price_delta = buy_vol_total - sell_vol_total
					price_volume = buy_vol_total + sell_vol_total
					price_delta_percent = price_delta / price_volume
					price_trade_count = buy_count + sell_count
				
				# append to the candle
				imb_candles = np.append(
						imb_candles,
						[[candle_open_time, candle_close_time, candle_duration, candle_open, candle_high, candle_low, candle_close, candle_volume,
						  # for loop stuff
						  real_prices[ss], buy_vol_total, buy_count, sell_vol_total, sell_count, price_delta, price_delta_percent, price_volume, price_trade_count,
						  ]],
						axis=0)
				ss += 1
				break
			else:
				buy_vol_total = buy_count = sell_vol_total = sell_count = price_delta = price_delta_percent = price_volume = price_trade_count = 0
				
				# append to the candle
				imb_candles = np.append(
						imb_candles,
						[[candle_open_time, candle_close_time, candle_duration, candle_open, candle_high, candle_low, candle_close, candle_volume,
						  # for loop stuff
						  real_prices[ss], buy_vol_total, buy_count, sell_vol_total, sell_count, price_delta, price_delta_percent, price_volume, price_trade_count,
						  ]],
						axis=0)
				ss += 1
	candle_volume = 0
	open_row = i
	x = i - 1
	print(i)

df_test = pd.DataFrame(
		imb_candles,
		columns=[
				'open time',
				'close time',
				'duration',
				'open',
				'high',
				'low',
				'close',
				'volume',
				'price',
				'buy vol',
				'buy count',
				'sell vol',
				'sell count',
				'price delta',
				'price delta %',
				'price vol',
				'price trade count'
				]).head(50)

df_test
