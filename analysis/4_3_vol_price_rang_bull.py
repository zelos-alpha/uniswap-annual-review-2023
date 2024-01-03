from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt, ticker
import math
from tqdm import tqdm
import os

# 读取ETH价格日数据
df_eth = pd.read_csv('/home/zelos/src/UGPCA/result/csv/eth_price_hourly.csv', sep=',', index_col=[], dtype=object, header=0)
df_eth_vol = pd.read_csv('/home/zelos/src/UGPCA/result/csv/volatility_hour.csv')
# block_timestamp,open,high,low,close
# 2021-12-21,3998.9602573708376,4006.4445151230057,3986.539004827416,4003.531161230306
# 2021-12-22,4003.531161230306,4050.2275010884905,3958.1008303274352,3980.0034836239465

# date,volatility
# 2023-01-09,0.01421078921127109
# 2023-01-10,0.015220145020647972
# 2023-01-11,0.0143665776619194

df_eth['price'] = df_eth['close'].astype(float)
df_eth['date'] = df_eth['block_timestamp']
merged_df = df_eth_vol.merge(df_eth, on='date', how='inner')


def is_bear_market(date):
    market_dict = {
    ("2023-01-01 00:00:00", "2023-03-11 00:00:00"): "bear",
    ("2023-03-11 00:00:00", "2023-08-17 00:00:00"): "bull",
    ("2023-08-17 00:00:00", "2023-10-20 00:00:00"): "bear",
    ("2023-10-24 00:00:00", "2023-12-17 00:00:00"): "bull"
    }
    try:
        date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        date = datetime.strptime(date, "%Y-%m-%d").date()
    for time_range, market_status in market_dict.items():
        start_date = datetime.strptime(time_range[0], "%Y-%m-%d %H:%M:%S").date()
        end_date = datetime.strptime(time_range[1], "%Y-%m-%d %H:%M:%S").date()

        if start_date <= date <= end_date:
            if market_status == "bear":
                return True
            elif market_status == "bull":
                return False
    return False

filtered_df = merged_df[merged_df['date'].apply(is_bear_market)]
merged_df=filtered_df

df_eth_sorted = merged_df.sort_values('volatility')
df_eth_sorted = df_eth_sorted.dropna(subset=['volatility'])
df_eth_sorted['date'] = pd.to_datetime(df_eth_sorted['date'])
df_eth_sorted = df_eth_sorted[(df_eth_sorted['date'] >= '2023-01-01')]
df_eth_sorted.to_csv('/home/zelos/src/UGPCA/result/csv/sample.csv', index=False)
# 分箱
n_bins = 100 // 5 
bins = pd.cut(np.arange(len(df_eth_sorted)), n_bins, labels=False)

df_eth_sorted['bin'] = bins
grouped = df_eth_sorted.groupby('bin')['date'].apply(list)
volatility_ranges = pd.cut(df_eth_sorted['volatility'], n_bins).unique()
dfs = pd.read_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_ETH_BearBull_bull.csv')
up_dfs = dfs[(dfs['up_persentage'] <= 100) & (dfs['up_persentage'] >= -25)]
up_dfs = up_dfs[(up_dfs['low_persentage'] <= 100) & (up_dfs['low_persentage'] >= -25)]
up_dfs.loc[:, 'start_time'] = pd.to_datetime(up_dfs['start_time']).dt.floor('H')
result_dict = {}
n=0

for i, bin_values in grouped.items():
    try:
        range_tuple = (volatility_ranges[i].left, volatility_ranges[i].right)
    except:
        print('skip')

    up_percentages = []
    low_percentages = []
    liquidity_values = []

    for hour_time in tqdm(bin_values, desc='Processing Bin'):
        for pos_time in up_dfs['start_time']:
            if pos_time == hour_time:
                up_percentage = up_dfs.loc[up_dfs['start_time'] == pos_time, 'up_persentage'].values[0]
                low_percentage = up_dfs.loc[up_dfs['start_time'] == pos_time, 'low_persentage'].values[0]
                liquidity_value = up_dfs.loc[up_dfs['start_time'] == pos_time, 'liquidity'].values[0]
                # 更新最大值和最小值
                up_percentages.append(up_percentage)
                low_percentages.append(low_percentage)
                liquidity_values.append(liquidity_value)

    result_dict[range_tuple] = {
        'up_percentage': up_percentages,
        'low_percentage': low_percentages,
        'liquidity': liquidity_values
    }

print(result_dict)

data_list = []
for range_tuple, data in result_dict.items():
    up_percentages = data['up_percentage']
    low_percentages = data['low_percentage']
    liquidity = data['liquidity']
    for i in range(len(up_percentages)):
        data_list.append([range_tuple, up_percentages[i], low_percentages[i],liquidity[i]])

df = pd.DataFrame(data_list, columns=['volatility_ranges', 'up_percentage', 'low_percentage','liquidity'])

df.to_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX_bull_bull.csv', index=False)





