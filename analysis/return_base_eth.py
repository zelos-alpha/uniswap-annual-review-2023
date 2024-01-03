import os
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib
# 读取ETH价格日数据
df_eth = pd.read_csv("/data/research/uni_return_rate_service/4_price.csv", sep=',', index_col=[], dtype=object, header=0)
# block_timestamp,price,total_liquidity,sqrtPriceX96
# 2021-12-21 19:10:00,3998.9602573708376,53867413708118,1252870085262684304653774744217702
df_eth[['price']] = df_eth[['price']].astype(float)
df_eth['returns'] = df_eth['price'].pct_change()
df_eth["hourly_return"] = df_eth["returns"].apply(lambda x: (1 + x) ** 60 - 1)
df_eth["daily_return"] = df_eth["hourly_return"].apply(lambda x: (1 + x) ** 24 - 1)

df_eth['block_timestamp'] = pd.to_datetime(df_eth['block_timestamp'])
df_eth = df_eth[(df_eth['block_timestamp'] >= '2023-01-01') ]

df_eth['date_day'] = df_eth['block_timestamp'].dt.date
df_daily = df_eth.groupby('date_day')['daily_return'].apply(lambda x: (1 + x).prod() ** (1 / len(x)) - 1).reset_index(name='geo_annualized_return')
plt.figure(figsize=(10, 6))
plt.plot(df_daily['date_day'], df_daily['geo_annualized_return'])
plt.title('ETH Dailyized Return')

plt.xlabel('Time')
plt.ylabel('Market Return')


plt.savefig('/home/zelos/src/UGPCA/result/plot/market_return_day_day_eth.png') 