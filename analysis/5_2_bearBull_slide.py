import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import math
eth_price_path = "/data/research/task2302-uni-annual-analysis/return_rate/ethereum/2_price.csv"
eth_volume_path = "/data/research/task2302-uni-annual-analysis/return_rate/ethereum/uni_volume.csv"
poly_ = "/data/research/task2302-uni-annual-analysis/return_rate/polygon/2_price.csv"
poly_swap = "/data/research/task2302-uni-annual-analysis/return_rate/polygon/uni_volume.csv"
# block_timestamp,price,total_liquidity,sqrtPriceX96
# 2023-01-01 00:00:00,1195.3734951754468,1930258157767812104,2291541742459223019076980091157254
# 2023-01-01 00:01:00,1195.3735380161602,1930255195432837114,2291541701396207625051794890229861

# date,liquidity,price,swap_amount
# 2021-12-21,53867413708118,4003.531161,173.054891
# 2021-12-22,25713911126710577,3980.003484,25614.255778

eth_data = pd.read_csv(eth_price_path, dtype={'total_liquidity': float,'price': float})
eth_volume = pd.read_csv(eth_volume_path, dtype={'swap_amount': float,'price': float})

eth_data['block_timestamp'] = pd.to_datetime(eth_data.iloc[:, 0])
eth_volume['date'] = pd.to_datetime(eth_volume.iloc[:, 0])

eth_data = eth_data[(eth_data['block_timestamp'] >= '2023-01-01')]
eth_volume = eth_volume[(eth_volume['date'] >= '2023-01-01')]

eth_data['date'] =eth_data['block_timestamp']
merged_df = pd.merge(eth_data, eth_volume, on='date')
merged_df['volume'] = merged_df['swap_amount']

merged_df['volume_scaled'] = (merged_df['volume'] - merged_df['volume'].min()) / ((merged_df['volume'].max()*2)/3 - merged_df['volume'].min())
merged_df['price_scaled'] = (merged_df['price_x'] - merged_df['price_x'].min()) / (merged_df['price_x'].max() - merged_df['price_x'].min())
merged_df['liquidity_scaled'] = (merged_df['total_liquidity'] - merged_df['total_liquidity'].min()) / (merged_df['total_liquidity'].max() - merged_df['total_liquidity'].min())



# 定义每个指标的权重
volume_weight = 0.3
liquidity_weight = 0.3
price_weight = 0.4

weighted_values = (
    merged_df['volume_scaled'] * volume_weight +
    merged_df['liquidity_scaled'] * liquidity_weight +
    merged_df['price_scaled'] * price_weight
)
plt.figure(figsize=(12, 6))
plt.plot(merged_df['date'], weighted_values)
plt.xlabel('Time')
plt.ylabel('bear bull index')
plt.title('bear bull index - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/bear bull index.png')
plt.clf()

# 判断加权值的分位数来划分市场状态
quantile_25 = weighted_values.quantile(0.25)
quantile_75 = weighted_values.quantile(0.75)
merged_df['bearBullIndex'] = weighted_values
merged_df['Market_Status'] = pd.cut(weighted_values, bins=[weighted_values.min(), quantile_25, quantile_75, weighted_values.max()], labels=['Bear', 'Neutral', 'Bull'])
merged_df['price'] = merged_df['price_x']
merged_df['volume'] = merged_df['swap_amount']

columns_to_drop = ['block_timestamp', 'price_y','sqrtPriceX96','liquidity','price_x','swap_amount']
merged_df = merged_df.drop(columns=columns_to_drop)

merged_df.to_csv('/home/zelos/src/UGPCA/result/csv/Merge_Bear_Bull.csv', index=False)
