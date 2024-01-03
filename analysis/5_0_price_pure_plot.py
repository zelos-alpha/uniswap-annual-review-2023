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
poly_volume = pd.read_csv(poly_swap, dtype={'swap_amount': float,'price': float})
poly_real_data =pd.read_csv(poly_,dtype={'total_liquidity': float})

eth_data['block_timestamp'] = pd.to_datetime(eth_data.iloc[:, 0])
eth_volume['date'] = pd.to_datetime(eth_volume.iloc[:, 0])
poly_volume['date'] = pd.to_datetime(poly_volume.iloc[:, 0])
poly_real_data['block_timestamp'] = pd.to_datetime(poly_real_data.iloc[:, 0])

eth_data = eth_data[(eth_data['block_timestamp'] >= '2023-01-01')]
eth_volume = eth_volume[(eth_volume['date'] >= '2023-01-01')]
poly_volume = poly_volume[(poly_volume['date'] >= '2023-01-01')]
poly_real_data =poly_real_data[(poly_real_data['block_timestamp'] >= '2023-01-01')]

plt.figure(figsize=(10, 6))
eth_data.set_index('block_timestamp', inplace=True)
eth_hourly_data_volume = eth_data['total_liquidity'].resample('1D').mean()
eth_hourly_data_price = eth_data['price'].resample('1D').mean()

poly_real_data.set_index('block_timestamp', inplace=True)
poly_real_hour_data_volume = poly_real_data['total_liquidity'].resample('1D').mean()
# eth_hourly_data_price = eth_data['price'].resample('1D').mean()




plt.plot(eth_hourly_data_volume.index, eth_hourly_data_volume)
plt.xlabel('Time')
plt.ylabel('Liquidity')
plt.title('Liquidity - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/ETH&MATIC liq.png')
plt.clf()



plt.plot(poly_real_hour_data_volume.index, poly_real_hour_data_volume)
plt.xlabel('Time')
plt.ylabel('Liquidity')
plt.title('Liquidity - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/Polygon liq.png')
plt.clf()



plt.plot(eth_hourly_data_price.index, eth_hourly_data_price)
plt.xlabel('Time')
plt.ylabel('Price')
plt.title('Price - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/ETH&MATIC price.png')
plt.clf()



plt.plot(eth_volume['date'], eth_volume['swap_amount'])
plt.xlabel('Time')
plt.ylabel('Volume')
plt.title('Volume - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/ETH volume.png')
plt.clf()


plt.plot(poly_volume['date'], poly_volume['swap_amount'])
plt.xlabel('Time')
plt.ylabel('Volume')
plt.title('Volume - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/Poly volume.png')
plt.clf()



plt.plot(eth_hourly_data_price.index, eth_hourly_data_price,label = "Price of ETH") 
plt.plot(eth_volume['date'], eth_volume['swap_amount']*0.0000006+1000, label='Volume on Ethereum',alpha = 0.5)
plt.plot(poly_volume['date'], poly_volume['swap_amount']*0.000005+1000,label = "Volume on Polygon",alpha = 0.5)
plt.plot(eth_hourly_data_volume.index, math.prod([eth_hourly_data_volume, 6, 10**(-18)])+1500,label = "Liquidity of usdc-eth-005 on Ethereum",alpha = 0.5, linestyle='dashed')
plt.plot(poly_real_hour_data_volume.index, math.prod([poly_real_hour_data_volume, 6, 10**(-17)])+1500,label = "Liquidity of usdc-weth-005 on Polygon",alpha = 0.5, linestyle='dashed')
plt.xlabel('Time')
plt.ylabel('Price/Volume/Liquidity')
plt.title('Price/Volume/Liquidity Trend')
plt.legend()
plt.savefig('/home/zelos/src/UGPCA/result/plot/ETH&Polygon Trend.png')
plt.clf()

fig, ax1 = plt.subplots(figsize=(12,6))
ax1.plot(eth_hourly_data_price.index,  eth_hourly_data_price , label = "Price of ETH",color = '#1f77b4')
ax1.set_ylabel('Price')

ax2 = ax1.twinx()
bar1 = ax2.bar(eth_volume['date'],  eth_volume['swap_amount'], label='Volume on Ethereum',color = 'aquamarine')
bar2 = ax2.bar(poly_volume['date'],  poly_volume['swap_amount'], label='Volume on Polygon',color = 'dodgerblue')
ax2.set_ylabel('Volume')

ax3 = ax1.twinx()
ax3.spines['right'].set_position(('outward', 60))  # 将第三个Y轴绘制到右边
ax3.plot(eth_hourly_data_volume.index, eth_hourly_data_volume, label = "Liquidity of usdc-eth-005 on Ethereum",color = 'hotpink', linestyle='dashed')
ax3.plot(poly_real_hour_data_volume.index, poly_real_hour_data_volume, label = "Liquidity of usdc-weth-005 on Polygon",color = 'sandybrown', linestyle='dashed')
ax3.set_ylabel('Liquidity')

ax3.yaxis.set_label_coords(1.17, 0.5)
lines = ax1.get_lines() + [bar1]+[bar2] + ax3.get_lines()
labels = [line.get_label() for line in lines]
ax1.legend(lines, labels, loc='upper right')

ax1.set_title('Price of ETH, Liquidity and Volume on Ethereum and Polygon')
plt.savefig('/home/zelos/src/UGPCA/result/plot/ETH&Polygon Real Data.png')

# 画TVL的图
# /home/zelos/src/UGPCA/result/csv/uniswap-v3_1.csv
# Date,Timestamp,boba,rsk,moonbeam,era,optimism,celo,base,bsc,polygon,arbitrum,ethereum,Total,avalanche
# 01/01/2023,1672531200,,,,,40188744.01,1106538.254,,,94788372.97,75144311.15,2203049140,2414277106,




# plt.plot(data['block_timestamp'], data['total_liquidity'], label='Total Liquidity')

# plt.xlabel('Time')

# plt.ylabel('Liquidity')

# plt.title('Liquidity - Time')

# plt.savefig('/home/zelos/src/UGPCA/result/plot/liq.png')

# plt.clf()

# data['eth_return_rate'] = (data['price'] - data['price'].shift(1)) / data['price'].shift(1)

# plt.plot(data['block_timestamp'], data['eth_return_rate'], label='Return of ETH Price')

# plt.xlabel('Time')

# plt.ylabel('Return Rate')

# plt.title('Return Rate - Time')

# plt.savefig('/home/zelos/src/UGPCA/result/plot/return.png')