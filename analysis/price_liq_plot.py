import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
csv_path = "/data/research/uni_return_rate_service/4_price.csv"
# block_timestamp,price,total_liquidity,sqrtPriceX96

data = pd.read_csv(csv_path, dtype={'total_liquidity': float,'price': float})
data['block_timestamp'] = pd.to_datetime(data.iloc[:, 0])
data = data[(data['block_timestamp'] >= '2023-01-01')]

plt.figure(figsize=(10, 6))

plt.plot(data['block_timestamp'], data['price'], label='Price')

plt.xlabel('Time')

plt.ylabel('Price')

plt.title('Price of ETH - Time')

plt.savefig('/home/zelos/src/UGPCA/result/plot/price.png')

plt.clf()

plt.plot(data['block_timestamp'], data['total_liquidity'], label='Total Liquidity')

plt.xlabel('Time')

plt.ylabel('Liquidity')

plt.title('Liquidity - Time')

plt.savefig('/home/zelos/src/UGPCA/result/plot/liq.png')

plt.clf()

data['eth_return_rate'] = (data['price'] - data['price'].shift(1)) / data['price'].shift(1)

plt.plot(data['block_timestamp'], data['eth_return_rate'], label='Return of ETH Price')

plt.xlabel('Time')

plt.ylabel('Return Rate')

plt.title('Return Rate - Time')

plt.savefig('/home/zelos/src/UGPCA/result/plot/return.png')