import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
csv_path = '/data/research/task2302-uni-annual-analysis/return_rate/ethereum/uni_volume.csv'
# date,liquidity,price,swap_amount

data = pd.read_csv(csv_path, dtype={'liquidity': float,'price':float,'swap_amount':float})
data['date'] = pd.to_datetime(data.iloc[:, 0])

plt.figure(figsize=(10, 6))

plt.plot(data['date'], data['liquidity'], label='Liquidity on ETH')
plt.xlabel('Time')
plt.ylabel('Liquidity on ETH')
plt.title('Liquidity on ETH - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/Liquidity&swap_amount&price on ETH.png')
plt.clf()


plt.plot(data['date'], data['price'], label='Price on ETH')
plt.xlabel('Time')
plt.ylabel('Price on ETH')
plt.title('Price on ETH - Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/Price on ETH.png')
plt.clf()

plt.plot(data['date'], data['swap_amount'], label='swap_amount on ETH')
plt.xlabel('Time')
plt.ylabel('Swap_amount on ETH')

plt.title('Swap_amount on ETH - Time')

plt.savefig('/home/zelos/src/UGPCA/result/plot/Swap_amount on ETH.png')
