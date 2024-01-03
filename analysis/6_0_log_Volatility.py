import matplotlib.pyplot as plt
import pandas as pd
import math
from tqdm import tqdm
data = pd.read_csv("/data/research/task2302-uni-annual-analysis/return_rate/ethereum/uni_volume.csv")
# date,liquidity,price,swap_amount
# 2023-01-01,33612410851777965230,1199.785842,80873863.143511
data['date'] = pd.to_datetime(data['date'])
# 计算波动率

price_list = data['price'].tolist()
log_returns = [math.log(price_list[i]/price_list[i+1]) for i in range(len(price_list)-1)]

# 计算滚动7日波动率
n = 7  # 滚动窗口大小为7
window_volatility = []
test = [0,2]
print("num of volatility0: "+str(len(test)))
for i in tqdm(range(n, len(log_returns))):
    window = log_returns[i-n:i]
    sum_ui = sum(window)
    sum_ui_squared = sum([ui**2 for ui in window])
    s = math.sqrt((1/(n-1))*(sum_ui_squared - (1/n)*sum_ui**2))
    r = 1  # 每日时间间隔为1分钟
    fluctuation_rate = s * math.sqrt(r)
    window_volatility.append(fluctuation_rate)


# 创建滚动波动率的时间序列
timestamps = data['date'].tolist()[(n+1):]

print("num of date: "+str(len(timestamps)))
print("num of volatility: "+str(len(window_volatility)))
df = pd.DataFrame({'date': timestamps, 'volatility': window_volatility})

# 保存为CSV文件
df.to_csv('/home/zelos/src/UGPCA/result/csv/volatility_date.csv', index=False)


data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/eth_price_hourly.csv")
# block_timestamp,open,high,low,close
# 2021-12-21 19:00:00,3998.9602573708376,4006.4445151230057,3986.539004827416,4002.954053823359
# 2021-12-21 20:00:00,4002.954053823359,4003.531161230306,4002.954053823359,4003.531161230306
data['block_timestamp'] = pd.to_datetime(data['block_timestamp'])
# 计算波动率

price_list = data['close'].tolist()
log_returns = [math.log(price_list[i]/price_list[i+1]) for i in range(len(price_list)-1)]

# 计算滚动1日波动率
n = 24  # 滚动窗口大小为7
window_volatility = []
test = [0,2]
print("num of volatility0: "+str(len(test)))
for i in tqdm(range(n, len(log_returns))):
    window = log_returns[i-n:i]
    sum_ui = sum(window)
    sum_ui_squared = sum([ui**2 for ui in window])
    s = math.sqrt((1/(n-1))*(sum_ui_squared - (1/n)*sum_ui**2))
    r = 1  # 每日时间间隔为1分钟
    fluctuation_rate = s * math.sqrt(r)
    window_volatility.append(fluctuation_rate)


# 创建滚动波动率的时间序列
timestamps = data['block_timestamp'].tolist()[(n):-1]

print("num of date: "+str(len(timestamps)))
print("num of volatility: "+str(len(window_volatility)))
df = pd.DataFrame({'date': timestamps, 'volatility': window_volatility})

# 保存为CSV文件
df.to_csv('/home/zelos/src/UGPCA/result/csv/volatility_hour.csv', index=False)






# data.to_csv('/home/zelos/src/UGPCA/result/csv/Volatility_Bear_Bull.csv', index=False)
# plt.figure(figsize=(12, 6))
# plt.plot(log_returns)
# plt.xlabel('Time')
# plt.ylabel('Log Returns')
# plt.title('Volatility')
# plt.savefig('/home/zelos/src/UGPCA/result/plot/Volatility_log.png')

