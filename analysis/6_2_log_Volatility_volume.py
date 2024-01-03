import matplotlib.pyplot as plt
import pandas as pd
import math
from tqdm import tqdm
data = pd.read_csv("/data/research/task2302-uni-annual-analysis/return_rate/ethereum/uni_volume.csv")
# date,liquidity,price,swap_amount
# 2023-01-01,33612410851777965230,1199.785842,80873863.143511
data['date'] = pd.to_datetime(data['date'])
# 计算波动率

price_list = data['swap_amount'].tolist()
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
df.to_csv('/home/zelos/src/UGPCA/result/csv/volatility_volume_date.csv', index=False)