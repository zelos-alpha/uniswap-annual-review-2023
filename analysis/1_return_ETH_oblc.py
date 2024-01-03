import pandas as pd
import matplotlib.pyplot as plt
import matplotlib


colors = {"bear": "red", "bull": "green", "neutral": "blue"}

time = ["2021-12-26 19:10:00","2022-01-23 19:10:00","2022-04-03 19:10:00","2022-07-10 19:10:00","2022-08-14 19:10:00","2023-01-01 19:10:00","2023-04-16 19:10:00","2023-06-18 19:10:00","2023-08-13 19:10:00","2023-10-22 19:10:00","2023-12-03 19:10:00"]
market_dict = {
    ("2021-12-26 19:10:00", "2022-01-23 19:10:00"): "bear",
    ("2022-01-23 19:10:00", "2022-04-03 19:10:00"): "bull",
    ("2022-04-03 19:10:00", "2022-07-10 19:10:00"): "bear",
    ("2022-07-10 19:10:00", "2022-08-14 19:10:00"): "bull",
    ("2022-08-14 19:10:00", "2023-01-01 19:10:00"): "bear",
    ("2023-01-01 19:10:00", "2023-04-16 19:10:00"): "bull",
    ("2023-04-16 19:10:00", "2023-06-18 19:10:00"): "bear",
    ("2023-06-18 19:10:00", "2023-08-13 19:10:00"): "bull",
    ("2023-08-13 19:10:00", "2023-10-22 19:10:00"): "bear",
    ("2023-10-22 19:10:00", "2023-12-03 19:10:00"): "bull"
}




csv_path = "/data/research/uni_return_rate_service/4_price.csv"

# 将时间列转换为日期时间类型
data = pd.read_csv(csv_path, dtype={'total_liquidity': float,'price': float})
data['block_timestamp'] = pd.to_datetime(data.iloc[:, 0])

# 设置时间列为索引
data.set_index('block_timestamp', inplace=True)

# 将每分钟数据聚合为每天数据
daily_eth_prices = data['price'].resample('H').ohlc()
daily_eth_prices= daily_eth_prices.reset_index()
daily_eth_prices.to_csv('/home/zelos/src/UGPCA/result/csv/eth_price_hourly.csv', index=False)
# 计算每一天的ETH回报率
# daily_eth_prices['daily_eth_returns'] = daily_eth_prices['close'].pct_change()

# plt.figure(figsize=(10, 6))
# for i in range(len(time)-1):
#     start_time = time[i]
#     end_time = time[i+1]
    
#     filtered_data = daily_eth_prices[(daily_eth_prices.index >= start_time) & (daily_eth_prices.index <= end_time)]

#     if(i%2==0):
#         # 熊市
#         plt.plot(filtered_data.index, filtered_data['daily_eth_returns'], color='red')
#     else:
#         plt.plot(filtered_data.index, filtered_data['daily_eth_returns'], color='green')
        
# plt.plot([], color='green', label="Bull Market")
# plt.plot([], color='red', label= "Bear Market")

# plt.xlabel('Date')
# plt.ylabel('ETH Return Rate')
# plt.title('Daily ETH Return Rate')
# plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/bear_bull_return_daily.png')