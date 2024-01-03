import matplotlib.pyplot as plt
import pandas as pd
# ETH价格数据（示例数据）
csv_path = "/data/research/uni_return_rate_service/4_price.csv"
# block_timestamp,price,total_liquidity,sqrtPriceX96

data = pd.read_csv(csv_path, dtype={'total_liquidity': float,'price': float})
data['block_timestamp'] = pd.to_datetime(data.iloc[:, 0])

weekly_range = pd.date_range(start=data['block_timestamp'].min(), end=data['block_timestamp'].max(), freq='W')

# 计算价格涨跌幅（波动率）
price_changes = []

for start_date, end_date in zip(weekly_range[:-1], weekly_range[1:]): # 前后两个week时间点开始计算change
    weekly_prices = data[(data['block_timestamp'] >= start_date) & (data['block_timestamp'] < end_date)]['price']
    # 以防取不到完整的week
    if len(weekly_prices) > 1:
        change = (weekly_prices.iloc[-1] - weekly_prices.iloc[0]) / weekly_prices.iloc[0] * 100
    else:
        change = 0
    
    print("From {} to {}, the change of price is: {:.2f}%".format(start_date, end_date, change))
    price_changes.append(change)

# 设置熊市和牛市的涨跌幅阈值（示例阈值）
bear_threshold = -5
bull_threshold = 5

# 划分熊市和牛市
markets = []
for change in price_changes:
    if change <= bear_threshold:
        markets.append("bear")
    elif change >= bull_threshold:
        markets.append("bull")
    else:
        markets.append("neutral")

colors = {"bear": "red", "bull": "green", "neutral": "blue"}
plt.figure(figsize=(10, 6))
# 价格折线图
plt.plot(data['block_timestamp'], data['price'])
plt.xlabel('Time')
plt.ylabel('Price')

# 市场划分标记
for i, market in enumerate(markets):
    if market == 'bear':
        bear_prices = data[(data['block_timestamp'] >= weekly_range[i]) & (data['block_timestamp'] < weekly_range[i+1])]['price']
        plt.fill_between([weekly_range[i], weekly_range[i+1]], bear_prices.min(), bear_prices.max(), color='red', alpha=0.5) # 用红色区域表示熊市
    elif market == 'bull':
        bull_prices = data[(data['block_timestamp'] >= weekly_range[i]) & (data['block_timestamp'] < weekly_range[i+1])]['price']
        plt.fill_between([weekly_range[i], weekly_range[i+1]], bull_prices.min(), bull_prices.max(), color='green', alpha=0.5) # 用绿色区域表示牛市
    else:
        neutral_prices = data[(data['block_timestamp'] >= weekly_range[i]) & (data['block_timestamp'] < weekly_range[i+1])]['price']
        plt.fill_between([weekly_range[i], weekly_range[i+1]], neutral_prices.min(), neutral_prices.max(), color='white', alpha=0.5) # 用黄色区域表示中性市场
plt.title('Bear Bull Divide')
plt.show()

plt.savefig('/home/zelos/src/UGPCA/result/plot/bull_bear.png')
