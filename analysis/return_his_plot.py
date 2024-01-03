import pandas as pd
import matplotlib.pyplot as plt

# 读取CSV文件
data = pd.read_csv('/home/zelos/src/UGPCA/result/csv/net_value_result_demo.csv', dtype={'return_rate': float,'cumulate_return_rate': float})
# ,lp_net_value,lp_return_rate,lp_cumulate_return_rate,positions,lp_fee0,lp_fee1,lp_amount0,lp_amount1,cash_amount0,cash_amount1,price,cash_net_value,net_value,price_prev,net_value_start,return_rate,cumulate_return_rate
# 提取return列的数据

data['block_timestamp'] = pd.to_datetime(data.iloc[:, 0])
data = data[(data['block_timestamp'] >= '2023-01-01')]

plt.figure(figsize=(10, 6))

plt.plot(data['block_timestamp'], data['return_rate'], label='return_rate')
plt.plot(data['block_timestamp'], data['cumulate_return_rate'], label='cumulate_return_rate')

plt.xlabel('Time')

plt.ylabel('return_rate')

plt.title('return rate and cumulate return rate with cash - Time')
plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/return_rate_and_cum_cash.png')
