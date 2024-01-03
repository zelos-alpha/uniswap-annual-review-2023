import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import matplotlib.patches as mpatches
spot_df = pd.read_csv('/data/research/task2302-uni-annual-analysis/return_rate/ethereum/uni_volume.csv')
# date,liquidity,price,swap_amount
spot_df['date'] = pd.to_datetime(spot_df['date'], format='%Y-%m-%d')

spot_df['date'] = spot_df['date'].dt.date


burn_df = pd.read_csv('/home/zelos/src/UGPCA/result/csv/BM_bull_bear.csv')
burn_df['date'] = pd.to_datetime(burn_df['date'], format='%Y-%m-%d')
burn_df['date'] = burn_df['date'].dt.date

# 合并两个DataFrame，根据date字段进行对应关系的匹配
merged_df = pd.merge(spot_df, burn_df, on='date')
merged_df['swap_amount'] = merged_df['swap_amount'].astype(float)

# 排序'liquidity'列，并选择除了最后三个索引之外的所有行
sorted_indices = np.argsort(merged_df['swap_amount'])
trimmed_indices = sorted_indices[:-3]
trimmed_df = merged_df.iloc[trimmed_indices]

# 找到'mint count'列最大值的索引
max_index = trimmed_df['mint_count'].idxmax()

# 删除'mint count'列最大值的行
trimmed_df = trimmed_df.drop(max_index)

print(trimmed_df.columns)

plt.bar(trimmed_df['swap_amount'], trimmed_df['burn_count'],  alpha=0.7,color=np.where(trimmed_df['market_type'] == 'bull', 'yellowgreen', 'crimson'))
# plt.scatter(trimmed_df['swap_amount'], trimmed_df['mint_count'], s=7, alpha=0.7,c=np.where(trimmed_df['market_type'] == 'bull', 'yellowgreen', 'crimson'))
# plt.scatter(trimmed_df['liquidity'], trimmed_df['mint_count'], s=7, alpha=0.7)

plt.xlabel('Spot Volume')
plt.ylabel('Burn Count')
plt.title('Spot Volume vs Burn Count')


# plt.xlabel('Spot Volume')
# plt.ylabel('Mint Count')
# plt.title('Spot Volume vs Mint Count')

# plt.legend(['Burn Count', 'Mint Count'])


bull_point = plt.scatter([], [], color='yellowgreen', marker='o') 
bear_point = plt.scatter([], [], color='crimson', marker='o')  
plt.legend((bull_point, bear_point), ('bull', 'bear'))

# 设置x轴刻度间隔
x_ticks = np.linspace(trimmed_df['swap_amount'].min(), trimmed_df['swap_amount'].max(), 5)
plt.xticks(x_ticks)

plt.savefig('/home/zelos/src/UGPCA/result/plot/Spot Volume vs Burn Count.png')
plt.show()