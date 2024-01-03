from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt, ticker
import math
from tqdm import tqdm
import os

# 从CSV文件中读取数据
df = pd.read_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX.csv')
df = df.drop_duplicates()
fig, ax = plt.subplots()

def weighted_average(group):
    total_liquidity = group['liquidity'].sum()
    weighted_up_percentage = (group['liquidity'] * group['up_percentage']).sum() / total_liquidity
    weighted_low_percentage = -(group['liquidity'] * group['low_percentage']).sum() / total_liquidity
    result = {'volatility_ranges': group['volatility_ranges'].iloc[0],
              'weighted_up_percentage': weighted_up_percentage,
              'weighted_low_percentage': weighted_low_percentage}
    return result

# 按照'volatility_ranges'分组，并应用weighted_average函数
result_list = df.groupby('volatility_ranges').apply(weighted_average).tolist()

result_dict_list = [{item['volatility_ranges']: item} for item in result_list]
result_dict = {k: v for item in result_dict_list for k, v in item.items()}

# df.to_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX_weighted.csv', index=False)

data_list = []
for range_tuple, data in result_dict.items():
    up_percentages = data['weighted_up_percentage']
    low_percentages = data['weighted_low_percentage']
    data_list.append([range_tuple, up_percentages, low_percentages])

# 创建DataFrame对象
df = pd.DataFrame(data_list, columns=['volatility_ranges', 'up_percentage', 'low_percentage'])

# 将DataFrame保存为CSV文件
df.to_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX_weighted.csv', index=False)

result_dict = pd.read_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX_weighted.csv')

for index, row in tqdm(result_dict.iterrows(), desc='Processing Bin'):
    range_tuple = eval(row['volatility_ranges'])
    up_percentage = row['up_percentage']
    low_percentage = row['low_percentage']

    # 计算长方形的高度和宽度
    height = up_percentage - low_percentage
    width = range_tuple[1] - range_tuple[0]

    # 计算长方形的坐标
    x = range_tuple[0]
    y = low_percentage

    # 绘制填充长方形
    ax.fill_between([x, x+width], [y, y], [y+height, y+height], alpha=1,color = '#1f77b4')

# 设置图表的横轴和纵轴标签
ax.set_xlabel('Volatility Ranges')
ax.set_ylabel('Position up-low Percentages')

# 显示图表
plt.savefig('/home/zelos/src/UGPCA/result/plot/Pos_Range_VOX_weighted.png')