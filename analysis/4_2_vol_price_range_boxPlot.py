from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt, ticker
import math
from tqdm import tqdm
import os
data = pd.read_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_VOX_bull_bull.csv')

data['volatility_ranges'] = data['volatility_ranges'].str.replace('(', '')
data['volatility_ranges'] = data['volatility_ranges'].str.replace(')', '')
bounds = data['volatility_ranges'].str.split(',', expand=True).astype(float)
data['volatility_mid'] = ((bounds[0] + bounds[1]) / 2)*100
data['volatility_mid'] = data['volatility_mid'].round(decimals=1)
data['low_percentage'] = -data['low_percentage']

color = ['pink','mediumpurple','lightsalmon','powderblue','gold','aquamarine']
grouped_data = data.groupby('volatility_mid')['up_percentage'].apply(list)
plt.figure(figsize=(10, 6))
# 绘制箱线图
bp = plt.boxplot(grouped_data.values, labels=grouped_data.index, patch_artist=True, notch=True, 
            flierprops={ 'markeredgecolor': 'gold'},medianprops={'color': 'crimson', 'linewidth': 2})

for patch in bp['boxes']:
    patch.set_facecolor('gold')

grouped_data_low = data.groupby('volatility_mid')['low_percentage'].apply(list)
bp2 = plt.boxplot(grouped_data_low.values, labels=grouped_data_low.index, patch_artist=True, notch=True, 
            flierprops={ 'markeredgecolor': 'aquamarine'},medianprops={'color': 'blue', 'linewidth': 2})
for patch in bp2['boxes']:
    patch.set_facecolor('aquamarine')

plt.bar([],[], color='aquamarine', label="Lower Price")
plt.bar([],[], color='gold', label= "Upper Price")

plt.xlabel('Volatility Ranges (Mid Point)')

# 设置 y 轴标签
plt.ylabel('Up/Low Percentage (%)')
labels = ['upper price', 'lower price']
# 设置标题
plt.title('Boxplot of Price Range over Volatility for Address with High Alpha under Bear Market')
plt.legend(labels)
# 显示图形
plt.show()
plt.savefig('/home/zelos/src/UGPCA/result/plot/Pos_Range_VOX_boxplot_bull_bear.png')