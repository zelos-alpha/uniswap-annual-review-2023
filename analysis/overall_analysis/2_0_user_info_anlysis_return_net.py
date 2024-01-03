import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import numpy as np
from datetime import timedelta
import matplotlib.pyplot as plt
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib


file_path = '/home/zelos/src/UGPCA/result/csv/user_info_total.csv'
save_path = '/home/zelos/src/UGPCA/result/plot'
file_path1 = '/home/zelos/src/UGPCA/result/csv/user_info_total1.csv'
file_path2 = '/home/zelos/src/UGPCA/result/csv/user_info_total2.csv'
file_path3 = '/home/zelos/src/UGPCA/result/csv/user_info_total3.csv'
# 'Name', 'Holding Time Per', 'Max Net Value','Annual Return','Excess Rerturn','beta','alpha'
df = pd.read_csv(file_path)

df['Max Net Value'] = df['Max Net Value'].astype(float)
df = df.sort_values(by='Max Net Value')
print(len(df))
df['max_net_value_per'] = df['Max Net Value'] / df['Max Net Value'].max()
df = df[df['max_net_value_per'] >= 0.01]
print(len(df))
df.to_csv(file_path2, index=False)
df['Holding Time Per'] = df['Holding Time Per'].astype(float)
df = df[df['Holding Time Per'] >=0.01]
print(len(df))
# df = df[df['mean_return_rate'] >=0]
df.to_csv(file_path3, index=False)
