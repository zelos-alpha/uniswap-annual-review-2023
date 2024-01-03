import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import numpy as np
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib
pos_cal_path = '/home/zelos/src/UGPCA/result/csv/pos_analysis'
file_path3 = '/home/zelos/src/UGPCA/result/csv/user_info_total3.csv'
pos_paths = [os.path.join(pos_cal_path, file) for file in os.listdir(pos_cal_path) if file.endswith('.csv')]
def is_bear_market(date):
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
    for time_range, market_status in market_dict.items():
        start_date = datetime.strptime(time_range[0], "%Y-%m-%d %H:%M:%S").date()
        end_date = datetime.strptime(time_range[1], "%Y-%m-%d %H:%M:%S").date()

        if start_date <= date <= end_date:
            if market_status == "bear":
                return True
            elif market_status == "bull":
                return False
    return False
df_list=pd.read_csv(file_path3)
fig, ax = plt.subplots(figsize=(8, 6))



for file_path in tqdm(pos_paths, desc='read address list'):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    #print(file_name)
    if df_list['Name'].isin([file_name]).any():
        data = pd.read_csv(file_path)
        holding_time = data['holding_time']
        start_time = pd.to_datetime(data['start_time']) # 将 "start_time" 列转换为日期时间格式
        start_time = start_time.dt.date
        for i in range(len(start_time)):
            # 根据 "start_time" 在 bear market 还是 bull market 来决定点的颜色
            if is_bear_market(start_time[i]):
                colors = 'red'
            else:
                colors = 'green'

            # 绘制散点图
            ax.scatter(start_time[i], holding_time[i], color=colors)

# 添加标签和标题
fig.autofmt_xdate()
fig.autofmt_ydate()
ax.set_xlabel('Start Time')
ax.set_ylabel('Holding Time')
ax.set_title('Holding Time in Bear & Bull Market')

# 显示图形
plt.tight_layout()
plt.savefig('/home/zelos/src/UGPCA/result/plot/Holding_time_bear_bull.png')