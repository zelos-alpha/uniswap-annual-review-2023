import os
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import re
import datetime
import numpy as np
folder_path = '/home/zelos/src/UGPCA/result/csv/pos_analysis_bull'
# id,holding_time,pos_inter_time,pos_inter_price,pos_num,start_time
# 200310209470,20 days 11:12:00,20 days 11:12:00,1199.8748754944736,1,2023-01-01

bear_mint_count = {}
bear_burn_count = {}
bull_mint_count = {}
bull_burn_count = {}

def is_bear_market(date):
    market_dict = {
    ("2023-01-01 00:00:00", "2023-03-11 00:00:00"): "bear",
    ("2023-03-11 00:00:00", "2023-08-17 00:00:00"): "bull",
    ("2023-08-17 00:00:00", "2023-10-20 00:00:00"): "bear",
    ("2023-10-24 00:00:00", "2023-12-17 00:00:00"): "bull"
    }
    try:
        date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    for time_range, market_status in market_dict.items():
        start_date = datetime.datetime.strptime(time_range[0], "%Y-%m-%d %H:%M:%S").date()
        end_date = datetime.datetime.strptime(time_range[1], "%Y-%m-%d %H:%M:%S").date()

        if start_date <= date <= end_date:
            if market_status == "bear":
                return True
            elif market_status == "bull":
                return False
    return False

def convert_to_seconds(time_str):
    match = re.match(r'(\d+) days (\d{2}):(\d{2}):(\d{2})', time_str)
    if match:
        days = int(match.group(1))
        hours = int(match.group(2))
        minutes = int(match.group(3))
        seconds = int(match.group(4))
        total_seconds = (days * 24 * 60 * 60) + (hours * 60 * 60) + (minutes * 60) + seconds
        return total_seconds
    else:
        return None

def convert_to_timedelta(time_str):
    time_parts = time_str.split(", ")
    time_dict = {part.split(" ")[1]: int(part.split(" ")[0]) for part in time_parts}
    return datetime.timedelta(**time_dict)

all_data = pd.DataFrame()
for file_name in tqdm(os.listdir(folder_path), desc='Processing Files1'):
    if file_name.endswith(".csv"):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        df['is_bear'] = df['start_time'].apply(is_bear_market)  # 添加一列用于标记bear市场
        all_data = all_data._append(df)

# all_data['holding_time_sec'] = all_data['pos_inter_time'].apply(convert_to_seconds)
all_data['holding_time_sec'] = all_data['pos_inter_time'].apply(convert_to_timedelta)
all_data = all_data[all_data['holding_time_sec'] >= datetime.timedelta(seconds=0)]

bear_market_data = all_data[all_data['is_bear']].copy()
bull_market_data = all_data[~all_data['is_bear']].copy()

bear_market_data.to_csv('/home/zelos/src/UGPCA/result/csv/prob_bear_market_holding_time.csv', index=False)
bull_market_data.to_csv('/home/zelos/src/UGPCA/result/csv/prob_bull_market_holding_time.csv', index=False)

bear_market_data['holding_time_sec_float'] = bear_market_data['holding_time_sec'].apply(lambda x: x.total_seconds())
bull_market_data['holding_time_sec_float'] = bull_market_data['holding_time_sec'].apply(lambda x: x.total_seconds())

bear_market_data =  bear_market_data[bear_market_data['holding_time_sec_float'] >= 0]
# bull_market_density =  bull_market_density[bull_market_density['holding_time_sec_float'] >= 0]

bear_market_data['holding_time_sec_float'] = bear_market_data['holding_time_sec_float']/ (24 * 60 * 60)
bull_market_data['holding_time_sec_float'] = bull_market_data['holding_time_sec_float']/ (24 * 60 * 60)

# 统计各个市场下holding_time的概率密度
plt.figure()

bear_market_freq, bear_market_bins = np.histogram(bear_market_data['holding_time_sec_float'], bins=50)
bull_market_freq, bull_market_bins = np.histogram(bull_market_data['holding_time_sec_float'], bins=50)

log_bear_market_freq = np.log(bear_market_freq)
log_bull_market_freq = np.log(bull_market_freq)

bear_market_fit = np.poly1d(np.polyfit(bear_market_bins[:-1], log_bear_market_freq, 3))
bull_market_fit = np.poly1d(np.polyfit(bull_market_bins[:-1], log_bull_market_freq, 3))

plt.bar(bear_market_bins[:-1], log_bear_market_freq, width=np.diff(bear_market_bins), align='edge', label='Bear Market', alpha=0.5, color='crimson')
plt.bar(bull_market_bins[:-1], log_bull_market_freq, width=np.diff(bull_market_bins), align='edge', label='Bull Market', alpha=0.5, color='yellowgreen')

# plt.plot(bear_market_bins[:-1], bear_market_fit(bear_market_bins[:-1]), color='crimson', label='Bear Market Fit')
# plt.plot(bull_market_bins[:-1], bull_market_fit(bull_market_bins[:-1]), color='yellowgreen', label='Bull Market Fit')

# plt.hist(log_bear_market_data, bins=50, label='Bear Market', alpha=0.5,color = 'red')
# plt.hist(log_bull_market_data,  bins=50,label='Bull Market', alpha=0.5,color = 'green')


plt.xlabel('Holding Time (days)')
plt.ylabel('Frequency (log)')
# bear_market_density = bear_market_data['holding_time_sec_float'].plot(kind='density', label='Bear Market')
# bull_market_density = bull_market_data['holding_time_sec_float'].plot(kind='density', label='Bull Market')

# x_ticks = bear_market_density.get_xticks()

# x_labels = [f"{int(x/3600/24)} days" for x in x_ticks]

# plt.xticks(x_ticks, x_labels)


plt.title('Probability Density of Holding Time in Bear/Bull Markets (High Alpha in Bull)')
plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/Probability Density of holding_time in BearBull Markets Bull.png')