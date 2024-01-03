import os
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import datetime
import matplotlib.dates as mdates
import numpy as np

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

dfs = pd.read_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_ETH_BearBull_bull.csv')


up_dfs = dfs[(dfs['up_persentage'] <= 100) & (dfs['up_persentage'] >= -25)]
low_dfs = dfs[(dfs['low_persentage'] <= 100) & (dfs['low_persentage'] >= -25)]

up_dfs.loc[:, 'is_bear'] = up_dfs['start_time'].apply(is_bear_market)
up_dfs_bear = up_dfs[up_dfs['is_bear']]
up_dfs_bull = up_dfs[~up_dfs['is_bear']]

low_dfs.loc[:, 'is_bear'] = low_dfs['start_time'].apply(is_bear_market)
low_dfs_bear = low_dfs[low_dfs['is_bear']]
low_dfs_bear['low_persentage'] = -low_dfs_bear['low_persentage']
low_dfs_bull = low_dfs[~low_dfs['is_bear']]
low_dfs_bull['low_persentage'] = -low_dfs_bull['low_persentage']
plt.figure(figsize=(10, 6))


bear_market_freq_up, bear_market_bins_up = np.histogram(up_dfs_bear['up_persentage'], bins=100)
bear_market_freq_low, bear_market_bins_low = np.histogram(low_dfs_bear['low_persentage'], bins=100)

log_bear_market_freq_up = np.log(bear_market_freq_up)
log_bear_market_freq_up[np.isinf(log_bear_market_freq_up)] = 0
log_bear_market_freq_low = np.log(bear_market_freq_low)
log_bear_market_freq_low[np.isinf(log_bear_market_freq_low)] = 0

bear_market_fit_up = np.poly1d(np.polyfit(bear_market_bins_up[:-1], log_bear_market_freq_up, 5))
bear_market_fit_low = np.poly1d(np.polyfit(bear_market_bins_low[:-1], log_bear_market_freq_low,5))

plt.bar(bear_market_bins_up[:-1], log_bear_market_freq_up, width=np.diff(bear_market_bins_up), align='edge', label='Upper Price of Bear Market', alpha=0.5, color='red')
plt.bar(bear_market_bins_low[:-1], log_bear_market_freq_low, width=np.diff(bear_market_bins_low), align='edge', label='Lower Price of Bear Market', alpha=0.5, color='blue')

plt.plot(bear_market_bins_up[:-1], bear_market_fit_up(bear_market_bins_up[:-1]), color='red', label='Upper Price Fit of Bear Market')
plt.plot(bear_market_bins_low[:-1], bear_market_fit_low(bear_market_bins_low[:-1]), color='blue', label='Lower Price Fit of Bear Market')


bull_market_freq_up, bull_market_bins_up = np.histogram(up_dfs_bull['up_persentage'], bins=100)
bull_market_freq_low, bull_market_bins_low = np.histogram(low_dfs_bull['low_persentage'], bins=100)

log_bull_market_freq_up = np.log(bull_market_freq_up)
log_bull_market_freq_up[np.isinf(log_bull_market_freq_up)] = 0
log_bull_market_freq_low = np.log(bull_market_freq_low)
log_bull_market_freq_low[np.isinf(log_bull_market_freq_low)] = 0

bull_market_fit_up = np.poly1d(np.polyfit(bull_market_bins_up[:-1], log_bull_market_freq_up, 3))
bull_market_fit_low = np.poly1d(np.polyfit(bull_market_bins_low[:-1], log_bull_market_freq_low, 3))


plt.bar(bull_market_bins_up[:-1], log_bull_market_freq_up, width=np.diff(bull_market_bins_up), align='edge', label='Upper Price of Bull Market', alpha=0.5, color='yellow')
plt.bar(bull_market_bins_low[:-1], log_bull_market_freq_low, width=np.diff(bull_market_bins_low), align='edge', label='Lower Price of Bull Market', alpha=0.5, color='green')

plt.plot(bull_market_bins_up[:-1], bull_market_fit_up(bull_market_bins_up[:-1]), color='yellow', label='Upper Price Fit of Bull Market')
plt.plot(bull_market_bins_low[:-1], bull_market_fit_low(bull_market_bins_low[:-1]), color='green', label='Lower Price Fit of Bull Market')

plt.ylim(bottom=0)

# 绘制直方图
# plt.hist(up_dfs_bear['up_persentage'], bins=100, label='Upper Price', alpha=0.5,color='red')
# plt.hist(low_dfs_bear['low_persentage'], bins=100, label='Lower Price', alpha=0.5,color='blue')

plt.xlabel('Upper/Lower Price Range (%)')
plt.ylabel('Frequency (log)')

plt.title('Probability Density of Upper and Lower Price in Bull/Bear Markets (high alpha address of bull)')
plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/Pos_Range_ETH_Bear_bull.png')

plt.clf()

plt.hist(up_dfs_bull['up_persentage'], bins=100, label='Upper Price', alpha=0.5,color='red')
plt.hist(low_dfs_bull['low_persentage'], bins=100, label='Lower Price', alpha=0.5,color='blue')

plt.xlabel('Upper/Lower Price Range (%)')
plt.ylabel('Frequency (count)')

plt.title('Probability Density of Upper and Lower Price in Bull/Bear Markets')
plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/Pos_Range_ETH_Bull.png')