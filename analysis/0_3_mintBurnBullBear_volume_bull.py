import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

folder_path = '/home/zelos/src/UGPCA/result/csv/02_init'

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
    for time_range, market_status in market_dict.items():
        start_date = datetime.strptime(time_range[0], "%Y-%m-%d %H:%M:%S").date()
        end_date = datetime.strptime(time_range[1], "%Y-%m-%d %H:%M:%S").date()

        if start_date <= date <= end_date:
            if market_status == "bear":
                return True
            elif market_status == "bull":
                return False
    return False
for file in os.listdir(folder_path):
    if file.endswith('.csv'):
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        df['blk_time'] = pd.to_datetime(df['blk_time'], format='%Y-%m-%d %H:%M:%S')
        df = df[df['blk_time'].dt.year >= 2023]
        df['date'] = df['blk_time'].dt.date
        
        # 按日期、tx_type进行分组并计数
        count = df.groupby(['date', 'tx_type']).size().unstack(fill_value=0)

        for date, row in count.iterrows():
  
            if date not in bear_mint_count:
                bear_mint_count[date] = 0
            if date not in bear_burn_count:
                bear_burn_count[date] = 0
            if date not in bull_mint_count:
                bull_mint_count[date] = 0
            if date not in bull_burn_count:
                bull_burn_count[date] = 0
            
            if 'MINT' in row and row['MINT'] != 0:
                if is_bear_market(date):
                    bear_mint_count[date] += row['MINT']
                else:
                    bull_mint_count[date] += row['MINT']
            elif  'BURN' in row and row['BURN'] != 0 :
                if is_bear_market(date):
                    bear_burn_count[date] += row['BURN']
                else:
                    bull_burn_count[date] += row['BURN']

# 提取日期和对应的数据
dates = list(bear_burn_count.keys())
# bear_burn_counts = [bear_burn_count[date] for date in dates]
# bull_burn_counts = [bull_burn_count[date] for date in dates]


# bear_mint_counts = [bear_mint_count[date] for date in dates]
# bull_mint_counts = [bull_mint_count[date] for date in dates]

print(bear_burn_count)
print(bull_burn_count)
print(bear_mint_count)
print(bull_mint_count)

df_BM_bull_bear = pd.DataFrame(columns=['date', 'market_type', 'burn_count','mint_count'])

for i in range(len(dates)):
    if(is_bear_market(dates[i])):
        df_BM_bull_bear.loc[i] = [dates[i], 'bear', bear_burn_count[dates[i]],bear_mint_count[dates[i]]]
    else:
        df_BM_bull_bear.loc[i] = [dates[i], 'bull', bull_burn_count[dates[i]],bull_mint_count[dates[i]]]

df_BM_bull_bear.to_csv('/home/zelos/src/UGPCA/result/csv/BM_bull_bear_capmBULL.csv', index=False)




# csv_path = "/data/research/uni_return_rate_service/4_price.csv"
# data = pd.read_csv(csv_path, dtype={'total_liquidity': float,'price': float})
# data['block_timestamp'] = pd.to_datetime(data.iloc[:, 0])



# plt.figure(figsize=(10, 6))

# fig, ax = plt.subplots(figsize=(8, 6))

# ax.bar(dates, bear_burn_counts, label='Bear Market',color = "red")
# ax.bar(dates, bull_burn_counts, bottom=bear_burn_counts, label='Bull Market',color = "green")
# plt.plot(data['block_timestamp'], data['price']/10, label='Price',alpha = 0.5)
# plt.xlabel('Date')
# plt.ylabel('BURN Count')
# plt.title('BURN Count over Time')
# plt.legend()
# plt.xticks(rotation=45)

# plt.savefig('/home/zelos/src/UGPCA/result/plot/BURN Count over bull and bear.png')

# plt.clf()

# fig, ax = plt.subplots(figsize=(8, 6))
# ax.bar(dates, bear_mint_counts, label='Bear Market',color = "red")
# ax.bar(dates, bull_mint_counts, bottom=bear_mint_counts, label='Bull Market',color = "green")
# plt.plot(data['block_timestamp'], data['price']*2/3, label='Price',alpha = 0.5)
# plt.xlabel('Date')
# plt.ylabel('MINT Count')
# plt.title('MINT Count over Time')
# plt.legend()
# plt.xticks(rotation=45)

# plt.ylim(0,max(bear_mint_counts))
# plt.savefig('/home/zelos/src/UGPCA/result/plot/MINT Count over bull and bear.png')

# plt.clf()