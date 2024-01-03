import os
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import datetime
import matplotlib.dates as mdates

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
# ETH价格数据
eth_price_data = pd.read_csv("/data/research/uni_return_rate_service/4_price.csv", dtype={'total_liquidity': float,'price': float})
eth_price_data['block_timestamp'] = pd.to_datetime(eth_price_data.iloc[:, 0])
eth_price_data['start_time'] = eth_price_data['block_timestamp']
# block_timestamp,price,total_liquidity,sqrtPriceX96
folder_path = '/home/zelos/src/UGPCA/result/csv/pos_analysis_bull'

dfs = pd.DataFrame(columns=['id','holding_time','pos_inter_time','pos_inter_price','pos_num','start_time','end_time','up_price','low_price','up_persentage','low_persentage','liquidity'])


for file_name in tqdm(os.listdir(folder_path), desc='Processing Files1'):
    if file_name.endswith(".csv"):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        # df['is_bear'] = df['start_time'].apply(is_bear_market)  # 添加一列用于标记bear市场
        df.loc[df['start_time'] == '2023-01-01', 'start_time'] = '2023-01-01 00:00:00'
        df['start_time'] = pd.to_datetime(df['start_time'])
        df = pd.merge(df, eth_price_data, on='start_time', how='left')

        df['up_persentage'] = ((df['up_price']-df['price'])/df['price'])*100
        df['low_persentage'] = ((df['price']-df['low_price'])/df['price'])*100
        dfs = dfs._append(df, ignore_index=True)


dfs.to_csv('/home/zelos/src/UGPCA/result/csv/Pos_Range_ETH_BearBull_bull.csv', index=False)

plt.figure()
plt.hist(dfs['up_persentage'], bins=10, label='Upper Price', alpha=0.5)
plt.xlabel('Upper Price Range (%)')
plt.ylabel('Frequency (count)')

plt.title('Probability Density of Upper Price in Bear/Bull Markets')
plt.legend()

plt.savefig('/home/zelos/src/UGPCA/result/plot/Pos_Range_ETH_BearBull.png')