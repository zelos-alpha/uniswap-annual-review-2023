import os
import pandas as pd
from tqdm import tqdm
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler

save_path = '/home/zelos/src/UGPCA/result/csv/alpha'
if not os.path.exists(save_path):
    os.makedirs(save_path)

index_path = '/home/zelos/src/UGPCA/result/csv/user_info_total.csv'
info_df = pd.read_csv(index_path,index_col=0)


def _is_have_position(start_date,end_date,address):
    file = "/home/zelos/src/UGPCA/result/csv/pos_analysis/{}.csv".format(address)
    pd_df = pd.read_csv(file)
    # pd_df columns: ['id', 'holding_time', 'pos_inter_time', 'pos_inter_price', 'pos_num','start_time', 'end_time', 'up_price', 'low_price']
    



file_paths = None
folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/ethereum/9_address_result'
file_paths = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
file_paths[:5]


df_eth = pd.read_csv('/data/research/task2302-uni-annual-analysis/return_rate/ethereum/2_price.csv', sep=',', index_col=[],  header=0)
# block_timestamp,price,total_liquidity,sqrtPriceX96
# 2023-01-01 00:00:00,1195.5839948533555,39974582212968359597,2291340004214720379120476440275511

df_eth['block_timestamp'] = pd.to_datetime(df_eth.iloc[:, 0])

df_eth.set_index('block_timestamp', inplace=True)

hourly_eth_prices = df_eth['price'].resample('H').ohlc()

# 计算每一的小时回报率
hourly_eth_prices['hourly_eth_returns'] = hourly_eth_prices['close'].pct_change()
hourly_eth_prices['hour'] = hourly_eth_prices.index


def eth_ver():
    result_df = pd.DataFrame(columns=['address', 'alpha', 'beta'])
    no_position = 0
    no_alpha = 0
    for address_name in tqdm(info_df.index, desc='Processing Files'):
        df = pd.read_csv(folder_path+"/"+address_name+".csv")
        df['hour'] = pd.to_datetime(df.iloc[:, 0])
try:
    df["hourly_return_u"] = df["return_rate"]-1
    
    # df["daily_returna_u"] = df["hourly_return_u"].apply(lambda x: (1 + x) ** 24 - 1)
except:
    no_position += 1
    continue

merged_df = pd.merge(df, hourly_eth_prices, on='hour', how='inner')
merged_df = merged_df[(merged_df['hour'] >= '2023-01-01') & (merged_df['hour'] <= '2023-12-31')]

ri = merged_df['hourly_return_u'].values  # 资产收益率
rm = merged_df['hourly_eth_returns'].values  # 市场收益率
ri = ri.astype('float64')
rm = rm.astype('float64')
X = np.vstack([rm, np.ones(len(rm))]).T
X = X.astype('float64')
        if len(ri) != len(rm):
            raise ValueError("The lengths of `ri` and `rm` are not equal!")
        try:
            beta,alpha= np.linalg.lstsq(X, ri, rcond=None)[0]
            result_df = result_df._append({'address': address_name, 'alpha': alpha, 'beta': beta}, ignore_index=True)
            
        except:
            no_alpha += 1
            continue
    print("no position: "+str(no_position))
    print("no alpha: "+str(no_alpha))
    result_df = result_df.sort_values('alpha', ascending=False)
    new_file_name = f'{save_path}/Month12_address_alpha_beta_eth_total.csv'
    result_df.to_csv(new_file_name, index=False)
eth_ver()
