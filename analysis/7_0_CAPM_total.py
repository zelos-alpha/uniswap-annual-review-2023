import os
import pandas as pd
from tqdm import tqdm
import numpy as np
from datetime import date

chain = "ethereum"
save_path = '/home/zelos/src/UGPCA/result/csv/alpha'
if not os.path.exists(save_path):
    os.makedirs(save_path)


folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/{}/9_address_result'.format(chain)


address_list = [file.split(".")[0] for file in os.listdir(folder_path) if file.endswith('.csv')]
address_list = list(set(address_list))
# address_list = address_list[:20]


df_eth = pd.read_csv('/data/research/task2302-uni-annual-analysis/return_rate/ethereum/2_price.csv', sep=',', index_col=[],  header=0)
# block_timestamp,price,total_liquidity,sqrtPriceX96
# 2023-01-01 00:00:00,1195.5839948533555,39974582212968359597,2291340004214720379120476440275511

df_eth['block_timestamp'] = pd.to_datetime(df_eth.iloc[:, 0])

df_eth.set_index('block_timestamp', inplace=True)

hourly_eth_prices = df_eth['price'].resample('H').ohlc()

# 计算每一的小时回报率
hourly_eth_prices['hourly_eth_returns'] = hourly_eth_prices['close'].pct_change()
hourly_eth_prices['hour'] = hourly_eth_prices.index

merged_return_path = '/data/research/task2302-uni-annual-analysis/merged_return_rate/{}/'.format(chain)

def df_capm(df):
    #df["hourly_return_u"] = df["return_rate"]-1
    ri = df['return_rate'].values-1  # 个股收益率
    rm = df['hourly_eth_returns'].values  # 市场收益率
    ri = ri.astype('float64')
    rm = rm.astype('float64')

    X = np.vstack([rm, np.ones(len(rm))]).T
    X = X.astype('float64')
    try:
        beta,alpha= np.linalg.lstsq(X, ri, rcond=None)[0]
        return [alpha*12*24*30,beta]
    except:
        return None

def monthly_capm():
    skip_address = []
    result_df = pd.DataFrame(columns=['address', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct','Nov','Dec'])
    for address_name in tqdm(address_list, desc='Processing Files'):
        try:
            df = pd.read_csv(merged_return_path+address_name+".csv")
            df['hour'] = pd.to_datetime(df.iloc[:, 0])
            merged_df = pd.merge(df, hourly_eth_prices, on='hour', how='inner')
            merged_df = merged_df[(merged_df['hour'] >= '2023-01-01') & (merged_df['hour'] <= '2023-12-31')]
            g = merged_df.groupby(pd.Grouper(key='hour', freq='M'))
            all_12_month_capm = [() for i in range(12)]
            for index,month_df  in g:
                alpha_beta = df_capm(month_df)
                all_12_month_capm[index.month-1] = alpha_beta

            # append all 12 month_capm to result_df as new row
            result_df.loc[len(result_df)]= [address_name] + all_12_month_capm
        except:
            skip_address.append(address_name)
            print(address_name, "error")
            continue

    new_file_name = f'{save_path}/month_address_alpha_beta_{chain}_total.csv'
    result_df.to_csv(new_file_name, index=False)

def calc_yearly_capm():
    skip_address = []
    result_df = pd.DataFrame(columns=['address', 'alpha', 'beta'])
    for address_name in tqdm(address_list, desc='Processing Files'):
        try:
            df = pd.read_csv(merged_return_path + address_name + ".csv")
            df['hour'] = pd.to_datetime(df.iloc[:, 0])
            merged_df = pd.merge(df, hourly_eth_prices, on='hour', how='inner')
            merged_df = merged_df[(merged_df['hour'] >= '2023-01-01') & (merged_df['hour'] <= '2023-12-31')]
            alpha_beta = df_capm(merged_df)
            result_df.loc[len(result_df)] = [address_name] + alpha_beta
        except:
            skip_address.append(address_name)
            print(address_name, "error")
            continue

    new_file_name = f'{save_path}/year_address_alpha_beta_{chain}_total.csv'
    result_df = result_df.sort_values(by='alpha', ascending=False)
    result_df.to_csv(new_file_name, index=False)

def calc_bear_capm():
    # time = ["2023-01-01","2023-03-11","2023-08-17","2023-10-20","2023-12-17"]
    skip_address = []
    result_df = pd.DataFrame(columns=['address', 'alpha', 'beta'])
    for address_name in tqdm(address_list, desc='Processing Files'):
        try:
            df = pd.read_csv(merged_return_path + address_name + ".csv")
            df['hour'] = pd.to_datetime(df.iloc[:, 0])
            merged_df = pd.merge(df, hourly_eth_prices, on='hour', how='inner')
            mmerged_df = merged_df[((merged_df['hour'] >= '2023-01-01') & (merged_df['hour'] <= '2023-03-11')) | ((merged_df['hour'] >= '2023-08-17') & (merged_df['hour'] <= '2023-10-20'))]
            alpha_beta = df_capm(mmerged_df)
            result_df.loc[len(result_df)] = [address_name] + alpha_beta
        except:
            skip_address.append(address_name)
            print(address_name, "error")
            continue

    new_file_name = f'{save_path}/bear_address_alpha_beta_{chain}_total.csv'
    result_df = result_df.sort_values(by='alpha', ascending=False)
    result_df.to_csv(new_file_name, index=False)

def calc_bull_capm():
    # time = ["2023-01-01","2023-03-11","2023-08-17","2023-10-20","2023-12-17"]
    skip_address = []
    result_df = pd.DataFrame(columns=['address', 'alpha', 'beta'])
    for address_name in tqdm(address_list, desc='Processing Files'):
        try:
            df = pd.read_csv(merged_return_path + address_name + ".csv")
            df['hour'] = pd.to_datetime(df.iloc[:, 0])
            merged_df = pd.merge(df, hourly_eth_prices, on='hour', how='inner')
            mmerged_df = merged_df[((merged_df['hour'] >= '2023-03-11 00:00:00') & (merged_df['hour'] <= '2023-08-17 00:00:00')) |  ((merged_df['hour'] >= '2023-10-20 00:00:00') & (merged_df['hour'] <= '2023-12-17 00:00:00'))]  
            alpha_beta = df_capm(mmerged_df)
            result_df.loc[len(result_df)] = [address_name] + alpha_beta
        except:
            skip_address.append(address_name)
            continue
    result_df = result_df.sort_values(by='alpha', ascending=False)
    new_file_name = f'{save_path}/bull_address_alpha_beta_{chain}_total.csv'
    result_df.to_csv(new_file_name, index=False)
calc_yearly_capm()
calc_bear_capm()
calc_bull_capm()