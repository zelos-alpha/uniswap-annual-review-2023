import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

save_path = '/home/zelos/src/uni_pos_research/result/csv/'
user_overlap_type_return = []

data_folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/ethereum/9_address_result/'
data_folder_root = '/data/research/task2302-uni-annual-analysis/return_rate/ethereum'

overlap_path = '/home/zelos/src/uni_pos_research/result/csv/overlap_periods.csv' 
save_path = '/home/zelos/src/uni_pos_research/result/csv/'

overlap_data = pd.read_csv('/home/zelos/src/uni_pos_research/result/csv/overlap_periods.csv')
time_period_data = pd.read_csv('/home/zelos/src/uni_pos_research/result/csv/market_periods.csv')

total_rows = overlap_data.shape[0]  # 获取总行数

for i in tqdm(range(total_rows), desc='Processing Files1'):
    row = overlap_data.iloc[i]  # 根据索引获取行数据
    address = row['address']
    time_period_rows = time_period_data[time_period_data['address']==address]
    percentage = row['persentage %']
    overlap_num = row['overlap_num']
    return_file_path = os.path.join(data_folder_path, f'{address}.csv')
    if os.path.exists(return_file_path):
        df = pd.read_csv(return_file_path)
        returns_list=[]
        # 找到lp存在的时候的return
        for _index, _row in time_period_rows.iterrows():
            mint_timestamp = datetime.strptime(_row['mint_timestamp'], "%Y-%m-%d %H:%M:%S")
            burn_timestamp = datetime.strptime(_row['burn_timestamp'], "%Y-%m-%d %H:%M:%S")
            time_list = pd.to_datetime(df.iloc[:, 0])
            for index, row_ in df.iterrows():
                if time_list[index]>=mint_timestamp and time_list[index]<=burn_timestamp:
                    returns_list.append(row_['return_rate'])
        # 获取'cumulate_return_rate'列的最后一行作为返回率(return)的值
        returns_series = pd.Series(returns_list)
        return_variance = returns_series.var()
        no_time_return =df['return_rate']
        no_time_return_series = pd.Series(no_time_return)
        print("the previous return_var is: ",no_time_return_series.var())
        print("now the return_var is: ",return_variance)
        return_value = df['cumulate_return_rate'].iloc[-1]
        max_net_value = df['net_value'].max()
    else:
        return_value = None
        max_net_value = None
        print("cannot find file")
    return_address = {'address': address, 'percentage': percentage,'return': return_value,'max_net_value' : max_net_value,'return_variance':return_variance,'overlap_num': overlap_num}
    user_overlap_type_return.append(return_address)
_df = pd.DataFrame(user_overlap_type_return)
_df.to_csv(os.path.join(save_path, 'position_overlap_return.csv'), index=False)

average_return_zero_overlap = _df[_df['percentage'] == 0]['return'].mean()
average_return_bigger_zero_overlap = _df[_df['percentage'] > 0]['return'].mean()

print("不重合position的LP回报均值为： ",average_return_zero_overlap)