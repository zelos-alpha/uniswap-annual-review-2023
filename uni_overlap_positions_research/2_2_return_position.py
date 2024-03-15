import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

save_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/'
user_overlap_type_return = []

data_folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/polygon/9_address_result/'
data_folder_root = '/data/research/task2302-uni-annual-analysis/return_rate/polygon'

overlap_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/overlap_periods.csv' 
save_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/'

overlap_data = pd.read_csv('/home/zelos/src/uni_pos_research/result/csv/polygon/overlap_periods.csv')

total_rows = overlap_data.shape[0]  # 获取总行数

for i in tqdm(range(total_rows), desc='Processing Files1'):
    row = overlap_data.iloc[i]  # 根据索引获取行数据
    address = row['address']
    percentage = row['persentage %']
    return_file_path = os.path.join(data_folder_path, f'{address}.csv')
    if os.path.exists(return_file_path):
        df = pd.read_csv(return_file_path)
        return_list = df['return_rate']
        return_variance = return_list.var()
        # 获取'cumulate_return_rate'列的最后一行作为返回率(return)的值
        return_value = df['cumulate_return_rate'].iloc[-1]
        max_net_value = df['net_value'].max()
    else:
        return_value = None
        max_net_value = None
        print("cannot find file")
    return_address = {'address': address, 'percentage': percentage,'return': return_value,'max_net_value' : max_net_value,'return_variance':return_variance}
    user_overlap_type_return.append(return_address)
_df = pd.DataFrame(user_overlap_type_return)
_df.to_csv(os.path.join(save_path, 'position_overlap_return.csv'), index=False)
