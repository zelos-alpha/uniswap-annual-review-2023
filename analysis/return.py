import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import numpy as np
from datetime import timedelta
folder_path = "/data/research/uni_return_rate_service/9_address_result"
save_path_plot = '/home/zelos/src/UGPCA/result/plot'
save_path_csv = '/home/zelos/src/UGPCA/result/csv'


file_paths = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
data_address_return = pd.DataFrame(columns=['Name','Excess Rerturn'])
address_list = []

for file_path in tqdm(file_paths, desc='read address list'):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    address_list.append(file_name)


for address_name in tqdm(address_list, desc='Processing Files'):
    data_path = os.path.join(folder_path, address_name+'.csv')
    df1 = pd.read_csv(data_path,index_col=["Unnamed: 0"],)
    last_return_rate = (df1.iloc[-1]['cumulate_return_rate']) # 最后一行的收益率
    data_address_return = data_address_return._append({'Name': address_name,'Excess Rerturn': last_return_rate}, ignore_index=True)
new_csv_path = save_path_csv+'excess_return.csv'
data_address_return.to_csv(new_csv_path, index=False)