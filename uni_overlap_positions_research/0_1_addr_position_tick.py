from datetime import datetime
import pandas as pd
import numpy as np
import config
from tqdm import tqdm
import os

### Run this file before "1_user_information.py"

# There are some abnormalities in the data of some addresses, such as holding positions for too long.
# Abnormal positions provide very low liquidity, so remove individual abnormal positions for clear images.
# Manually remove the exception when necessary, see the comments in the code.
data_folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/polygon/9_address_result'
data_folder_root = '/data/research/task2302-uni-annual-analysis/return_rate/polygon'

folder_path = '/home/zelos/src/uni_pos_research/result/csv/polygon' 
save_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/01_init'

if not os.path.exists(save_path):
    os.makedirs(save_path)
for root, dirs, files in os.walk(save_path):
    for file_name in files:
        file_path = os.path.join(root, file_name)
        os.remove(file_path)

pos_addr_path = os.path.join(data_folder_root,'7_position_address.csv')
pos_liq_path = os.path.join(data_folder_root,'5_position_liquidity.csv') 

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
# print(eth)
df_pos_addr = pd.read_csv(pos_addr_path,low_memory=False) #position,address,day
df_pos_liq = pd.read_csv(pos_liq_path,low_memory=False) #id,lower_tick,upper_tick,tx_type,block_number,tx_hash,log_index,blk_time,liquidity,final_amount0,final_amount1

address_list = []

for filename in os.listdir(data_folder_path):
    if filename.endswith('.csv'):
        file_name = os.path.splitext(filename)[0]
        address_list.append(file_name)

# info = pd.read_csv(os.path.join(folder_path, 'check_position_certain.csv'), sep=',', index_col=[], dtype=object, header=0)# [address,alpha,beta]
# info.index = info['address']

for name in tqdm(address_list, desc='Processing Files1'):
    # print('name: '+name)
    path_result = os.path.join(save_path, name + '.csv')
    position_list = df_pos_addr[df_pos_addr['address'] == name]['position']
    df_addr_liq = df_pos_liq[df_pos_liq['id'].isin(position_list)]
    df_addr_liq.to_csv(path_result, index=False)

# print(info.index)
# for name in ['a8']:

for name in tqdm(address_list, desc='Processing Files2'):

    path_data = os.path.join(save_path, name + '.csv')
    save_path_price = os.path.join(save_path,"price_result")
    if not os.path.exists(save_path_price):
        os.makedirs(save_path_price)
    path_result = os.path.join(save_path+"/price_result", name + '.csv')
    
    # [id,lower_tick,upper_tick,tx_type,block_number,tx_hash,log_index,blk_time,liquidity,final_amount0,final_amount1]
    # read and reformat the tick data
    action = pd.read_csv(path_data, sep=',', dtype=object, header=0)
    num_index = list(range(1, len(action) + 1))
    action = action.set_index(pd.Index(num_index))

    action = action[action.tx_type != 'COLLECT']
    action.loc[:, "blk_time"] = action.loc[:, "blk_time"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    action['tick_id'] = action['lower_tick']+ '-' + action['upper_tick']
    action[['final_amount0', 'final_amount1',  "lower_tick", "upper_tick",'liquidity']] = action[['final_amount0', 'final_amount1',  "lower_tick", "upper_tick",'liquidity']].astype(float)
    action[["lower_tick", "upper_tick"]] = action[["lower_tick", "upper_tick"]].astype('Int64')

    idx = action[(action.tx_type == 'BURN')&(action.liquidity == 0)].index.values
    action = action.drop(idx)

    # action.loc[action['tx_type'].str.contains('BURN'), 'liquidity'] = action.loc[action['tx_type'].str.contains('BURN'), 'liquidity'] * (-1)

    action = action.sort_values(['blk_time','tx_type'], ascending=[True,False])
    action = action.reset_index()
    action_id = action.groupby('tick_id')

    idx = np.array([])
    for id, df in action_id:
        df['cum_liq'] = df['liquidity'].cumsum()
        # print(id)
        # print(df.loc[:,['block_timestamp', 'tx_type', 'liquidity', 'cum_liq']])

        mint = np.array(df[df.tx_type == 'MINT'].index.values)

        if len(mint) == 0: # 0 mint: error
            print(name, id, 'no mint')
            continue
        #
        # if mint[0] == 2676:
        #     continue

        # try:
        #     df.loc[1329, 'cum_liq'] = 0
        #     burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <= 200)].index.values)
        # except:
        #     burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <=200)].index.values)

        burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <= 200)].index.values)
        burn0 = burn0[burn0>mint[0]]
        idx = np.append(idx, burn0)


        if len(burn0) == 0: # 0 burn
            idx = np.append(idx, mint[0])
        else:
            count = 0
            for i in burn0:
                try:
                    idx = np.append(idx, mint[0])
                    action.loc[[mint[0], i], 'tick_id'] = action.loc[[mint[0], i], 'tick_id'] +\
                                                                '-' + str(count)
                    mint = mint[mint>i]
                    count += 1
                except IndexError:
                    print("IndexError occurred. Skipping this iteration.")
                    continue

            if mint.size > 0:
                idx = np.append(idx, mint[0])
                action.loc[mint[0], 'tick_id'] = action.loc[mint[0], 'tick_id'] + \
                                                             '-' + str(count)



    action = action.loc[idx,:]
    action.loc[:, 'price_upper'] = action.loc[:, 'lower_tick'].map(config.fun)
    action.loc[:, 'price_lower'] = action.loc[:, 'upper_tick'].map(config.fun)
    action.to_csv(path_result)