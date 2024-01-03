import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import numpy as np
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib

folder_path = '/data/research/task2302-uni-annual-analysis/return_rate/ethereum/9_address_result'
position_data_path = '/home/zelos/src/UGPCA/result/csv/01_init/price_result_bull'
pos_cal_path = '/home/zelos/src/UGPCA/result/csv/pos_analysis'
if not os.path.exists(pos_cal_path):
    os.makedirs(pos_cal_path)

file_paths = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
pos_paths = [os.path.join(position_data_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

# Name /Holding Time Per/ Closing Value/ Liquidity Change/  Period Rertun/ Annual Return/ Excess Rerturn/ beta/ alpha
data = pd.DataFrame(columns=['Name', 'Holding Time Per', 'Max Net Value','Annual Return','Excess Rerturn','beta','alpha'])

address_list = []

for file_path in tqdm(pos_paths, desc='read address list'):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    address_list.append(file_name)

outRange = 0
outRange1 = 0
outRange2  = 0
for address_name in tqdm(address_list, desc='Processing Files'):
    data_path = os.path.join(folder_path, address_name+'.csv')
    liq_path = os.path.join(position_data_path, address_name+'.csv')
    df1 = pd.read_csv(data_path,index_col=["Unnamed: 0"],) # ,net_value,return_rate,cumulate_return_rate,positions
    df2 = pd.read_csv(liq_path,dtype=object, header=0) # ,index,id,lower_tick,upper_tick,tx_type,block_number,tx_hash,log_index,blk_time,liquidity,final_amount0,final_amount1,tick_id,price_upper,price_lower
    df1.index = pd.to_datetime(df1.index)

    # select df from 2021-01-01 to 2021-06-21 ,by index timestamp
    # df1 = df1[(df1.index >=  datetime.strptime('2023-01-01','%Y-%m-%d'))& (df1.index <= datetime.strptime('2023-06-30','%Y-%m-%d'))]

    # 判断当前hour是否在做市
    df1 = df1[df1['net_value'] >= 1]
    if df1.empty:
        outRange2+=1
        continue

    df1['hourly_return'] = np.log(df1['return_rate']) 
    max_net_value = df1['net_value'].max()

    # get average hourly log return and std
    hour_mean_return = df1['hourly_return'].mean()
    hour_std = df1['hourly_return'].std()

    # transfer hour_mean_return to yearly mean, hour_std to yearly std
    yearly_return = hour_mean_return * 24 * 365
    yearly_std = hour_std * (24 * 365) ** 0.5

    last_return_rate = (df1.iloc[-1]['cumulate_return_rate']) # 最后一行的收益率
    mean_return_rate = yearly_return
    # filtered_df["annualized_return"] = (1-filtered_df["return_rate"]).apply(lambda x: np.power(1 + x, 365*24) - 1)
    # filtered_df["annualized_return"] = (1-filtered_df["return_rate"])*(365*24)


    # 算持仓时间和平均间隔
    df2 = df2[df2.tx_type != 'COLLECT']
    # 修改数据格式
    df2.loc[:, "blk_time"] = df2.loc[:, "blk_time"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    # print(type(action.loc[:,'block_timestamp'][0]), action.loc[:,'block_timestamp'][0])
    # df2 = df2[df2.blk_time >= datetime.strptime('2022-12-25 00:00:00', "%Y-%m-%d %H:%M:%S")]
    # df2 = df2[df2.blk_time <= datetime.strptime('2023-06-21 00:00:00', "%Y-%m-%d %H:%M:%S")]
    if df2.empty:
        outRange+=1
        continue

    df2.loc[:, "blk_time"] = df2.loc[:, "blk_time"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))

    df2 = df2.sort_values(['blk_time', 'tx_type'], ascending=[True, False])
    df2.index = df2.loc[:, "blk_time"]
    df2.loc[:,'tick_id'] = df2.loc[:,'lower_tick']+df2.loc[:,'upper_tick']

    try:
        df2[['price_lower', 'price_upper','liquidity']] = df2[['price_lower', 'price_upper','liquidity']].astype(float)
    except:
        df2[['lower_tick', 'upper_tick','liquidity']] = df2[['lower_tick', 'upper_tick','liquidity']].astype(float)
        df2[['lower_tick', 'upper_tick']] = df2[['lower_tick', 'upper_tick']].astype('Int64')

    # 删掉burn liquidity=0的数据
    idx = df2[(df2.tx_type == 'BURN')&(df2.liquidity == 0)].index.values
    df2 = df2.drop(idx)

    df2_id = df2.groupby('tick_id')
    
    # 'holding_time','pos_inter_time','pos_inter_price','pos_num'
    pos_data = pd.DataFrame(columns=['id','holding_time','pos_inter_time','pos_inter_price','pos_num','start_time','end_time','up_price','low_price','liquidity'])

    pos_num = 0
    holding_time = timedelta(hours=0)
    # 收集所有的start和end时间对
    formatted_periods = []
    for id, df in df2_id:
        a = 0
        try:
            df_mint = df[df.tx_type == 'MINT']
            start_hour = df_mint['blk_time'][0]
            a = 1
            price_up = df_mint['price_upper'][0]
            price_low = df_mint['price_lower'][0]
        except:
            start_hour = df2.loc[:, 'blk_time'].min() # 被时间截取错过的pos
            
        try:
            df_burn = df[df.tx_type == 'BURN']
            end_hour = df_burn['blk_time'][-1]
            if a == 0:
                price_up = df_burn['price_upper'][-1]
                price_low = df_burn['price_lower'][-1]
        except:
            end_hour = df2.loc[:, 'blk_time'].max()

        # 'holding_time','pos_inter_time','pos_inter_price','pos_num'
        addr = address_name
        start_hour = datetime.strptime(start_hour, "%Y-%m-%d %H:%M")
        end_hour = datetime.strptime(end_hour, "%Y-%m-%d %H:%M")
        _liquidity = df2['liquidity'][0]
        if end_hour>=start_hour:
            pos_inter_time = end_hour-start_hour
        else:
            continue

        formatted_periods.append({"start": start_hour, "end": end_hour})
        
        pos_inter_price = price_up-price_low
        pos_num += 1
        holding_time += pos_inter_time
        new_row = {'id': id, 'holding_time':holding_time, 'pos_inter_time': pos_inter_time, 'pos_inter_price': pos_inter_price, 'pos_num': pos_num,'start_time': start_hour,'end_time': end_hour,'up_price':price_up,'low_price': price_low,'liquidity' : _liquidity}
        pos_data = pos_data._append(new_row, ignore_index=True)

    pos_cal_file = os.path.join(pos_cal_path, address_name + '.csv')
    pos_data.to_csv(pos_cal_file, index=False)

    formatted_periods.sort(key=lambda x: x["start"])
    total_duration = timedelta(0)
    previous_end = formatted_periods[0]["start"]
    for period in formatted_periods:
        if period["start"] > previous_end:
            total_duration += period["end"] - period["start"]
        elif period["end"] > previous_end:
            total_duration += period["end"] - previous_end
        previous_end = max(previous_end, period["end"])
    
    s_time = datetime.strptime('2021-12-21 19:10:00', '%Y-%m-%d %H:%M:%S')
    e_time = datetime.strptime('2023-12-07 23:59:00', '%Y-%m-%d %H:%M:%S')
    total_duration_per = (total_duration/(e_time-s_time))

    # 整合
    holding_time = pos_data.iloc[-1]['holding_time']
    avg_pos_inter_time = pos_data['pos_inter_time'].mean()
    avg_pos_inter_price = pos_data['pos_inter_price'].mean()
    pos_num = pos_data.iloc[-1]['pos_num']

    time_str1 = df.iloc[0, 0]
    time_str2 = df.iloc[-1, 0]
    # 'Name', 'Holding Time Per', 'Max Net Value','Annual Return','Excess Rerturn','beta','alpha'
    data = data._append({'Name': address_name,'Holding Time Per': total_duration_per,'Max Net Value': max_net_value, 'Annual Return': mean_return_rate, 'Excess Rerturn': last_return_rate}, ignore_index=True)

# ['addr', 'max_net_value', 'avg_net_value', 'last_return_rate','mean_return_rate','ann_return_rate_mean','ann_return_rate_std','holding_time','pos_inter_time','pos_inter_price','pos_num']
print(outRange1)
print(outRange2)
print(outRange)
new_csv_path = '/home/zelos/src/UGPCA/result/csv/user_info_total.csv'
data.to_csv(new_csv_path, index=False)