import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

folder_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/01_init/price_result'
save_path = '/home/zelos/src/uni_pos_research/result/csv/polygon/'
user_market_periods = []

# Read and process transaction data
for filename in tqdm(os.listdir(folder_path), desc='Processing Files1'):
    if filename.endswith('.csv'):
        address_name = os.path.splitext(filename)[0]
        transactions = pd.read_csv(os.path.join(folder_path, filename))
        grouped_transactions = transactions.groupby('id')

        for tx_hash, user_transactions in grouped_transactions:
            mint_timestamp = None
            burn_timestamp = None
            for index, transaction in user_transactions.iterrows():
                if transaction['tx_type'] == 'MINT':
                    mint_timestamp = datetime.strptime(transaction['blk_time'], "%Y-%m-%d %H:%M:%S")
                elif transaction['tx_type'] == 'BURN':
                    burn_timestamp = datetime.strptime(transaction['blk_time'], "%Y-%m-%d %H:%M:%S")
            
            # If mint_timestamp is available but burn_timestamp is not, set a default burn_timestamp
            if mint_timestamp and not burn_timestamp:
                burn_timestamp = datetime.strptime("2023-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
                
            # Append market period details to the list
            if mint_timestamp and burn_timestamp:
                market_period = {'address': address_name, 'mint_timestamp': mint_timestamp, 'burn_timestamp': burn_timestamp}
                user_market_periods.append(market_period)

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(user_market_periods)
df.to_csv(os.path.join(save_path, 'market_periods.csv'), index=False)

# Calculate total market durations
total_market_durations = []

for address, user_transactions in tqdm(df.groupby('address'), desc='Processing Files2'):
    sorted_periods = user_transactions.sort_values(by='mint_timestamp')
    total_duration = timedelta(0)  
    previous_end = None
    overlap_periods = {}
    for index, row in sorted_periods.iterrows():
        mint_timestamp = row['mint_timestamp']
        burn_timestamp = row['burn_timestamp']
        if previous_end is None or mint_timestamp >= previous_end:
            total_duration += burn_timestamp - mint_timestamp
            previous_end = burn_timestamp
        elif mint_timestamp < previous_end and burn_timestamp >= previous_end:
            overlap_start = mint_timestamp
            overlap_end = previous_end
            overlap_periods[overlap_start] = overlap_end
            total_duration += burn_timestamp - previous_end
            previous_end = burn_timestamp
        elif previous_end >= burn_timestamp:
            overlap_start = mint_timestamp
            overlap_end = burn_timestamp
            overlap_periods[overlap_start] = overlap_end
    # 计算总体的overlap的时间
    sorted_overlap_periods = dict(sorted(overlap_periods.items(), key=lambda item: item[0])) # 先按照start time的顺序排列一下
    total_overlap_duration = timedelta(0)  
    previous_overlap_end = None
    for start_time, end_time in sorted_overlap_periods.items():
        if previous_overlap_end is None or start_time >= previous_overlap_end:
            total_overlap_duration += end_time - start_time
            previous_overlap_end = end_time
        elif start_time < previous_overlap_end and end_time >= previous_overlap_end:
            total_overlap_duration += end_time - previous_overlap_end
            previous_overlap_end = end_time
    if total_duration > timedelta(0):
        persentage = (total_overlap_duration/total_duration)*100
        total_market_durations.append((address, total_duration, total_overlap_duration, persentage))
    else:
        print("total time is zero!")

# Convert list of durations to DataFrame and save to CSV
total_market_df = pd.DataFrame(total_market_durations, columns=['address', 'total_market_duration','total_overlap_duration','persentage %'])
total_market_df.to_csv(os.path.join(save_path, 'overlap_periods.csv'), index=False)


# # Calculate overlap periods
# overlap_periods = []
# for address, user_transactions in tqdm(df.groupby('address'), desc='Processing Files3'):
#     total_market_duration = total_market_df.loc[total_market_df['address'] == address, 'total_market_duration'].iloc[0]
#     market_periods = list(zip(user_transactions['mint_timestamp'], user_transactions['burn_timestamp']))
#     all_overlaps_duration = timedelta(0)
    
#     # Calculate total overlap duration
#     for i in range(len(market_periods)):
#         for j in range(i+1, len(market_periods)):
#             if market_periods[i][1] >= market_periods[j][0] and market_periods[j][1] >= market_periods[i][0]:
#                 start_overlap = max(market_periods[i][0], market_periods[j][0])
#                 end_overlap = min(market_periods[i][1], market_periods[j][1])
#                 overlap_duration = end_overlap - start_overlap
#                 all_overlaps_duration += overlap_duration
 
#     # Calculate overlap percentage
#     if total_market_duration.total_seconds() == 0:
#         overlap_percentage = 0
#     else:
#         overlap_percentage = (all_overlaps_duration.total_seconds() / total_market_duration.total_seconds()) * 100
    
#     overlap_periods.append({
#         'address': address,
#         'total_market_duration': total_market_duration.total_seconds(),
#         'overlap_duration': all_overlaps_duration.total_seconds(),
#         'overlap_percentage': overlap_percentage
#     })

# # Convert list of dictionaries to DataFrame and save as CSV
# overlap_df = pd.DataFrame(overlap_periods)
# overlap_df.to_csv(os.path.join(save_path, 'overlap_periods.csv'), index=False)