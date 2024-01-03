import matplotlib.pyplot as plt
import pandas as pd
import math
from tqdm import tqdm

data_volume = pd.read_csv("/home/zelos/src/UGPCA/result/csv/volatility_volume_date.csv")
data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/volatility_date.csv")
data['date'] = pd.to_datetime(data['date'])
data_volume['date'] = pd.to_datetime(data_volume['date'])

fig, ax1 = plt.subplots(figsize=(12,6))

time = ["2023-01-01","2023-03-11","2023-08-17","2023-10-20","2023-12-17"]
for i in range(len(time)-1):
    start_time = time[i]
    end_time = time[i+1]
    filtered_data = data[(data['date'] >= start_time) & (data['date'] <= end_time)]

    if(i%2==0):
        # 熊市
        lin1 = ax1.plot(filtered_data['date'], filtered_data['volatility']*100, color='crimson', label="Vol of Price on Bear Market")
    else:
        lin2 = ax1.plot(filtered_data['date'], filtered_data['volatility']*100, color='yellowgreen', label= "Vol of Price Bull Market")
        
ax1.set_ylabel('Price Volatility(%)')

ax2 = ax1.twinx()

for i in range(len(time)-1):
    start_time = time[i]
    end_time = time[i+1]
    filtered_data_volume = data_volume[(data['date'] >= start_time) & (data_volume['date'] <= end_time)]

    if(i%2==0):
        # 熊市
        bar1=ax2.bar(filtered_data_volume['date'], filtered_data_volume['volatility']*100, color='crimson', label="Vol of Volume on Bear Market",alpha = 0.2)
    else:
        bar2=ax2.bar(filtered_data_volume['date'], filtered_data_volume['volatility']*100, color='yellowgreen', label= "Vol of Volume Bull Market",alpha = 0.2)
        
ax2.set_ylabel('Volume Volatility(%)')
line1 = ax1.lines[0]
line2 = ax1.lines[1] 
lines = [line1]+[line2]+ [bar1] + [bar2]
labels =  [line.get_label() for line in lines]
ax1.legend(lines, labels, loc='upper right')

ax1.set_xlabel('Time')

ax1.set_title('7 Days Rolling Volatility of Price and Volume (measured in day) - Time')

plt.savefig('/home/zelos/src/UGPCA/result/plot/volatility_volume_price_time.png')

plt.clf()


