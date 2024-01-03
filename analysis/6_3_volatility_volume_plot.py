import matplotlib.pyplot as plt
import pandas as pd
import math
from tqdm import tqdm
data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/volatility_volume_date.csv")
data['date'] = pd.to_datetime(data['date'])
plt.figure(figsize=(10, 6))
time = ["2023-01-01","2023-03-11","2023-08-17","2023-10-20","2023-12-17"]
for i in range(len(time)-1):
    start_time = time[i]
    end_time = time[i+1]
    filtered_data = data[(data['date'] >= start_time) & (data['date'] <= end_time)]

    if(i%2==0):
        # 熊市
        plt.plot(filtered_data['date'], filtered_data['volatility'], color='red')
    else:
        plt.plot(filtered_data['date'], filtered_data['volatility'], color='green')
        
plt.plot([], color='green', label="Bull Market")
plt.plot([], color='red', label= "Bear Market")



# plt.plot(data['date'], data['volatility']*100)
plt.xlabel('Time')
plt.ylabel('Volume')
plt.title('7 Days Rolling Volume Volatility (measured in day) - Time')
plt.legend()
plt.savefig('/home/zelos/src/UGPCA/result/plot/volatility_time_volume.png')

plt.clf()