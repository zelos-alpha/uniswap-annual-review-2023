import matplotlib.pyplot as plt
import pandas as pd
# ETH价格数据（示例数据）
csv_path = "/home/zelos/src/UGPCA/result/csv/Merge_Bear_Bull.csv"

data = pd.read_csv(csv_path)
data['date'] = pd.to_datetime(data.iloc[:, 1])

colors = {"bear": "red", "bull": "green", "neutral": "blue"}

time = ["2023-01-01","2023-03-11","2023-08-17","2023-10-20","2023-12-17"]
market_dict = {
    ("2023-01-01", "2023-03-11"): "bear",
    ("2023-03-11", "2023-08-17"): "bull",
    ("2023-08-17", "2023-10-20"): "bear",
    ("2023-10-24", "2023-12-17"): "bull"
}

fig, ax1 = plt.subplots(figsize=(12,6))


for i in range(len(time)-1):
    start_time = time[i]
    end_time = time[i+1]
    filtered_data = data[(data['date'] >= start_time) & (data['date'] <= end_time)]

    if(i%2==0):
        # 熊市
        ax1.plot(filtered_data['date'], filtered_data['price'], color='crimson')
    else:
        ax1.plot(filtered_data['date'], filtered_data['price'], color='yellowgreen')
        
ax1.plot([], color='green', label="Bull Market")
ax1.plot([], color='red', label= "Bear Market")

ax1.set_ylabel('Price')
path = "/home/zelos/src/UGPCA/result/csv/Merge_Bear_Bull.csv"

_data = pd.read_csv(path)
_data['date'] = pd.to_datetime(_data['date'])
ax2 = ax1.twinx()
colors = _data['Market_Status'].map({'Bear': 'crimson', 'Bull': 'yellowgreen','Neutral': '#1f77b4'}).fillna('yellow')

ax2.scatter(_data['date'], _data['bearBullIndex'],c=colors,s=8,alpha = 0.2)
ax2.set_ylabel('Bear/Bull Market Index')

bear_legend = plt.Line2D([], [], color='red', marker='o', linestyle='None', markersize=5, label='Bear')
bull_legend = plt.Line2D([], [], color='green', marker='o', linestyle='None', markersize=5, label='Bull')
neutral_legend = plt.Line2D([], [], color='#1f77b4', marker='o', linestyle='None', markersize=5, label='Neutral')

ax2.legend(handles=[bear_legend, bull_legend, neutral_legend], loc='best')

ax1.legend(loc='upper left')


ax1.set_xlabel('Time')
ax1.set_title('Classification of Bear Bull market and Bear/Bull Index')

plt.savefig('/home/zelos/src/UGPCA/result/plot/Classification of Bear Bull market and Index.png')

