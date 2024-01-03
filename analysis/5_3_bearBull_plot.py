import matplotlib.pyplot as plt
import pandas as pd

path = "/home/zelos/src/UGPCA/result/csv/Merge_Bear_Bull.csv"

data = pd.read_csv(path)
data['date'] = pd.to_datetime(data['date'])

colors = data['Market_Status'].map({'Bear': 'red', 'Bull': 'green','Neutral': 'yellow'}).fillna('yellow')

# 绘制散点图
plt.figure(figsize=(12, 6))

plt.scatter(data['date'], data['price'],c=colors)
plt.xlabel('Time')
plt.ylabel('price')
plt.title('bear bull price- Time')
plt.savefig('/home/zelos/src/UGPCA/result/plot/bear bull price base on index.png')