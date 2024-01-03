# 画TVL的图
# /home/zelos/src/UGPCA/result/csv/uniswap-v3_1.csv
# Date,Timestamp,boba,rsk,moonbeam,era,optimism,celo,base,bsc,polygon,arbitrum,ethereum,Total,avalanche
# 01/01/2023,1672531200,,,,,40188744.01,1106538.254,,,94788372.97,75144311.15,2203049140,2414277106,

import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv('/home/zelos/src/UGPCA/result/csv/uniswap-v3_1.csv')
data['Date'] = pd.to_datetime(data['Date'],format = "%d/%m/%Y")
data.set_index('Date', inplace=True)

fig, ax = plt.subplots(figsize = (12,6))

for column in data.columns:
    values = data[column].dropna()
    ax.plot(values.index,values, label=column)
ax.xaxis_date()

# 添加图例
ax.legend(loc = "center left")
fig.autofmt_xdate()
ax.set_title('TVL on Different Chains')

plt.savefig('/home/zelos/src/UGPCA/result/plot/TVL Real Data.png')