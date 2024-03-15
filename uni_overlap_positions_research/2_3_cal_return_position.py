import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
from scipy. stats import f_oneway

_df = pd.read_csv('/home/zelos/src/uni_pos_research/result/csv/polygon/position_overlap_return.csv')
save_folder = '/home/zelos/src/uni_pos_research/result/plot/polygon/'
os.makedirs(save_folder, exist_ok=True)
_df = _df[(_df['percentage'] >= 0)&(_df['percentage'] <= 100)]

group_pt=[]
for i in range(1, 100):
    group_return_1 =  _df[_df['percentage'] == 0]['return']
    group_return_2 =  _df[(_df['percentage'] > 0) & (_df['percentage'] <= i)]['return']
    group_return_3 =  _df[(_df['percentage'] > i) & (_df['percentage'] <=100)]['return']

    # 进行单因素方差分析
    f_statistic, p_value = f_oneway(group_return_1, group_return_2, group_return_3)

    # 输出结果
    group_oneway = {'group_pt': i, 'F_Statistic': f_statistic,'P_value': p_value}
    group_pt.append(group_oneway)
max_f_statistic_entry = max(group_pt, key=lambda x: x['F_Statistic'])
max_group_pt = max_f_statistic_entry['group_pt']
corresponding_p_value = max_f_statistic_entry['P_value']
print("The maximum F_Statistic value:", max_f_statistic_entry['F_Statistic'])
print("Corresponding group_pt value:", max_group_pt)
print("Corresponding P_value:", corresponding_p_value)
percentages = [entry['group_pt'] for entry in group_pt]
f_statistics = [entry['F_Statistic'] for entry in group_pt]
p_values = [entry['P_value'] for entry in group_pt]

average_return_group1 = _df[_df['percentage'] == 0]['return'].mean()
average_return_group2 = _df[(_df['percentage'] > 0) ]['return'].mean()

average_group1 = _df[_df['percentage'] == 0]['return']
average_group2 = _df[(_df['percentage'] > 0) ]['return']

f_statistic, p_value = f_oneway(average_group1, average_group2)
print('F_Statistic: ', f_statistic,'P_value: ', p_value)
print("不重合position的LP回报均值为： ",average_return_group1)
print("重合position的LP回报均值group2为： ",average_return_group2)

# 创建3D图
fig = plt.figure(figsize=(15,10))
ax = fig.add_subplot(111, projection='3d')

# 绘制数据点
ax.scatter(percentages, f_statistics, p_values,s=5)

# 设置坐标轴标签
ax.set_xlabel('Percentage')
ax.set_ylabel('F Statistic')
ax.set_zlabel('P Value')

plt.title('Single Factor Analysis of Variance: Comparison of Different Overlapping Position Groups')

plt.savefig(os.path.join(save_folder, '3D_oneway_analysis.png'))
plt.clf() 

plt.figure(figsize=(12, 6))

# # 根据条件绘制散点图
# plt.scatter(_df[_df['percentage'] == 0]['max_net_value'], _df[_df['percentage'] == 0]['return'], color='dodgerblue', label='overlap time=0%',s=10,alpha=0.5)
# plt.scatter(_df[(_df['percentage'] > 0) & (_df['percentage'] < 50)]['max_net_value'], _df[(_df['percentage'] > 0) & (_df['percentage'] < 50)]['return'], color='hotpink', label='0%<overlap time<50%',s=10,alpha=0.5)
# plt.scatter(_df[(_df['percentage'] >= 50) & (_df['percentage'] < 100)]['max_net_value'], _df[(_df['percentage'] >= 50) & (_df['percentage'] < 100)]['return'], color='crimson', label='50%<=overlap time<100%',s=10,alpha=0.5)

_df = _df.sort_values(by='percentage')
scatter = plt.scatter(_df['max_net_value'], _df['return'], c=_df['percentage'], cmap='viridis_r',alpha = 0.7, s=10)
plt.colorbar(scatter, label='overlap position time percentage')

# 添加标签和标题
plt.xlabel('max_net_value (log)')
plt.ylabel('return (log)')
plt.xscale('log')
plt.yscale('log')
plt.title('Return and Max Net Value for Different Overlap Position Time on Polygon')

# 显示图形
plt.show()
plt.savefig(os.path.join(save_folder, 'net_value_return.png'))

plt.clf() 

plt.figure(figsize=(12, 6))
# zero_variance_indexes = _df[_df['return_variance'] == 0].index
# _df = _df.drop(index=zero_variance_indexes)

_df = _df.sort_values(by='percentage')
scatter = plt.scatter(_df['return_variance'], _df['return'], c=_df['percentage'], cmap='viridis_r',alpha = 0.7, s=10)
plt.colorbar(scatter, label='overlap position time percentage')



# 添加标签和标题
plt.xlabel('variance of return (log)')
plt.ylabel('return (log)')
plt.xscale('log')
plt.yscale('log')
plt.xlim(left=10**-13)
plt.title('Return and Return Variance for Different Overlap Position Time on Polygon')

# 显示图形
plt.show()
plt.savefig(os.path.join(save_folder, 'return_var_return.png'))