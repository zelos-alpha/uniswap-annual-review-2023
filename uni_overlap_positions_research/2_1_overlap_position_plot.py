import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Create folder to save the image (if it doesn't exist)
save_folder = '/home/zelos/src/uni_pos_research/result/plot/polygon/'
os.makedirs(save_folder, exist_ok=True)

# Get the overlap position percentage data for users
total_market_df = pd.read_csv('/home/zelos/src/uni_pos_research/result/csv/polygon/overlap_periods.csv')

#计算一下重合时间占比为0的占用户总数的百分之几
count_satisfying = total_market_df[total_market_df['persentage %'] >= 100].shape[0]
print(count_satisfying)
total_rows = total_market_df.shape[0]
print(total_rows)
percentage = (count_satisfying / total_rows) * 100
print("persentage %>=100的行数占据总行数的百分之{:.2f}".format(percentage))


total_market_df = total_market_df[(total_market_df['persentage %'] >= 0)&(total_market_df['persentage %'] <=100)]

#计算一下重合时间占比为0的占用户总数的百分之几
count_satisfying = total_market_df[total_market_df['persentage %'] == 0].shape[0]
total_rows = total_market_df.shape[0]
print(total_rows)
percentage = (count_satisfying / total_rows) * 100
print("persentage %=0的行数占据总行数的百分之{:.2f}".format(percentage))



# Plot frequency graph
plt.figure(figsize=(10, 6))
#--------------------画图的时候不分类不染色-----------------------------
# 计算相对频率
# n, bins, patches = plt.hist(total_market_df['persentage %'], bins=10, color='skyblue', edgecolor='midnightblue', alpha=0.7)

# # 计算相对频率
# density = n / sum(n)

# # 在每个柱子上方添加经过归一化的数值标签
# for i in range(len(n)):
#     if n[i] > 0:
#         plt.text(bins[i] + (bins[i+1] - bins[i])/2, n[i], f'{density[i]:.3f}', ha='center', va='bottom')
#--------------------画图的时候给分类的染色-----------------------------
# # Define the ranges and colors
ranges = [(0, 1), (1, 2), (2, 3), (3, 4),(4, 5), (5, 6), (6, 7), (7, 8), (8, 9),(9, 10),(10, 20), (20, 30), (30, 40), (40, 50), (50, 60),(60, 70), (70, 80), (80, 90), (90, 100)]

# Plot the histogram with colored bars based on specified ranges
for start, end in ranges:
    if start <=9:
        plt.hist(total_market_df['persentage %'], bins=range(start, end+1, 1), color='skyblue', edgecolor='midnightblue', alpha=0.7, label=f'{start}-{end}%')
    else:
        plt.hist(total_market_df['persentage %'], bins=range(start, end+1, 5), color='skyblue', edgecolor='midnightblue', alpha=0.7, label=f'{start}-{end}%')

# Set labels and title
plt.xlabel('Overlap Position Percentage (%)')
plt.ylabel('Address Count (LOG)')
plt.title('Density of Overlap Position Percentage Distribution on Polygon')

# Set y-axis scale to logarithmic
plt.yscale('log')

# Filter out negative values on x-axis
# plt.xlim(left=0)

# Show grid
# plt.grid(True)

# Save the plot as an image
plt.savefig(os.path.join(save_folder, 'overlap_histogram_log_small10.png'))
