import pandas as pd
from scipy.stats import pearsonr
data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/bear_address_alpha_beta_ethereum_total.csv")

filtered_data = data[data['alpha'] > 0]

percentage = len(filtered_data) / len(data) * 100
alpha = data['alpha']
beta = data['beta']

correlation, p_value = pearsonr(alpha, beta)
mean_alpha = alpha.mean()

# 打印结果
print("Alpha的平均值：", mean_alpha)
# 打印结果
print("Bear 皮尔逊相关系数：", correlation)
print("p-value：", p_value)
print(f"Bear Alpha大于零的地址占总地址的百分之{percentage:.2f}%")

data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/bull_address_alpha_beta_ethereum_total.csv")

filtered_data = data[data['alpha'] > 0]

percentage = len(filtered_data) / len(data) * 100
alpha = data['alpha']
beta = data['beta']

correlation, p_value = pearsonr(alpha, beta)
mean_alpha = alpha.mean()

# 打印结果
print("Alpha的平均值：", mean_alpha)
# 打印结果
print("Bull 皮尔逊相关系数：", correlation)
print("p-value：", p_value)
print(f"Bull Alpha大于零的地址占总地址的百分之{percentage:.2f}%")

data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/year_address_alpha_beta_ethereum_total.csv")

filtered_data = data[data['alpha'] > 0]

percentage = len(filtered_data) / len(data) * 100

print(f"Year Alpha大于零的地址占总地址的百分之{percentage:.2f}%")


alpha = data['alpha']
beta = data['beta']

correlation, p_value = pearsonr(alpha, beta)
mean_alpha = alpha.mean()

# 打印结果
print("Alpha的平均值：", mean_alpha)
# 打印结果
print("Year 皮尔逊相关系数：", correlation)
print("p-value：", p_value)