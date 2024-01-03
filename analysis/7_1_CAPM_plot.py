import matplotlib.pyplot as plt
import pandas as pd
import math
from tqdm import tqdm
data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/year_address_alpha_beta_ethereum_total.csv")
bear_data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/bear_address_alpha_beta_ethereum_total.csv")
bull_data = pd.read_csv("/home/zelos/src/UGPCA/result/csv/alpha/bull_address_alpha_beta_ethereum_total.csv")

plt.figure(figsize=(10, 6))

sorted_data = data.sort_values(by=['alpha', 'beta'])
sorted_data = sorted_data.iloc[1:-1]

bear_sorted_data = bear_data.sort_values(by=['beta', 'alpha'])
bear_sorted_data = bear_sorted_data.iloc[1:-1]

bull_sorted_data = bull_data.sort_values(by=['beta', 'alpha'])
bull_sorted_data = bull_sorted_data.iloc[1:-1]



plt.scatter(sorted_data['beta'], sorted_data['alpha'], s=10,alpha=0.5,label = "Time of 2023 Whole Year")

plt.scatter(bear_sorted_data['beta'], bear_sorted_data['alpha'], s=10,alpha=0.5,color = "crimson",label = "Time of Bear Market")

plt.scatter(bull_sorted_data['beta'], bull_sorted_data['alpha'], s=10,alpha=0.5,color = "yellowgreen",label = "Time of Bull Market")
plt.axhline(y=0, color='blue', linestyle='--')
plt.xlabel('beta')
plt.ylabel('alpha')
plt.title("Scatter of User's Capm")
plt.ylim(-5, 10)
plt.xlim( -0.1,None)
plt.legend()
plt.savefig('/home/zelos/src/UGPCA/result/plot/scatter of capm ylim.png')