import pandas as pd

# 读取原始数据
df = pd.read_csv("data/random_binary_entries.csv")

# 删除无用列（不会用于建模）
cols_to_drop = [
    "entry_id", "composition_id", "element_list",
    "element_1_symbol", "element_2_symbol"
]
df = df.drop(columns=cols_to_drop)

# 删除含缺失值的行（如有）
df = df.dropna().reset_index(drop=True)

# 保存预处理后的数据
df.to_csv("data/processed_binary_compounds.csv", index=False)

print(f"数据处理完成，共 {df.shape[0]} 条样本，{df.shape[1]} 个特征。已保存为 processed_binary_compounds.csv")
