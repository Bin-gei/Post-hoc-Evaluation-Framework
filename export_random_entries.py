import pymysql
import pandas as pd
import time  # 导入时间模块

start_time = time.time()  # 记录开始时间

# 数据库连接参数
conn = pymysql.connect(
    host='localhost',       # 如果是远程数据库，改成 IP 地址
    user='root',
    password='123456',
    database='oqmd',
    charset='utf8mb4'
)

# SQL 查询语句（随机选取二元化合物的样本）
query = """
-- 子查询：找出每个 entry_id 最新的计算和结构记录
WITH latest_calc AS (
    SELECT entry_id, MAX(runtime) AS max_runtime
    FROM calculations
    GROUP BY entry_id
),
latest_structure AS (
    SELECT entry_id, MAX(id) AS max_structure_id
    FROM structures
    GROUP BY entry_id
)

SELECT
    e.id AS entry_id,
    fe.delta_e,
    fe.stability,
    e.natoms,
    e.ntypes,
    e.composition_id,
    c.element_list,
    el1.symbol AS element_1_symbol,
    el1.atomic_radii AS element_1_atomic_radii,
    el1.electronegativity AS element_1_electronegativity,
    el1.mass AS element_1_mass,
    el1.covalent_radii AS element_1_covalent_radii,
    el1.first_ionization_energy AS element_1_first_ionization_energy,
    el2.symbol AS element_2_symbol,
    el2.atomic_radii AS element_2_atomic_radii,
    el2.electronegativity AS element_2_electronegativity,
    el2.mass AS element_2_mass,
    el2.covalent_radii AS element_2_covalent_radii,
    el2.first_ionization_energy AS element_2_first_ionization_energy,
    s.spacegroup_id,
    s.volume_pa,
    s.sxx, s.syy, s.szz, s.sxy, s.syz, s.szx
FROM entries e
JOIN formation_energies fe ON e.id = fe.entry_id
JOIN compositions c ON e.composition_id = c.formula
JOIN compositions_element_set ces1 ON c.formula = ces1.composition_id
JOIN compositions_element_set ces2 ON c.formula = ces2.composition_id AND ces1.element_id < ces2.element_id
JOIN elements el1 ON ces1.element_id = el1.symbol
JOIN elements el2 ON ces2.element_id = el2.symbol
LEFT JOIN structures s ON e.id = s.entry_id
LEFT JOIN latest_structure ls ON s.entry_id = ls.entry_id AND s.id = ls.max_structure_id
LEFT JOIN calculations calc ON e.id = calc.entry_id
LEFT JOIN latest_calc lc ON calc.entry_id = lc.entry_id AND calc.runtime = lc.max_runtime
WHERE
    e.ntypes = 2
    AND fe.delta_e IS NOT NULL
    AND fe.stability IS NOT NULL
    AND ls.entry_id IS NOT NULL
    AND lc.entry_id IS NOT NULL
    -- 排除结构张量全为 0 的情况
    AND NOT (
        s.sxx = 0 AND s.syy = 0 AND s.szz = 0 AND
        s.sxy = 0 AND s.syz = 0 AND s.szx = 0
    )
    -- 排除共价半径为无效值的情况
    AND el1.covalent_radii != -1
    AND el2.covalent_radii != -1
ORDER BY RAND()
LIMIT 10000;

"""

# 执行查询
df = pd.read_sql(query, conn)

# 保存为 CSV 文件
df.to_csv('data/random_binary_entries.csv', index=False)

# 关闭连接
conn.close()

end_time = time.time()  # 记录结束时间
elapsed_time = end_time - start_time  # 计算时间差，单位秒

print(f"已将数据保存为 random_binary_entries.csv，耗时 {elapsed_time:.2f} 秒")

