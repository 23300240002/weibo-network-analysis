import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from time import sleep # 调试

from econml.dml import CausalForestDML # 从 econml 库导入因果森林模型
from sklearn.linear_model import LassoCV # 从 sklearn 库导入 LassoCV 模型
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
# RandomForestRegressor是随机森林回归模型，GradientBoostingRegressor是梯度提升回归模型
# 两者都是集成学习方法，通过多个弱学习器的组合来构建一个强学习器

# LassoCV自动选择最优的正则化参数 alpha

# 设置宋体以正常显示中文标签
plt.rcParams['font.sans-serif'] = ['SimSun']
plt.rcParams['axes.unicode_minus'] = False

# 读取 JSONL 文件，构造 DataFrame
def load_jsonl(jsonl_path):
    records = [] # records为列表，每个元素为一个字典
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            records.append(record)
    return pd.DataFrame(records) # 将列表转为 DataFrame

def run_causal_forest_analysis(data, treatment_var, outcome_var, confounders):
    """
    data: DataFrame，包含 treatment、outcome 与 confounders 变量
    treatment_var: 字符串，treatment 变量名
    outcome_var: 字符串，结果变量名
    confounders: list of str，混淆变量名
    返回：因果效应数组，以及平均因果效应
    """

    X = data[confounders].values # 提取混淆变量的值，转为数组
    # 现在X是一个二维数组，每一行代表一个样本，每一列代表一个混淆变量
    T = data[treatment_var].values # 提取 treatment 变量的值，转为数组
    Y = data[outcome_var].values  # 提取 outcome 变量的值，转为一维数组
    # Y = data[outcome_var].values.ravel()
    # 可以考虑 ravel() 将多维数组降为一维（将Y从列向量降为行向量，模型要求？）

    model_y = LassoCV(cv=3, max_iter=10000) # model_y 为 LassoCV 的回归模型
    # cv=3 表示交叉验证的折数，max_iter=10000 表示最大迭代次数
    model_t = LassoCV(cv=3, max_iter=10000)
    # 两个模型都是 LassoCV，因此可以用同一个模型类，只是用于不同的变量
    # LassoCV 模型的功能：将自变量与因变量之间的关系拟合为线性关系，同时自动选择最优的正则化参数 alpha
    # y和t分别代表因果效应的因变量和自变量，接下来将这两个模型传入 CausalForestDML

    # 修改：采用了 GradientBoostingRegressor 和 RandomForestRegressor 作为模型（更加复杂的模型）
    # 是否会过拟合数据？
    est = CausalForestDML(model_y=GradientBoostingRegressor(),
                          model_t=RandomForestRegressor(),
                          n_estimators=100,
                          random_state=123) # CausalForestDML 模型的参数设置
    # n_estimators=100 控制随机森林中树的数目，random_state=123 保证结果可复现

    est.fit(Y, T, X=X) # 拟合模型，得到因果效应
    # 1. 分别利用 model_y 和 model_t 以混淆变量 X 预测 Y 和 T，从而计算残差
    # 实际值与预测值之间的差异称为残差，残差越小，模型越好
    # 2. 用残差代替原始数据，利用随机森林回归捕捉残差间的关系，从而估计局部平均处理效应（CATE）
    # 3. 这种方法能够在控制混淆变量的基础上，得到 T 对 Y 的因果性影响

    te = est.effect(X) # 得到的te是一个一维数组，表示每个样本的因果效应
    # 表示在混淆变量 X 的条件下，T 变化单位对应的 Y 变化（即局部处理效应）
    
    avg_te = np.mean(te) # 平均因果效应，结果为一个数值
    
    return te, avg_te

def plot_effect_distribution(effect, avg_effect, treatment_name):
    plt.figure(figsize=(8, 6)) # 设置画布大小

    plt.hist(effect, bins=30, color='skyblue', edgecolor='black') # 绘制直方图
    # 这里需要用到te而非avg_te，是因为需要查看每个样本的因果效应，而不仅是平均
    # bins=30 表示将数据分为30个区间
    plt.axvline(avg_effect, color='red', linestyle='--', linewidth=2, label=f'平均效应={avg_effect:.4f}')
    # 绘制平均因果效应的垂直线，该虚线可看出平均因果效应大致所处位置

    plt.xlabel(f'{treatment_name} 的因果效应')
    plt.ylabel('样本数量')
    plt.title(f'{treatment_name} 因果效应分布')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # 载入数据：user_df 包含每个用户的邻居网络指标；popularity_df 包含每条微博的流行度和是否转发信息
    user_df = load_jsonl("./results/result.jsonl")
    popularity_df = load_jsonl("./results/popularity.jsonl")
    # 合并数据时以微博信息为主（样本数与 popularity.jsonl 保持一致，1830条微博）
    df = pd.merge(popularity_df, user_df, on="user_id", how="left")
    
    # 检查是否有 'popularity' 字段，以防错误
    if 'popularity' not in df.columns:
        print("请确保DataFrame中有 'popularity' 字段！")
        exit(1)
    
    # 将 'is_retweet' 转为数值型变量，0 表示原创，1 表示转发
    df['is_retweet_num'] = df['is_retweet'].astype(int)
    
    # 从用户的 ego_network_info 中提取解释变量指标
    candidates = ['clustering_coefficient', 'density', 'average_nearest_neighbor_degree', 'ego_betweenness']
    for col in candidates:
        df[col] = df['ego_network_info'].apply(lambda x: x.get(col, 0) if isinstance(x, dict) else 0)

    # 混淆变量：提取全局网络指标、是否转发、粉丝数，以及新增的整体发帖数
    df['global_degree'] = df['network_info'].apply(lambda x: x.get('degree', 0) if isinstance(x, dict) else 0)
    df['fans_count'] = df['personal_info'].apply(lambda x: int(x.get('fans_count', 0)) if isinstance(x, dict) else 0)
    df['total_posts'] = df['personal_info'].apply(lambda x: int(x.get('total_posts', 0)) if isinstance(x, dict) else 0)

    confounders = ['global_degree', 'is_retweet_num', 'fans_count', 'total_posts']

    outcome_var = 'popularity'

    # 针对每个 treatment 变量进行因果森林分析
    for treat in candidates:
        print(f"开始对 treatment 变量 {treat} 进行因果森林分析...")
        te, avg_te = run_causal_forest_analysis(df, treat, outcome_var, confounders)
        print(f"{treat} 的平均因果效应：{avg_te:.4f}")
        plot_effect_distribution(te, avg_te, treat)