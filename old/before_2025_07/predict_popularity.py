import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
import shap
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# 设置宋体以正常显示中文标签
plt.rcParams['font.sans-serif'] = ['SimSun']
plt.rcParams['axes.unicode_minus'] = False

# 加载 JSONL 文件
def load_jsonl(jsonl_path):
    records = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            records.append(record)
    return pd.DataFrame(records)

def main():
    # 载入用户与流行度数据
    user_df = load_jsonl("./results/result.jsonl")
    popularity_df = load_jsonl("./results/popularity.jsonl")
    
    # 合并数据（以 popularity_df 为主）
    df = pd.merge(popularity_df, user_df, on="user_id", how="left")
    
    # 将 is_retweet 转为数值型变量（0：原创, 1：转发）
    df['is_retweet_num'] = df['is_retweet'].astype(int)
    
    # 提取解释变量（候选特征）：来自 ego_network_info
    candidate_features = ['clustering_coefficient', 'density', 'average_nearest_neighbor_degree', 'ego_betweenness']
    for col in candidate_features:
        df[col] = df['ego_network_info'].apply(lambda x: x.get(col, 0) if isinstance(x, dict) else 0)
    
    # 提取混淆变量：global_degree, is_retweet_num, fans_count, total_posts
    df['global_degree'] = df['network_info'].apply(lambda x: x.get('degree', 0) if isinstance(x, dict) else 0)
    df['fans_count'] = df['personal_info'].apply(lambda x: int(x.get('fans_count', 0)) if isinstance(x, dict) else 0)
    df['total_posts'] = df['personal_info'].apply(lambda x: int(x.get('total_posts', 0)) if isinstance(x, dict) else 0)
    
    confounders = ['global_degree', 'is_retweet_num', 'fans_count', 'total_posts']
    
    # 构建模型输入：所有特征一起使用
    features = candidate_features + confounders
    X = df[features].values
    y = df['popularity'].values
    
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 使用 RandomForestRegressor 拟合流行度
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 在测试集上评估模型
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f"测试集均方误差 (MSE): {mse:.4f}")
    print(f"测试集 R²: {r2:.4f}")
    
    # 使用 SHAP 分析预测结果
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)
    
    # 绘制 SHAP summary plot（？）
    shap.summary_plot(shap_values, X_test, feature_names=features)
    
    # 新增：绘制真实值与预测值的散点图
    plt.figure(figsize=(8,6))
    plt.scatter(y_test, y_pred, alpha=0.6, color='blue')
    plt.plot([min(y_test), max(y_test)], [min(y_test), max(y_test)], color='red', linestyle='--')
    plt.xlabel("实际流行度")
    plt.ylabel("预测流行度")
    plt.title("实际值 vs 预测值")
    plt.grid(True)
    plt.show()
    
    # 新增: 输出部分样本的实际与预测值对比
    comparison = pd.DataFrame({"实际流行度": y_test, "预测流行度": y_pred})
    print("部分样本对比：")
    print(comparison.head(10))
    
if __name__ == "__main__":
    main()