import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# 解决中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        # 先转为字符串
        id_str = str(id_value).strip()
        # 特殊处理负数ID
        if id_str == '-2147483648':
            return id_str
        # 通过数值规范化，去除.0后缀
        return str(int(float(id_str)))
    except:
        # 如果转换失败，返回原始字符串
        return str(id_value).strip()

def analyze_influence_edge_ratio():
    """分析用户影响力与连边数的比值，识别异常用户"""
    # 创建结果目录
    result_dir = 'old/test/test_result'
    ensure_dir(result_dir)
    
    # 1. 加载边数据和流行度数据
    print("正在加载网络数据...")
    edges_df = pd.read_csv('data/domain_networks/merged_network/edges.csv')
    popularity_df = pd.read_csv('data/domain_networks/merged_network/popularity.csv')
    
    # 规范化ID，去除小数点
    edges_df['source'] = edges_df['source'].apply(normalize_id)
    edges_df['target'] = edges_df['target'].apply(normalize_id)
    popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
    
    # 2. 计算每个用户的入度和出度
    print("正在计算连边统计...")
    user_in_degree = edges_df['target'].value_counts().to_dict()
    user_out_degree = edges_df['source'].value_counts().to_dict()
    
    # 3. 计算每个用户的总连边数
    user_total_edges = {}
    for user, count in user_in_degree.items():
        user_total_edges[user] = count
    for user, count in user_out_degree.items():
        user_total_edges[user] = user_total_edges.get(user, 0) + count
    
    # 4. 合并流行度数据
    result_data = []
    for _, row in popularity_df.iterrows():
        user_id = normalize_id(row['user_id'])
        avg_popularity = row['avg_popularity']
        edge_count = user_total_edges.get(user_id, 0)
        
        # 计算比值 (添加小值避免除零)
        influence_edge_ratio = avg_popularity / (edge_count + 1e-10) 
        
        result_data.append({
            'user_id': user_id,
            'avg_popularity': avg_popularity,
            'edge_count': edge_count,
            'influence_edge_ratio': influence_edge_ratio
        })
    
    result_df = pd.DataFrame(result_data)
    
    # 5. 分析比值分布 - 生成散点图
    plt.figure(figsize=(10, 6))
    valid_points = result_df[(result_df['edge_count'] > 0) & (result_df['avg_popularity'] > 0)]
    if not valid_points.empty:
        plt.scatter(valid_points['edge_count'], valid_points['avg_popularity'], 
                   alpha=0.5, color='blue', edgecolor='navy')
        plt.xscale('log')
        plt.yscale('log')
    
    plt.title('影响力 vs 连边数 (对数刻度)', fontsize=14)
    plt.xlabel('连边数', fontsize=12)
    plt.ylabel('平均影响力', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{result_dir}/popularity_vs_edges.png', dpi=300)
    plt.close()
    
    # 6. 检测异常值 (使用IQR方法)
    Q1 = result_df['influence_edge_ratio'].quantile(0.25)
    Q3 = result_df['influence_edge_ratio'].quantile(0.75)
    IQR = Q3 - Q1
    threshold_upper = Q3 + 1.5 * IQR
    
    # 找出异常点并列出
    outliers = result_df[result_df['influence_edge_ratio'] > threshold_upper]
    outliers = outliers.sort_values('influence_edge_ratio', ascending=False)
    
    # 7. 将前10个异常用户的详细信息保存到文本文件
    top10_file_path = f'{result_dir}/top10_abnormal_users.txt'
    with open(top10_file_path, 'w', encoding='utf-8') as f:
        f.write("====== 前10个比值最大的异常用户详细信息 ======\n\n")
        f.write(f"使用IQR方法检测，异常阈值: {threshold_upper:.0f}\n\n")
        
        # 如果有异常用户，则详细展示前10个
        if not outliers.empty:
            top_10 = outliers.head(10)
            for i, (_, row) in enumerate(top_10.iterrows()):
                f.write(f"异常用户 #{i+1}:\n")
                f.write(f"  用户ID: {row['user_id']}\n")
                f.write(f"  影响力/连边数比值: {row['influence_edge_ratio']:.0f}\n")
                f.write(f"  平均影响力: {row['avg_popularity']:.2f}\n")
                f.write(f"  连边数量: {row['edge_count']}\n")
                f.write("\n")
        else:
            f.write("未检测到明显的异常用户\n")
    
    # 8. 在终端输出前10个异常用户信息
    print("\n====== 前10个比值最大的异常用户详细信息 ======")
    print(f"使用IQR方法检测，异常阈值: {threshold_upper:.0f}\n")
    
    if not outliers.empty:
        top_10 = outliers.head(10)
        for i, (_, row) in enumerate(top_10.iterrows()):
            print(f"异常用户 #{i+1}:")
            print(f"  用户ID: {row['user_id']}")
            print(f"  影响力/连边数比值: {row['influence_edge_ratio']:.0f}")
            print(f"  平均影响力: {row['avg_popularity']:.2f}")
            print(f"  连边数量: {row['edge_count']}")
            print()
    else:
        print("未检测到明显的异常用户")
    
    # 9. 计算各百分位数阈值并保存
    percentiles = [95, 97, 99]
    percentile_file = f'{result_dir}/percentile_thresholds.txt'
    
    with open(percentile_file, 'w', encoding='utf-8') as f:
        f.write("====== 各百分位数阈值统计 ======\n\n")
        f.write(f"总用户数: {len(result_df)}\n")
        f.write(f"IQR方法检测的异常用户数: {len(outliers)}\n\n")
        
        for p in percentiles:
            threshold = result_df['influence_edge_ratio'].quantile(p/100)
            count = len(result_df[result_df['influence_edge_ratio'] > threshold])
            f.write(f"{p}%分位数阈值: {threshold:.0f}, 超过此阈值的用户数: {count}\n")
    
    # 10. 打印各百分位数阈值
    print("\n====== 各百分位数阈值统计 ======")
    print(f"总用户数: {len(result_df)}")
    print(f"IQR方法检测的异常用户数: {len(outliers)}")
    
    for p in percentiles:
        threshold = result_df['influence_edge_ratio'].quantile(p/100)
        count = len(result_df[result_df['influence_edge_ratio'] > threshold])
        print(f"{p}%分位数阈值: {threshold:.0f}, 超过此阈值的用户数: {count}")
    
    print(f"\n所有结果已保存到: {result_dir}/ 目录")
    
    # 11. 保存异常用户列表
    outliers.to_csv(f'{result_dir}/abnormal_users.csv', index=False)
    
    return result_df, outliers

if __name__ == "__main__":
    result_df, outliers = analyze_influence_edge_ratio()