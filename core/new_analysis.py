import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import easygraph.classes as eg
import easygraph.functions as eg_f
from scipy import linalg
import networkx as nx
from datetime import datetime

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimSun']
plt.rcParams['axes.unicode_minus'] = False

def calculate_spectral_radius(G):
    """计算图的谱半径（最大特征值的绝对值）"""
    try:
        # 使用EasyGraph获取邻接矩阵
        adj_matrix = eg_f.to_numpy_array(G)
        # 计算特征值
        eigenvalues = linalg.eigvals(adj_matrix)
        # 返回最大特征值的绝对值
        return float(np.max(np.abs(eigenvalues)))
    except Exception as e:
        print(f"计算谱半径出错: {e}")
        try:
            # 使用NetworkX作为备选
            nx_G = nx.Graph()
            for u, v in G.edges:
                nx_G.add_edge(u, v)
            adj_matrix = nx.to_numpy_array(nx_G)
            eigenvalues = linalg.eigvals(adj_matrix)
            return float(np.max(np.abs(eigenvalues)))
        except:
            return 0.0

def calculate_modularity(G):
    """计算图的模块度"""
    try:
        # 先使用Louvain算法获取社区划分，再计算模块度
        # Todo：确定EasyGraph是否包含如下函数算法
        communities = eg_f.louvain_communities(G)
        return eg_f.modularity(G, communities)
    except Exception as e:
        print(f"计算模块度出错: {e}")
        try:
            # 使用NetworkX作为备选
            nx_G = nx.Graph()
            for u, v in G.edges:
                nx_G.add_edge(u, v)
            communities = nx.community.louvain_communities(nx_G)
            return nx.community.modularity(nx_G, communities)
        except:
            return 0.0

def calculate_average_neighbor_degree(G, node):
    """计算节点的邻居平均度数"""
    neighbors = list(G.neighbors(node))
    if not neighbors:
        return 0.0
    degrees = [G.degree(neighbor) for neighbor in neighbors]
    return sum(degrees) / len(degrees)

def create_ego_network(G, node):
    """创建二跳邻居网络"""
    try:
        # EasyGraph的ego_graph函数
        # Todo：确认到底是返回几跳网络
        return eg_f.ego_graph(G, node)
    except Exception as e:
        print(f"创建邻居网络出错: {e}")
        try:
            # 使用NetworkX作为备选
            nx_G = nx.Graph()
            for u, v in G.edges:
                nx_G.add_edge(u, v)
            return nx.ego_graph(nx_G, node, radius=2)
        except:
            return None

def calculate_network_metrics(ego_graph, center_node):
    """计算网络的六个指标"""
    metrics = {}
    
    try:
        # 1. 密度
        metrics['density'] = eg.density(ego_graph)
        
        # 2. 聚类系数
        metrics['clustering_coefficient'] = eg_f.clustering(ego_graph, center_node)
        
        # 3. 邻居平均度
        metrics['average_nearest_neighbor_degree'] = calculate_average_neighbor_degree(ego_graph, center_node)
        
        # 4. 局部介数中心性
        bc = eg_f.betweenness_centrality(ego_graph)
        metrics['ego_betweenness'] = bc[center_node]
        
        # 5. 谱半径
        metrics['spectral_radius'] = calculate_spectral_radius(ego_graph)
        
        # 6. 模块度
        metrics['modularity'] = calculate_modularity(ego_graph)
        
        return metrics
    except Exception as e:
        print(f"计算网络指标出错: {e}")
        return {
            'density': 0.0,
            'clustering_coefficient': 0.0,
            'average_nearest_neighbor_degree': 0.0,
            'ego_betweenness': 0.0,
            'spectral_radius': 0.0,
            'modularity': 0.0
        }

def save_metrics_to_jsonl(metrics_data, output_path):
    """保存网络指标到JSONL文件"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for user_id, metrics in metrics_data.items():
            record = {"user_id": user_id, "network_metrics": metrics}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"指标数据已保存到: {output_path}")

def load_metrics_from_jsonl(input_path):
    """从JSONL文件加载网络指标"""
    metrics_data = {}
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            user_id = record["user_id"]
            metrics = record["network_metrics"]
            metrics_data[user_id] = metrics
    return metrics_data

def metrics_to_dataframe(metrics_data):
    """将指标数据转换为DataFrame格式"""
    records = []
    for user_id, metrics in metrics_data.items():
        record = {"user_id": user_id}
        record.update(metrics)
        records.append(record)
    return pd.DataFrame(records)

def calculate_correlation(metrics_df, popularity_df):
    """计算网络指标与影响力的皮尔逊相关系数"""
    # 确保user_id列的类型一致
    metrics_df['user_id'] = metrics_df['user_id'].astype(str)
    popularity_df['user_id'] = popularity_df['user_id'].astype(str)
    
    # 合并数据
    merged_df = pd.merge(metrics_df, popularity_df[['user_id', 'avg_popularity']], 
                        on="user_id", how="inner")
    
    print(f"合并后的数据包含 {len(merged_df)} 行")
    
    # 计算相关性
    correlations = {}
    for metric in ['density', 'clustering_coefficient', 'average_nearest_neighbor_degree', 
                 'ego_betweenness', 'spectral_radius', 'modularity']:
        if metric not in merged_df.columns:
            print(f"警告: 列 {metric} 不在合并后的数据中")
            continue
            
        try:
            corr, p_value = stats.pearsonr(merged_df[metric], merged_df['avg_popularity'])
            correlations[metric] = {"correlation": corr, "p_value": p_value}
        except Exception as e:
            print(f"计算 {metric} 相关性时出错: {e}")
            correlations[metric] = {"correlation": 0, "p_value": 1}
    
    return correlations, merged_df

def plot_correlations(correlations, output_path=None):
    """绘制相关性条形图"""
    metrics = list(correlations.keys())
    corr_values = [correlations[m]["correlation"] for m in metrics]
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, corr_values)
    
    # 正相关为蓝色，负相关为红色
    for i, v in enumerate(corr_values):
        bars[i].set_color('blue' if v >= 0 else 'red')
    
    plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    plt.ylabel('皮尔逊相关系数')
    plt.title('网络指标与影响力的相关性')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path)
        print(f"相关性图表已保存到: {output_path}")
    
    plt.show()

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"开始分析时间: {start_time}")
    
    # 设置路径
    base_dir = 'C:/Tengfei/data/data/domain_networks/merged_network'
    edges_path = os.path.join(base_dir, 'edges.csv')
    popularity_path = os.path.join(base_dir, 'popularity.csv')
    output_dir = 'C:/Tengfei/data/results'
    metrics_output = os.path.join(output_dir, 'network_metrics.jsonl')
    
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 检查是否已经计算过网络指标
    if os.path.exists(metrics_output):
        print(f"找到已有的网络指标文件: {metrics_output}")
        print("是否重新计算网络指标？(y/n)")
        recalculate = input().lower() == 'y'
    else:
        recalculate = True
    
    # 步骤1和2：加载数据、构建图并计算网络指标
    if recalculate:
        print("正在加载网络数据...")
        edges_df = pd.read_csv(edges_path)
        popularity_df = pd.read_csv(popularity_path)
        
        # 构建无向图
        print("正在构建网络...")
        G = eg.Graph()
        edge_count = 0
        for _, row in edges_df.iterrows():
            source = str(row['source'])
            target = str(row['target'])
            G.add_edge(source, target)
            edge_count += 1
            if edge_count % 10000 == 0:
                print(f"已加载 {edge_count} 条边")
        
        print(f"网络构建完成，包含 {G.number_of_nodes()} 个节点和 {G.number_of_edges()} 条边")
        
        # 计算每个用户的二跳邻居网络指标
        print("正在计算用户网络指标...")
        metrics_data = {}
        
        # 获取需要计算的用户列表 - 使用popularity.csv中的用户
        popularity_df['user_id'] = popularity_df['user_id'].astype(str)
        users_to_process = set(popularity_df['user_id'].tolist())
        users_in_graph = set(str(node) for node in G.nodes)
        valid_users = users_to_process.intersection(users_in_graph)
        
        print(f"在图中找到 {len(valid_users)} 个有效用户")
        
        # 计算每个用户的指标
        processed_count = 0
        for user_id in valid_users:
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"正在处理：{processed_count}/{len(valid_users)}")
            
            try:
                # 创建用户的二跳邻居网络
                ego_graph = create_ego_network(G, user_id)
                if ego_graph is None or ego_graph.number_of_nodes() <= 1:
                    print(f"用户 {user_id} 的邻居网络为空或只有一个节点，跳过")
                    continue
                
                # 计算网络指标
                metrics = calculate_network_metrics(ego_graph, user_id)
                metrics_data[user_id] = metrics
            except Exception as e:
                print(f"处理用户 {user_id} 时出错: {e}")
        
        # 保存网络指标
        save_metrics_to_jsonl(metrics_data, metrics_output)
        print(f"已计算 {len(metrics_data)} 个用户的网络指标")
    else:
        # 加载已有的网络指标
        print("加载已有的网络指标...")
        metrics_data = load_metrics_from_jsonl(metrics_output)
        popularity_df = pd.read_csv(popularity_path)
        print(f"已加载 {len(metrics_data)} 个用户的网络指标")
    
    # 步骤3：将指标转换为DataFrame格式
    metrics_df = metrics_to_dataframe(metrics_data)
    
    # 步骤4：计算相关性
    print("正在计算相关性...")
    correlations, merged_df = calculate_correlation(metrics_df, popularity_df)
    
    # 打印相关性结果
    print("\n网络指标与影响力的皮尔逊相关系数：")
    print("-" * 60)
    print(f"{'指标':<30} {'相关系数':<15} {'p值':<15} {'显著性'}")
    print("-" * 60)
    for metric, stats in correlations.items():
        significance = "显著" if stats["p_value"] < 0.05 else "不显著"
        print(f"{metric:<30} {stats['correlation']:<15.4f} {stats['p_value']:<15.4f} {significance}")
    
    # 保存相关性结果
    correlation_output = os.path.join(output_dir, 'network_correlation_results.csv')
    correlation_df = pd.DataFrame([
        {"metric": metric, "correlation": stats["correlation"], "p_value": stats["p_value"]}
        for metric, stats in correlations.items()
    ])
    correlation_df.to_csv(correlation_output, index=False)
    print(f"\n相关性结果已保存到: {correlation_output}")
    
    # 步骤5：可选的可视化
    print("\n是否生成可视化图表？(y/n)")
    if input().lower() == 'y':
        plot_path = os.path.join(output_dir, 'network_correlation_plot.png')
        plot_correlations(correlations, plot_path)
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"分析完成，总耗时: {duration}")

if __name__ == "__main__":
    main()