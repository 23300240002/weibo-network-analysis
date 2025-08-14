import os
import pandas as pd
import networkx as nx
import json
from datetime import datetime

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def build_neighbor_network(edges_file, users_file=None, popularity_file=None):
    """根据关系构建邻居网络"""
    print(f"从 {edges_file} 构建邻居网络...")
    
    # 读取边数据
    edges_df = pd.read_csv(edges_file)
    
    # 创建有向图
    G = nx.DiGraph()
    
    # 添加边
    for _, edge in edges_df.iterrows():
        G.add_edge(str(edge['source']), str(edge['target']))
    
    # 如果提供了用户信息文件，则添加节点属性
    if users_file and os.path.exists(users_file):
        users_df = pd.read_csv(users_file)
        for _, user in users_df.iterrows():
            user_id = str(user['user_id'])
            if user_id in G.nodes:
                # 添加节点属性
                for col in users_df.columns:
                    if col != 'user_id':
                        G.nodes[user_id][col] = user.get(col)
    
    # 如果提供了流行度文件，则添加流行度属性
    if popularity_file and os.path.exists(popularity_file):
        pop_df = pd.read_csv(popularity_file)
        for _, row in pop_df.iterrows():
            user_id = str(row['user_id'])
            if user_id in G.nodes:
                # 添加流行度属性
                G.nodes[user_id]['avg_popularity'] = row.get('avg_popularity', 0)
                G.nodes[user_id]['interaction_count'] = row.get('interaction_count', 0)
    
    print(f"网络构建完成! 包含 {len(G.nodes)} 个节点和 {len(G.edges)} 条边")
    return G

def analyze_network(G, is_directed=True):
    """分析网络的基本指标"""
    print("计算网络基本指标...")
    metrics = {}
    
    # 基本指标
    metrics["节点数"] = len(G.nodes)
    metrics["边数"] = len(G.edges)
    
    # 度数统计
    if len(G.nodes) > 0:
        if is_directed:
            in_degrees = dict(G.in_degree())
            out_degrees = dict(G.out_degree())
            total_degrees = {node: in_degrees.get(node, 0) + out_degrees.get(node, 0) 
                            for node in G.nodes()}
            
            # 统计没有出边的节点
            nodes_without_outgoing_edges = [node for node, degree in out_degrees.items() if degree == 0]
            metrics["没有出边的节点数"] = len(nodes_without_outgoing_edges)
            metrics["没有出边节点占比"] = len(nodes_without_outgoing_edges) / len(G.nodes) if len(G.nodes) > 0 else 0
            
            metrics["平均入度"] = sum(in_degrees.values()) / len(G.nodes)
            metrics["平均出度"] = sum(out_degrees.values()) / len(G.nodes)
            metrics["平均总度数"] = sum(total_degrees.values()) / len(G.nodes)
            
            metrics["最大入度"] = max(in_degrees.values()) if in_degrees else 0
            metrics["最大出度"] = max(out_degrees.values()) if out_degrees else 0
            metrics["最大总度数"] = max(total_degrees.values()) if total_degrees else 0
        else:
            degrees = dict(G.degree())
            metrics["平均度数"] = sum(degrees.values()) / len(G.nodes)
            metrics["最大度数"] = max(degrees.values()) if degrees else 0
    else:
        metrics["平均度数"] = 0
    
    # 网络密度
    metrics["网络密度"] = nx.density(G)
    
    # 连通性分析
    if is_directed:
        # 有向图的弱连通和强连通分量
        weakly_connected_components = list(nx.weakly_connected_components(G))
        strongly_connected_components = list(nx.strongly_connected_components(G))
        
        metrics["弱连通分量数"] = len(weakly_connected_components)
        metrics["强连通分量数"] = len(strongly_connected_components)
        
        # 强连通分量信息
        if strongly_connected_components:
            largest_scc = max(strongly_connected_components, key=len)
            metrics["最大强连通分量节点数"] = len(largest_scc)
            metrics["最大强连通分量占比"] = len(largest_scc) / len(G.nodes) if len(G.nodes) > 0 else 0
    
    # 聚类系数
    try:
        if is_directed:
            metrics["有向聚类系数"] = nx.average_clustering(G)
        else:
            metrics["聚类系数"] = nx.average_clustering(G)
    except Exception as e:
        print(f"计算聚类系数时出错: {e}")
        metrics["聚类系数"] = 0
    
    # 流行度统计(如果节点有流行度属性)
    popularity_values = [G.nodes[n].get('avg_popularity', 0) for n in G.nodes if 'avg_popularity' in G.nodes[n]]
    if popularity_values:
        metrics["平均流行度"] = sum(popularity_values) / len(popularity_values)
        metrics["最大流行度"] = max(popularity_values)
        metrics["最小流行度"] = min(popularity_values)
    
    return metrics

def process_network(network_dir, output_dir):
    """处理单个网络目录下的数据"""
    # 确定文件路径
    edges_file = os.path.join(network_dir, 'edges.csv')
    users_file = os.path.join(network_dir, 'users.csv')
    popularity_file = os.path.join(network_dir, 'popularity.csv')
    
    # 检查必要文件是否存在
    if not os.path.exists(edges_file):
        print(f"错误: 找不到边文件 {edges_file}")
        return
    
    # 构建网络
    G = build_neighbor_network(edges_file, users_file, popularity_file)
    
    # 分析完整网络
    metrics = analyze_network(G, is_directed=True)
    
    # 提取网络名称(取目录名的最后一部分)
    network_name = os.path.basename(network_dir)
    
    # 保存分析结果
    result_file = os.path.join(output_dir, f'{network_name}_analysis.txt')
    with open(result_file, 'w', encoding='utf-8') as f:
        f.write(f"=== {network_name} 网络分析结果 ===\n")
        f.write(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 添加没有出边用户的详细说明
        if "没有出边的节点数" in metrics:
            f.write(f"=== 关注可见性分析 ===\n")
            f.write(f"没有出边的用户数量: {metrics['没有出边的节点数']} (占比 {metrics['没有出边节点占比']*100:.2f}%)\n")
            f.write("说明: 这些用户可能是真的没有关注任何人，或者设置了关注列表对未关注者不可见\n")
            f.write("这一比例过高可能会影响网络分析的准确性\n\n")
        
        f.write("=== 完整网络指标 ===\n")
        for key, value in metrics.items():
            if isinstance(value, float):
                f.write(f"{key}: {value:.6f}\n")
            else:
                f.write(f"{key}: {value}\n")
    
    # 分析最大强连通分量
    scc_result = None
    if metrics.get("强连通分量数", 0) > 0:
        largest_scc = max(nx.strongly_connected_components(G), key=len)
        SG = G.subgraph(largest_scc).copy()
        
        # 分析最大强连通分量
        scc_metrics = analyze_network(SG, is_directed=True)
        
        # 将结果追加到文件
        with open(result_file, 'a', encoding='utf-8') as f:
            f.write("\n=== 最大强连通分量指标 ===\n")
            for key, value in scc_metrics.items():
                if isinstance(value, float):
                    f.write(f"{key}: {value:.6f}\n")
                else:
                    f.write(f"{key}: {value}\n")
        
        scc_result = scc_metrics
    
    print(f"{network_name} 网络分析完成，结果已保存到 {result_file}")
    return metrics, scc_result

def main():
    start_time = datetime.now()
    
    # 设置输入和输出目录
    base_dir = 'C:/Tengfei/data/data/domain_networks'
    output_dir = 'C:/Tengfei/data/results/network_analysis'
    
    # 确保输出目录存在
    ensure_dir(output_dir)
    
    # 加载配置文件获取用户ID列表
    config_path = 'C:/Tengfei/data/crawler/weiboSpider/config.json'
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            user_ids = config.get('user_id_list', [])
    except Exception as e:
        print(f"读取配置文件出错: {e}")
        user_ids = []
    
    # 处理每个用户的独立网络
    results = {}
    for user_id in user_ids:
        user_dir = f'{base_dir}/user_{user_id}'
        if os.path.exists(user_dir):
            print(f"\n===== 分析用户 {user_id} 的网络 =====")
            metrics, scc_metrics = process_network(user_dir, output_dir)
            results[f'user_{user_id}'] = {
                'complete': metrics,
                'largest_scc': scc_metrics
            }
    
    # 处理合并后的网络
    merged_dir = f'{base_dir}/merged_network'
    if os.path.exists(merged_dir):
        print(f"\n===== 分析合并网络 =====")
        metrics, scc_metrics = process_network(merged_dir, output_dir)
        results['merged_network'] = {
            'complete': metrics,
            'largest_scc': scc_metrics
        }
    
    # 保存所有结果的比较
    comparison_file = os.path.join(output_dir, 'networks_comparison.json')
    with open(comparison_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n所有网络分析完成!")
    print(f"总耗时: {duration}")
    print(f"比较结果已保存到 {comparison_file}")
    print(f"各网络详细分析结果已保存到 {output_dir} 目录")

if __name__ == "__main__":
    main()