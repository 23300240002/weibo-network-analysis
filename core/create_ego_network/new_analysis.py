import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import easygraph
import easygraph.classes as eg
import easygraph.functions as eg_f
from scipy import linalg
from datetime import datetime
from collections import defaultdict # 用于存储社区权重

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimSun']
plt.rcParams['axes.unicode_minus'] = False

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        # 先转为字符串
        id_str = str(id_value).strip()
        # 特殊处理-2147483648
        if id_str == '-2147483648':
            return id_str  # 保持原样
        # 尝试通过数值规范化，去除.0后缀
        return str(int(float(id_str)))
    except:
        # 如果转换失败，返回原始字符串
        return str(id_value).strip()

def calculate_spectral_radius(G):
    """计算图的谱半径（最大特征值的绝对值）"""
    try:
        # 使用easygraph模块调用
        adj_matrix = easygraph.to_numpy_array(G)
        eigenvalues = linalg.eigvals(adj_matrix)
        return float(np.max(np.abs(eigenvalues)))
    except Exception as e:
        print(f"计算谱半径出错: {e}")
        return 0.0

def calculate_modularity(G):
    """计算图的模块度，使用test1.py中已验证的算法"""
    try:   
        # 直接调用test1.py中验证有效的函数
        partition, modularity_value = louvain_communities_fixed(G, threshold=0.001)
        return modularity_value
    except Exception as e:
        print(f"  - 计算模块度出错: {e}")
        import traceback
        print(f"  - 详细错误: {traceback.format_exc()}")
        return 0.0

def louvain_communities_fixed(G, weight="weight", threshold=0.001, max_iterations=100, max_levels=10):
    """修复版的Louvain社区检测算法，基于test1.py的验证代码"""
    # 初始化，每个节点作为一个社区
    partition = [{u} for u in G.nodes]
    m = G.size(weight="weight")
    is_directed = G.is_directed()
    
    # 计算初始模块度
    initial_mod = modularity_fixed(G, partition)
    
    # 第一轮迭代
    level = 1
    partition, inner_partition, improvement = _one_level_fixed(G, m, partition, is_directed)
    
    # 计算新模块度
    new_mod = modularity_fixed(G, partition)
    mod_gain = new_mod - initial_mod
    
    # 继续迭代直到没有改进或达到最大层数
    while improvement and level < max_levels:
        level += 1
        
        # 构建新图
        G_new = G.__class__()
        node2com = {}
        
        for i, part in enumerate(partition):
            G_new.add_node(i)
            for node in part:
                node2com[node] = i
        
        # 添加边 - 使用正确的edge访问方式
        for edge in G.edges:
            u, v, data = edge
            if u in node2com and v in node2com:  # 确保节点在映射中
                com1 = node2com[u]
                com2 = node2com[v]
                edge_weight = data.get(weight, 1)
                
                # 如果边已存在，增加权重
                if G_new.has_edge(com1, com2):
                    G_new[com1][com2][weight] += edge_weight
                else:
                    G_new.add_edge(com1, com2, **{weight: edge_weight})
        
        # 更新图
        G = G_new
        
        # 下一轮迭代
        partition = [{u} for u in G.nodes]
        partition, inner_partition, improvement = _one_level_fixed(G, m, partition, is_directed)
        
        # 计算新模块度
        if improvement:
            cur_mod = modularity_fixed(G, partition)
            mod_gain = cur_mod - new_mod
            
            # 检查是否达到阈值
            if mod_gain <= threshold:
                break
            new_mod = cur_mod
    
    # 返回社区划分和模块度值
    return partition, new_mod

def _one_level_fixed(G, m, partition, is_directed=False, max_iterations=100):
    """修复版的_one_level函数，解决索引错误和振荡问题"""
    # 初始化每个节点为单独社区
    node2com = {u: i for i, u in enumerate(G.nodes)}
    inner_partition = [{u} for u in G.nodes]
    
    # 获取节点度数和总度
    degrees = dict(G.degree(weight="weight"))
    Stot = []
    for i in range(len(partition)):
        Stot.append(sum(degrees.get(node, 0) for node in partition[i]))
    
    # 获取邻居信息
    nbrs = {u: {v: data.get("weight", 1) for v, data in G[u].items() if v != u} for u in G}
    rand_nodes = list(G.nodes)
    
    # 主循环
    nb_moves = 1
    iteration = 0
    total_moves = 0
    
    # 振荡检测变量
    recent_moves = []  # 记录最近几次迭代的移动数量
    oscillation_threshold = 3  # 连续相同移动次数视为振荡
    
    while nb_moves > 0 and iteration < max_iterations:
        iteration += 1
        nb_moves = 0
        
        # 处理每个节点
        for u in rand_nodes:
            best_mod = 0
            best_com = node2com[u]
            
            # 计算到邻居社区的权重
            weights2com = defaultdict(float)
            for nbr, wt in nbrs.get(u, {}).items():
                weights2com[node2com[nbr]] += wt
            
            # 移除节点的成本
            degree = degrees.get(u, 0)
            if best_com < len(Stot):  # 确保索引有效
                Stot[best_com] -= degree
                remove_cost = -weights2com.get(best_com, 0) / m + (Stot[best_com] * degree) / (2 * m**2)
            else:
                remove_cost = 0
            
            # 计算最佳社区
            for nbr_com, wt in weights2com.items():
                if nbr_com < len(Stot):  # 确保索引有效
                    gain = remove_cost + wt / m - (Stot[nbr_com] * degree) / (2 * m**2)
                    if gain > best_mod:
                        best_mod = gain
                        best_com = nbr_com
            
            # 恢复Stot值
            if best_com < len(Stot):  # 确保索引有效
                Stot[best_com] += degree
            
            # 如果找到更好的社区，执行移动
            if best_com != node2com[u]:
                com = G.nodes[u].get("nodes", {u})
                if not isinstance(com, set):
                    com = {com}
                
                partition[node2com[u]].difference_update(com)
                inner_partition[node2com[u]].remove(u)
                
                if best_com < len(partition):  # 确保索引有效
                    partition[best_com].update(com)
                    inner_partition[best_com].add(u)
                
                nb_moves += 1
                total_moves += 1
                node2com[u] = best_com
        
        # 清理空社区
        old_partition = partition.copy()
        partition = list(filter(len, partition))
        inner_partition = list(filter(len, inner_partition))
        
        # 重要修复：重建node2com映射
        if len(old_partition) != len(partition):
            new_node2com = {}
            for i, community in enumerate(partition):
                for node in community:
                    new_node2com[node] = i
            node2com = new_node2com
            
            # 更新Stot数组
            new_Stot = []
            for i in range(len(partition)):
                new_Stot.append(sum(degrees.get(node, 0) for node in partition[i]))
            Stot = new_Stot
        
        # 振荡检测，避免在振荡模式下无限循环
        recent_moves.append(nb_moves)
        if len(recent_moves) > oscillation_threshold:
            recent_moves.pop(0)
            if len(set(recent_moves)) == 1 and recent_moves[0] > 0:
                # 一旦检测到振荡，说明没有进一步的改进，则退出循环
                break
    
    return partition, inner_partition, total_moves > 0

def modularity_fixed(G, communities, weight="weight"):
    """计算给定社区划分的模块度，基于test1.py的验证版本"""
    if not isinstance(communities, list):
        communities = list(communities)

    directed = G.is_directed()
    m = G.size(weight=weight)
    if m == 0:
        return 0
        
    if directed:
        out_degree = dict(G.out_degree(weight=weight))
        in_degree = dict(G.in_degree(weight=weight))
        norm = 1 / m
    else:
        out_degree = dict(G.degree(weight=weight))
        in_degree = out_degree
        norm = 1 / (2 * m)
    
    # 计算社区内部边数（用于调试）
    internal_edges = 0
    for c in communities:
        for u in c:
            for v in c:
                if G.has_edge(u, v):
                    internal_edges += 1
    
    # 定义边权重计算函数
    def val(u, v):
        try:
            w = G[u][v].get(weight, 1)
        except KeyError:
            w = 0
        # Double count self-loops if the graph is undirected.
        if u == v and not directed:
            w *= 2
        return w - in_degree.get(u, 0) * out_degree.get(v, 0) * norm
    
    # 计算模块度
    Q = 0
    for c in communities:
        for u in c:
            for v in c:
                Q += val(u, v)
    
    Q = Q * norm
    
    # 调试用，这些信息可以体现每次迭代的社区划分和模块度，判断是否有改进
    print(f"  - 社区数量: {len(communities)}")
    print(f"  - 社区内部边数: {internal_edges}")
    print(f"  - 最终模块度: {Q:.6f}")
    
    return Q

def calculate_average_neighbor_degree(G, node):
    """计算节点的邻居平均度数"""
    neighbors = list(G.neighbors(node))
    if not neighbors:
        return 0.0
    
    # 直接计算邻居的度数（通过计算邻居的邻居数量）
    neighbor_degrees = []
    for neighbor in neighbors:
        # 直接计算每个邻居有多少个连接节点
        deg_val = len(list(G.neighbors(neighbor)))
        neighbor_degrees.append(deg_val)
    
    return sum(neighbor_degrees) / len(neighbor_degrees)

def create_ego_network(G, node):
    """创建二跳邻居网络"""
    try:
        # 使用EasyGraph创建二跳邻居网络
        return eg_f.ego_graph(G, node, radius=2)
    except Exception as e:
        print(f"创建邻居网络出错: {e}")
        # 使用基本EasyGraph操作手动构建
        try:
            # 获取一跳和二跳邻居
            one_hop = set(G.neighbors(node))
            two_hop = set()
            for n in one_hop:
                two_hop.update(G.neighbors(n))
            
            # 创建新图
            H = G.copy()
            # 只保留所需节点
            nodes_to_keep = {node} | one_hop | two_hop
            nodes_to_remove = set(H.nodes) - nodes_to_keep
            for n in nodes_to_remove:
                H.remove_node(n)
            return H
        except Exception as e2:
            print(f"手动构建二跳邻居网络也失败: {e2}")
            return None

def calculate_network_metrics(ego_graph, center_node):
    """计算网络的六个指标，并输出每一步计算结果"""
    metrics = {}
    
    try:
        # 基本网络信息
        metrics['node_count'] = ego_graph.number_of_nodes()
        metrics['edge_count'] = ego_graph.number_of_edges()
        metrics['center_node'] = center_node
        
        # 密度
        try:
            metrics['density'] = eg.density(ego_graph)
            print(f"  - 密度计算完成: {metrics['density']:.6f}")
        except Exception as e:
            print(f"  - 计算密度出错: {e}")
            metrics['density'] = 0.0
        
        # 聚类系数
        try:
            metrics['clustering_coefficient'] = eg_f.clustering(ego_graph, center_node)
            print(f"  - 聚类系数计算完成: {metrics['clustering_coefficient']:.6f}")
        except Exception as e:
            print(f"  - 计算聚类系数出错: {e}")
            metrics['clustering_coefficient'] = 0.0
        
        # 邻居平均度
        try:
            metrics['average_nearest_neighbor_degree'] = calculate_average_neighbor_degree(ego_graph, center_node)
            print(f"  - 邻居平均度计算完成: {metrics['average_nearest_neighbor_degree']:.6f}")
        except Exception as e:
            print(f"  - 计算邻居平均度出错: {e}")
            metrics['average_nearest_neighbor_degree'] = 0.0
        
        # 介数中心性
        try:
            bc_start = datetime.now()
            bc = eg_f.betweenness_centrality(ego_graph)
            # 处理EasyGraph返回列表的情况
            if isinstance(bc, list):
                node_list = list(ego_graph.nodes)
                center_index = node_list.index(center_node)
                metrics['ego_betweenness'] = bc[center_index]
            else:
                metrics['ego_betweenness'] = bc[center_node]
            bc_time = datetime.now() - bc_start
            print(f"  - 介数中心性计算完成: {metrics['ego_betweenness']:.6f}, 耗时: {bc_time}")
        except Exception as e:
            print(f"  - 计算介数中心性出错: {e}")
            metrics['ego_betweenness'] = 0.0
        
        # 谱半径
        try:
            sr_start = datetime.now()
            metrics['spectral_radius'] = calculate_spectral_radius(ego_graph)
            sr_time = datetime.now() - sr_start
            print(f"  - 谱半径计算完成: {metrics['spectral_radius']:.6f}, 耗时: {sr_time}")
        except Exception as e:
            print(f"  - 计算谱半径出错: {e}")
            metrics['spectral_radius'] = 0.0
        
        # 模块度
        try:
            mod_start = datetime.now()
            metrics['modularity'] = calculate_modularity(ego_graph)
            mod_time = datetime.now() - mod_start
            print(f"  - 模块度计算完成: {metrics['modularity']:.6f}, 耗时: {mod_time}")
        except Exception as e:
            print(f"  - 计算模块度出错: {e}")
            metrics['modularity'] = 0.0
        
        return metrics
    except Exception as e:
        print(f"  - 计算网络指标整体出错: {e}")
        return {
            'node_count': 0,
            'edge_count': 0,
            'center_node': center_node,
            'density': 0.0,
            'clustering_coefficient': 0.0,
            'average_nearest_neighbor_degree': 0.0,
            'ego_betweenness': 0.0,
            'spectral_radius': 0.0,
            'modularity': 0.0
        }

def save_ego_networks_info(ego_networks_info, output_path):
    """保存二跳邻居网络信息到JSONL文件"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for user_id, info in ego_networks_info.items():
            record = {"user_id": user_id, "ego_network_info": info}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"二跳邻居网络信息已保存到: {output_path}")

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
    # 确保user_id列的类型一致，使用规范化处理
    metrics_df['user_id'] = metrics_df['user_id'].apply(normalize_id)
    popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
    
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
    ego_networks_output = os.path.join(output_dir, 'ego_networks_info.jsonl')
    
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
        
        # 预处理：规范化ID
        print("正在规范化用户ID...")
        # 创建规范化ID的映射
        source_id_map = {row['source']: normalize_id(row['source']) for _, row in edges_df.iterrows()}
        target_id_map = {row['target']: normalize_id(row['target']) for _, row in edges_df.iterrows()}
        edges_df['source'] = edges_df['source'].map(source_id_map)
        edges_df['target'] = edges_df['target'].map(target_id_map)
        
        # 规范化popularity的ID
        popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
        
        # 构建有向图
        print("正在构建网络...")
        G = eg.DiGraph()  # 有向图
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
        ego_networks_info = {}  # 存储二跳邻居网络信息
        
        # 获取需要计算的用户列表 - 使用所有规范化后的用户ID
        users_to_process = set(popularity_df['user_id'].tolist())
        users_in_graph = set(str(node) for node in G.nodes)
        
        # 创建用户ID映射表
        id_mapping = {}
        for orig_id in popularity_df['user_id'].unique():
            norm_id = normalize_id(orig_id)
            if norm_id in users_in_graph:
                id_mapping[orig_id] = norm_id
        
        valid_users = set(id_mapping.values())
        
        # 打印匹配统计信息
        print(f"\n=== 用户匹配统计 ===")
        print(f"Popularity文件中用户总数: {len(popularity_df['user_id'].unique())}")
        print(f"图中节点总数: {len(G.nodes)}")
        print(f"有效匹配用户数: {len(valid_users)}")
        print(f"匹配率: {len(valid_users)/len(popularity_df['user_id'].unique())*100:.2f}%")
        
        # 显示一些样本
        print(f"\n=== 样本匹配示例 ===")
        sample_size = min(5, len(id_mapping))
        sample_ids = list(id_mapping.items())[:sample_size]
        for orig_id, norm_id in sample_ids:
            print(f"原始ID: {orig_id} -> 规范化ID: {norm_id}")
        
        # 计算每个用户的指标
        processed_count = 0
        total_users = len(valid_users)
        print(f"\n开始计算 {total_users} 个用户的网络指标...")
        loop_start_time = datetime.now()
        
        for user_id in valid_users:
            processed_count += 1
            completion = processed_count / total_users * 100
            print(f"\n处理用户 {user_id} (第{processed_count}/{total_users}个, 完成{completion:.1f}%):")
            
            try:
                # 创建用户的二跳邻居网络
                print(f"  - 开始创建二跳邻居网络...")
                ego_start_time = datetime.now()
                ego_graph = create_ego_network(G, user_id)
                ego_time = datetime.now() - ego_start_time
                
                if ego_graph:
                    print(f"  - 二跳邻居网络创建完成: {ego_graph.number_of_nodes()} 节点, {ego_graph.number_of_edges()} 边")
                    print(f"  - 耗时: {ego_time}")
                else:
                    print(f"  - 二跳邻居网络创建失败, 跳过此用户")
                    continue
                
                if ego_graph.number_of_nodes() <= 1:
                    print(f"  - 二跳邻居网络节点数过少，跳过此用户")
                    continue
                
                # 计算网络指标
                print(f"  - 开始计算网络指标...")
                metrics_start_time = datetime.now()
                metrics = calculate_network_metrics(ego_graph, user_id)
                metrics_time = datetime.now() - metrics_start_time
                print(f"  - 所有网络指标计算完成, 总耗时: {metrics_time}")
                
                metrics_data[user_id] = metrics
                
                # 存储二跳邻居网络信息
                ego_networks_info[user_id] = {
                    'node_count': ego_graph.number_of_nodes(),
                    'edge_count': ego_graph.number_of_edges(),
                    'nodes': list(ego_graph.nodes),
                    'metrics': {
                        'density': metrics['density'],
                        'clustering_coefficient': metrics['clustering_coefficient'],
                        'average_nearest_neighbor_degree': metrics['average_nearest_neighbor_degree'],
                        'ego_betweenness': metrics['ego_betweenness'],
                        'spectral_radius': metrics['spectral_radius'],
                        'modularity': metrics['modularity']
                    }
                }
                
                # 每处理100个用户保存一次中间结果
                if processed_count % 100 == 0:
                    save_metrics_to_jsonl(metrics_data, metrics_output)
                    save_ego_networks_info(ego_networks_info, ego_networks_output)
                    print(f"  - 已保存中间结果 ({processed_count}/{total_users})")
                    
                
            except Exception as e:
                print(f"  - 处理用户 {user_id} 时出错: {e}")
        
        # 保存最终结果
        save_metrics_to_jsonl(metrics_data, metrics_output)
        save_ego_networks_info(ego_networks_info, ego_networks_output)
        
        loop_duration = datetime.now() - loop_start_time
        print(f"\n用户处理循环完成，总耗时: {loop_duration}")
        print(f"平均每个用户处理时间: {loop_duration.total_seconds() / max(1, len(metrics_data)):.2f} 秒")
        print(f"已计算 {len(metrics_data)} 个用户的网络指标")
    else:
        # 加载已有的网络指标
        print("加载已有的网络指标...")
        metrics_data = load_metrics_from_jsonl(metrics_output)
        popularity_df = pd.read_csv(popularity_path)
        print(f"已加载 {len(metrics_data)} 个用户的网络指标")
    
    # 步骤3：将指标转换为DataFrame格式
    metrics_df = metrics_to_dataframe(metrics_data)
    
    # 处理空结果情况
    if len(metrics_df) == 0:
        print("错误：没有有效的网络指标数据，无法计算相关性")
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"分析中止，总耗时: {duration}")
        return
    
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
    
    # 生成相关性图表
    plot_path = os.path.join(output_dir, 'network_correlation_plot.png')
    plot_correlations(correlations, plot_path)
    
    # 保存合并后的数据，以便进一步分析
    merged_output = os.path.join(output_dir, 'merged_metrics_popularity.csv')
    merged_df.to_csv(merged_output, index=False)
    print(f"合并后的数据已保存到: {merged_output}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"分析完成，总耗时: {duration}")

if __name__ == "__main__":
    main()