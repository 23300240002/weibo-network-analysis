# 3.3 - 兼容新fetch阶段输出
import os
import json
import pandas as pd
import numpy as np
import easygraph as eg
import easygraph.functions as eg_f
from scipy import linalg
from datetime import datetime
from collections import defaultdict, deque
import signal
import sys

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        id_str = str(id_value).strip()
        if id_str == '-2147483648':
            return id_str
        return str(int(float(id_str)))
    except:
        return str(id_value).strip()

def signal_handler(signum, frame):
    """处理Ctrl+C信号"""
    print(f"\n\n⚠️ 收到中断信号 (Ctrl+C)")
    print(f"📁 正在保存当前进度...")
    sys.exit(0)

def load_celebrity_users(base_dir):
    """加载明星用户列表"""
    high_fans_file = os.path.join(base_dir, 'high_fans_users.csv')
    
    if not os.path.exists(high_fans_file):
        print(f"⚠️ 未找到明星用户文件: {high_fans_file}")
        print(f"   所有用户的is_celebrity将设为False")
        return set()
    
    try:
        high_fans_df = pd.read_csv(high_fans_file)
        celebrity_users = set(high_fans_df['user_id'].apply(normalize_id))
        print(f"✅ 成功加载 {len(celebrity_users)} 个明星用户")
        return celebrity_users
    except Exception as e:
        print(f"❌ 加载明星用户文件失败: {e}")
        print(f"   所有用户的is_celebrity将设为False")
        return set()

def load_user_categories(base_dir):
    """🔥 新增：加载用户类别信息（A/B/C）"""
    users_file = os.path.join(base_dir, 'users.csv')
    
    if not os.path.exists(users_file):
        print(f"⚠️ 未找到用户文件: {users_file}")
        print(f"   所有用户的category将设为Unknown")
        return {}
    
    try:
        users_df = pd.read_csv(users_file)
        users_df['user_id'] = users_df['user_id'].apply(normalize_id)
        
        # 检查是否有category列
        if 'category' not in users_df.columns:
            print(f"⚠️ users.csv中未找到category列")
            print(f"   所有用户的category将设为Unknown")
            return {}
        
        user_categories = dict(zip(users_df['user_id'], users_df['category']))
        
        # 统计各类用户数量
        category_counts = users_df['category'].value_counts()
        print(f"✅ 成功加载用户类别信息:")
        for category, count in category_counts.items():
            print(f"   - {category}类用户: {count} 个")
        
        return user_categories
    except Exception as e:
        print(f"❌ 加载用户类别文件失败: {e}")
        print(f"   所有用户的category将设为Unknown")
        return {}

def get_user_selected_metrics():
    """交互式选择要计算的网络指标"""
    print("\n" + "="*60)
    print("请选择要计算的网络指标：")
    print("="*60)
    print("1. 密度 (Density)")
    print("2. 聚类系数 (Clustering Coefficient)")  
    print("3. 邻居平均度 (Average Nearest Neighbor Degree)")
    print("4. 介数中心性 (Betweenness Centrality) - 计算较慢")
    print("5. 谱半径 (Spectral Radius)")
    print("6. 模块度 (Modularity)")
    print("="*60)
    print("输入示例：")
    print("  - 计算前三个指标：1 2 3")
    print("  - 计算五大指标（推荐）：1 2 3 5 6") 
    print("  - 计算全部六大指标：1 2 3 4 5 6")
    print("  - 快速模式（前三个）：1 2 3")
    
    while True:
        try:
            user_input = input("\n请输入指标序号（用空格分隔）: ").strip()
            if not user_input:
                print("❌ 输入不能为空，请重新输入")
                continue
                
            selected_numbers = [int(x.strip()) for x in user_input.split()]
            
            # 验证输入范围
            if not all(1 <= num <= 6 for num in selected_numbers):
                print("❌ 请输入1-6之间的数字")
                continue
                
            # 去重并排序
            selected_numbers = sorted(list(set(selected_numbers)))
            
            # 显示选择的指标
            metric_names = {
                1: "密度",
                2: "聚类系数", 
                3: "邻居平均度",
                4: "介数中心性",
                5: "谱半径",
                6: "模块度"
            }
            
            print(f"\n✅ 已选择 {len(selected_numbers)} 个指标：")
            for num in selected_numbers:
                print(f"   {num}. {metric_names[num]}")
            
            # 特别提醒介数中心性的计算时间
            if 4 in selected_numbers:
                print(f"\n⚠️ 注意：介数中心性计算较慢，大网络可能需要很长时间")
                confirm = input("确认要包含介数中心性吗？(y/n): ").strip().lower()
                if confirm != 'y':
                    selected_numbers.remove(4)
                    print(f"✅ 已移除介数中心性，当前选择：{selected_numbers}")
            
            return selected_numbers
            
        except ValueError:
            print("❌ 输入格式错误，请输入数字，用空格分隔")
        except KeyboardInterrupt:
            print("\n❌ 用户取消操作")
            return []

def load_existing_progress(metrics_output, ego_networks_output):
    """加载已有进度，返回已完成的用户集合和数据"""
    completed_users = set()
    existing_metrics = {}
    existing_ego_info = {}
    
    # 加载已有的网络指标
    if os.path.exists(metrics_output):
        print(f"找到已有网络指标文件: {metrics_output}")
        with open(metrics_output, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    user_id = record["user_id"]
                    metrics = record["network_metrics"]
                    existing_metrics[user_id] = metrics
                    completed_users.add(user_id)
                except Exception as e:
                    print(f"  - 警告：解析网络指标文件中的行时出错：{e}")
                    continue
    
    # 加载已有的ego网络信息
    if os.path.exists(ego_networks_output):
        print(f"找到已有ego网络信息文件: {ego_networks_output}")
        with open(ego_networks_output, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    user_id = record["user_id"]
                    ego_info = record["ego_network_info"]
                    existing_ego_info[user_id] = ego_info
                except Exception as e:
                    print(f"  - 警告：解析ego网络信息文件中的行时出错：{e}")
                    continue
    
    print(f"已加载 {len(completed_users)} 个已完成用户的数据")
    return completed_users, existing_metrics, existing_ego_info

def append_to_jsonl(data, file_path, is_metrics=True):
    """追加数据到JSONL文件"""
    with open(file_path, 'a', encoding='utf-8') as f:
        if is_metrics:
            for user_id, metrics in data.items():
                record = {"user_id": user_id, "network_metrics": metrics}
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:
            for user_id, ego_info in data.items():
                record = {"user_id": user_id, "ego_network_info": ego_info}
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

def ego_graph_fixed(G, n, radius=1, center=True, undirected=False, distance=None):
    """修复版的ego_graph函数，真正支持双向边"""
    
    if distance is not None:
        # 如果指定了距离权重，使用dijkstra算法
        if undirected and G.is_directed():
            # 对有向图执行双向搜索
            sp_out = eg_f.single_source_dijkstra(G, n, weight=distance)
            # 反向图搜索入边
            G_reversed = G.reverse()
            sp_in = eg_f.single_source_dijkstra(G_reversed, n, weight=distance)
            # 合并结果
            sp = {}
            for node, dist in sp_out.items():
                if dist <= radius:
                    sp[node] = dist
            for node, dist in sp_in.items():
                if dist <= radius:
                    if node not in sp or dist < sp[node]:
                        sp[node] = dist
        else:
            sp = eg_f.single_source_dijkstra(G, n, weight=distance)
    else:
        # 使用BFS进行双向搜索
        if undirected and G.is_directed():
            sp = bidirectional_bfs(G, n, radius)
        else:
            sp = eg_f.single_source_dijkstra(G, n)
    
    # 过滤距离范围内的节点
    nodes = [node for node, dist in sp.items() if dist <= radius]
    
    # 创建子图
    H = G.nodes_subgraph(nodes)
    
    if not center:
        H.remove_node(n)
    
    return H

def bidirectional_bfs(G, start_node, radius):
    """双向BFS：同时沿入边和出边扩展"""
    distances = {start_node: 0}
    current_level = {start_node}
    
    for level in range(1, radius + 1):
        next_level = set()
        
        for node in current_level:
            # 出边邻居（node关注的人）
            for successor in G.successors(node):
                if successor not in distances:
                    distances[successor] = level
                    next_level.add(successor)
            
            # 入边邻居（关注node的人）
            for predecessor in G.predecessors(node):
                if predecessor not in distances:
                    distances[predecessor] = level
                    next_level.add(predecessor)
        
        current_level = next_level
        if not current_level:
            break
    
    return distances

def calculate_global_degrees(G, center_node):
    """计算用户在全图中的出度和入度"""
    try:
        # 出度：该用户指向多少其他用户
        out_degree = G.out_degree(center_node) if G.has_node(center_node) else 0
        
        # 入度：多少其他用户指向该用户
        in_degree = G.in_degree(center_node) if G.has_node(center_node) else 0
        
        # 总度数
        total_degree = out_degree + in_degree
        
        return out_degree, in_degree, total_degree
    except Exception as e:
        print(f"    ⚠️ 计算全图度数失败: {e}")
        return 0, 0, 0

def calculate_spectral_radius(G):
    """计算图的谱半径（最大特征值的绝对值）"""
    adj_matrix = eg.to_numpy_array(G)
    eigenvalues = linalg.eigvals(adj_matrix)
    return float(np.max(np.abs(eigenvalues)))

def calculate_modularity(G):
    """计算图的模块度"""
    partition, modularity_value = louvain_communities_fixed(G, threshold=0.001)
    return modularity_value

def calculate_betweenness_centrality(G, center_node):
    """计算介数中心性，正确处理EasyGraph返回的结果"""
    bc_start = datetime.now()
    bc = eg_f.betweenness_centrality(G)
    
    # 处理EasyGraph可能返回列表或字典的情况
    if isinstance(bc, list):
        # 如果返回列表，需要找到中心节点的索引
        node_list = list(G.nodes)
        if center_node in node_list:
            center_index = node_list.index(center_node)
            result = bc[center_index] if center_index < len(bc) else 0.0
        else:
            result = 0.0
    elif isinstance(bc, dict):
        # 如果返回字典，直接获取
        result = bc.get(center_node, 0.0)
    else:
        # 其他情况，返回0
        result = 0.0
    
    bc_time = datetime.now() - bc_start
    return result, bc_time

def louvain_communities_fixed(G, weight="weight", threshold=0.001, max_iterations=100, max_levels=10):
    """修复版的Louvain社区检测算法"""
    partition = [{u} for u in G.nodes]
    m = G.size(weight="weight")
    is_directed = G.is_directed()
    
    initial_mod = modularity_fixed(G, partition)
    
    level = 1
    partition, inner_partition, improvement = _one_level_fixed(G, m, partition, is_directed)
    
    new_mod = modularity_fixed(G, partition)
    mod_gain = new_mod - initial_mod
    
    while improvement and level < max_levels:
        level += 1
        
        G_new = G.__class__()
        node2com = {}
        
        for i, part in enumerate(partition):
            G_new.add_node(i)
            for node in part:
                node2com[node] = i
        
        for edge in G.edges:
            u, v, data = edge
            if u in node2com and v in node2com:
                com1 = node2com[u]
                com2 = node2com[v]
                edge_weight = data.get(weight, 1)
                
                if G_new.has_edge(com1, com2):
                    G_new[com1][com2][weight] += edge_weight
                else:
                    G_new.add_edge(com1, com2, **{weight: edge_weight})
        
        G = G_new
        partition = [{u} for u in G.nodes]
        partition, inner_partition, improvement = _one_level_fixed(G, m, partition, is_directed)
        
        if improvement:
            cur_mod = modularity_fixed(G, partition)
            mod_gain = cur_mod - new_mod
            
            if mod_gain <= threshold:
                break
            new_mod = cur_mod
    
    return partition, new_mod

def _one_level_fixed(G, m, partition, is_directed=False, max_iterations=100):
    """修复版的_one_level函数"""
    node2com = {u: i for i, u in enumerate(G.nodes)}
    inner_partition = [{u} for u in G.nodes]
    
    degrees = dict(G.degree(weight="weight"))
    Stot = []
    for i in range(len(partition)):
        Stot.append(sum(degrees.get(node, 0) for node in partition[i]))
    
    nbrs = {u: {v: data.get("weight", 1) for v, data in G[u].items() if v != u} for u in G}
    rand_nodes = list(G.nodes)
    
    nb_moves = 1
    iteration = 0
    total_moves = 0
    recent_moves = []
    oscillation_threshold = 3
    
    while nb_moves > 0 and iteration < max_iterations:
        iteration += 1
        nb_moves = 0
        
        for u in rand_nodes:
            best_mod = 0
            best_com = node2com[u]
            
            weights2com = defaultdict(float)
            for nbr, wt in nbrs.get(u, {}).items():
                weights2com[node2com[nbr]] += wt
            
            degree = degrees.get(u, 0)
            if best_com < len(Stot):
                Stot[best_com] -= degree
                remove_cost = -weights2com.get(best_com, 0) / m + (Stot[best_com] * degree) / (2 * m**2)
            else:
                remove_cost = 0
            
            for nbr_com, wt in weights2com.items():
                if nbr_com < len(Stot):
                    gain = remove_cost + wt / m - (Stot[nbr_com] * degree) / (2 * m**2)
                    if gain > best_mod:
                        best_mod = gain
                        best_com = nbr_com
            
            if best_com < len(Stot):
                Stot[best_com] += degree
            
            if best_com != node2com[u]:
                com = G.nodes[u].get("nodes", {u})
                if not isinstance(com, set):
                    com = {com}
                
                partition[node2com[u]].difference_update(com)
                inner_partition[node2com[u]].remove(u)
                
                if best_com < len(partition):
                    partition[best_com].update(com)
                    inner_partition[best_com].add(u)
                
                nb_moves += 1
                total_moves += 1
                node2com[u] = best_com
        
        old_partition = partition.copy()
        partition = list(filter(len, partition))
        inner_partition = list(filter(len, inner_partition))
        
        if len(old_partition) != len(partition):
            new_node2com = {}
            for i, community in enumerate(partition):
                for node in community:
                    new_node2com[node] = i
            node2com = new_node2com
            
            new_Stot = []
            for i in range(len(partition)):
                new_Stot.append(sum(degrees.get(node, 0) for node in partition[i]))
            Stot = new_Stot
        
        recent_moves.append(nb_moves)
        if len(recent_moves) > oscillation_threshold:
            recent_moves.pop(0)
            if len(set(recent_moves)) == 1 and recent_moves[0] > 0:
                break
    
    return partition, inner_partition, total_moves > 0

def modularity_fixed(G, communities, weight="weight"):
    """计算给定社区划分的模块度"""
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
    
    def val(u, v):
        try:
            w = G[u][v].get(weight, 1)
        except KeyError:
            w = 0
        if u == v and not directed:
            w *= 2
        return w - in_degree.get(u, 0) * out_degree.get(v, 0) * norm
    
    Q = 0
    for c in communities:
        for u in c:
            for v in c:
                Q += val(u, v)
    
    Q = Q * norm
    
    print(f"  - 社区数量: {len(communities)}")
    print(f"  - 最终模块度: {Q:.6f}")
    
    return Q

def calculate_average_neighbor_degree(G, node):
    """计算节点的邻居平均度数"""
    neighbors = list(G.neighbors(node))
    if not neighbors:
        return 0.0
    
    neighbor_degrees = []
    for neighbor in neighbors:
        deg_val = len(list(G.neighbors(neighbor)))
        neighbor_degrees.append(deg_val)
    
    return sum(neighbor_degrees) / len(neighbor_degrees)

def create_ego_network_fixed(G, node, radius=2):
    """使用修复版ego_graph创建真正的双向二跳邻居网络"""
    print(f"  - 开始创建真正的双向二跳邻居网络...")
    
    # 使用修复版的ego_graph函数，设置undirected=True以获取双向边
    ego_graph = ego_graph_fixed(G, node, radius=radius, center=True, undirected=True)
    
    if ego_graph:
        print(f"  - 双向ego_graph创建成功: {ego_graph.number_of_nodes()} 节点, {ego_graph.number_of_edges()} 边")
        
        # 验证中心节点的邻居情况
        if node in ego_graph:
            # 对于有向图，计算入邻居和出邻居
            if G.is_directed():
                in_neighbors = []
                out_neighbors = []
                
                # 在原图中查找中心节点的真实邻居
                for u in G.nodes:
                    if G.has_edge(u, node):  # u指向node（入邻居）
                        in_neighbors.append(u)
                    if G.has_edge(node, u):  # node指向u（出邻居）
                        out_neighbors.append(u)
                
                # 过滤：只统计在ego_graph中的邻居
                in_neighbors_in_ego = [n for n in in_neighbors if n in ego_graph]
                out_neighbors_in_ego = [n for n in out_neighbors if n in ego_graph]
                
                print(f"  - 中心节点 {node}: 入邻居(粉丝) {len(in_neighbors_in_ego)} 个, 出邻居(关注) {len(out_neighbors_in_ego)} 个")
            else:
                neighbors = list(ego_graph.neighbors(node))
                print(f"  - 中心节点 {node}: 邻居 {len(neighbors)} 个")
    
    return ego_graph

def calculate_network_metrics_selected(ego_graph, center_node, selected_metrics, global_graph, celebrity_users, user_categories):
    """🔥 修改版：计算网络指标，包含全图度数、明星用户标识和用户类别"""
    metrics = {}
    
    # 基本网络信息
    metrics['node_count'] = ego_graph.number_of_nodes()
    metrics['edge_count'] = ego_graph.number_of_edges()
    metrics['center_node'] = center_node
    
    # 计算用户在全图中的度数信息
    global_out_degree, global_in_degree, global_total_degree = calculate_global_degrees(global_graph, center_node)
    metrics['global_out_degree'] = global_out_degree
    metrics['global_in_degree'] = global_in_degree
    metrics['global_total_degree'] = global_total_degree
    
    # 判断是否为明星用户
    is_celebrity = center_node in celebrity_users
    metrics['is_celebrity'] = is_celebrity
    
    # 🔥 新增：获取用户类别信息
    user_category = user_categories.get(center_node, 'Unknown')
    metrics['user_category'] = user_category
    
    print(f"  - 全图度数信息: 出度 {global_out_degree}, 入度 {global_in_degree}, 总度数 {global_total_degree}")
    print(f"  - 明星用户标识: {'是' if is_celebrity else '否'}")
    print(f"  - 用户类别: {user_category}")
    
    # 根据选择计算指标
    for metric_num in selected_metrics:
        start_time = datetime.now()
        try:
            if metric_num == 1:  # 密度
                value = eg.density(ego_graph)
                metrics['density'] = value
                elapsed = datetime.now() - start_time
                print(f"  - density 计算完成: {value:.6f}, 耗时: {elapsed}")
                
            elif metric_num == 2:  # 聚类系数
                value = eg_f.clustering(ego_graph, center_node)
                metrics['clustering_coefficient'] = value
                elapsed = datetime.now() - start_time
                print(f"  - clustering_coefficient 计算完成: {value:.6f}, 耗时: {elapsed}")
                
            elif metric_num == 3:  # 邻居平均度
                value = calculate_average_neighbor_degree(ego_graph, center_node)
                metrics['average_nearest_neighbor_degree'] = value
                elapsed = datetime.now() - start_time
                print(f"  - average_nearest_neighbor_degree 计算完成: {value:.6f}, 耗时: {elapsed}")
                
            elif metric_num == 4:  # 介数中心性
                value, bc_time = calculate_betweenness_centrality(ego_graph, center_node)
                metrics['betweenness_centrality'] = value
                print(f"  - betweenness_centrality 计算完成: {value:.6f}, 耗时: {bc_time}")
                
            elif metric_num == 5:  # 谱半径
                value = calculate_spectral_radius(ego_graph)
                metrics['spectral_radius'] = value
                elapsed = datetime.now() - start_time
                print(f"  - spectral_radius 计算完成: {value:.6f}, 耗时: {elapsed}")
                
            elif metric_num == 6:  # 模块度
                value = calculate_modularity(ego_graph)
                metrics['modularity'] = value
                elapsed = datetime.now() - start_time
                print(f"  - modularity 计算完成: {value:.6f}, 耗时: {elapsed}")
                
        except Exception as e:
            metric_names = {1: 'density', 2: 'clustering_coefficient', 3: 'average_nearest_neighbor_degree',
                          4: 'betweenness_centrality', 5: 'spectral_radius', 6: 'modularity'}
            metric_name = metric_names.get(metric_num, f'metric_{metric_num}')
            print(f"  - ❌ {metric_name} 计算失败: {e}")
            metrics[metric_name] = 0.0
    
    return metrics

def save_all_metrics_to_jsonl(all_metrics_data, output_path):
    """将所有网络指标保存到JSONL文件（完整重写）"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for user_id, metrics in all_metrics_data.items():
            record = {"user_id": user_id, "network_metrics": metrics}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"所有网络指标数据已保存到: {output_path}")

def save_all_ego_networks_info(all_ego_networks_info, output_path):
    """将所有二跳邻居网络信息保存到JSONL文件（完整重写）"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for user_id, info in all_ego_networks_info.items():
            record = {"user_id": user_id, "ego_network_info": info}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"所有二跳邻居网络信息已保存到: {output_path}")

def metrics_to_dataframe(metrics_data):
    """将指标数据转换为DataFrame格式"""
    records = []
    for user_id, metrics in metrics_data.items():
        record = {"user_id": user_id}
        record.update(metrics)
        records.append(record)
    return pd.DataFrame(records)

def main():
    """主函数"""
    # 设置信号处理器，优雅处理Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    start_time = datetime.now()
    print(f"开始分析时间: {start_time}")
    print("使用修复版EasyGraph ego_graph，真正支持双向边")
    print("支持交互式选择网络指标")
    print("支持断点续传功能")
    print("🔥 已修复介数中心性计算和Ctrl+C处理")
    print("🔥 新增全图度数信息：出度、入度、总度数")
    print("🔥 新增明星用户标识：基于high_fans_users.csv")
    print("🔥 新增用户类别信息：A/B/C类标识")
    print("🔥 新增双重影响力指标：支持avg_popularity_of_all")
    
    try:
        # 交互式选择指标
        selected_metrics = get_user_selected_metrics()
        if not selected_metrics:
            print("❌ 未选择任何指标，程序退出")
            return
        
        # 显示最终选择
        metric_names = {
            1: "密度", 2: "聚类系数", 3: "邻居平均度",
            4: "介数中心性", 5: "谱半径", 6: "模块度"
        }
        print(f"\n✅ 将计算以下 {len(selected_metrics)} 个指标:")
        for num in selected_metrics:
            print(f"   - {metric_names[num]}")
        print(f"✅ 同时记录：出度、入度、总度数、二跳网络节点数、二跳网络边数、是否明星用户、用户类别")
        
        # 设置路径
        base_dir = 'C:/Tengfei/data/data/domain_network3/user_3855570307'
        edges_path = os.path.join(base_dir, 'edges.csv')
        popularity_path = os.path.join(base_dir, 'popularity.csv')
        output_dir = f'C:/Tengfei/data/results/user_3855570307_metrics'
        metrics_output = os.path.join(output_dir, 'network_metrics.jsonl')
        ego_networks_output = os.path.join(output_dir, 'ego_networks_info.jsonl')
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 检查输入文件
        if not os.path.exists(edges_path):
            print(f"❌ 未找到edges.csv文件: {edges_path}")
            return
        
        if not os.path.exists(popularity_path):
            print(f"❌ 未找到popularity.csv文件: {popularity_path}")
            return
        
        # 加载明星用户列表
        celebrity_users = load_celebrity_users(base_dir)
        
        # 🔥 新增：加载用户类别信息
        user_categories = load_user_categories(base_dir)
        
        print("正在加载网络数据...")
        edges_df = pd.read_csv(edges_path)
        popularity_df = pd.read_csv(popularity_path)
        
        # 预处理：规范化ID
        print("正在规范化用户ID...")
        edges_df['source'] = edges_df['source'].apply(normalize_id)
        edges_df['target'] = edges_df['target'].apply(normalize_id)
        popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
        
        # 🔥 新增：检查是否有avg_popularity_of_all列
        has_total_popularity = 'avg_popularity_of_all' in popularity_df.columns
        if has_total_popularity:
            print(f"✅ 检测到总体影响力列 (avg_popularity_of_all)")
            non_zero_total = (popularity_df['avg_popularity_of_all'] > 0).sum()
            print(f"   有 {non_zero_total} 个用户具有非零总体影响力")
        else:
            print(f"⚠️ 未检测到总体影响力列，请先运行fetch3_helper.py")
        
        # 构建有向图
        print("正在构建网络...")
        G = eg.DiGraph()
        edge_count = 0
        for _, row in edges_df.iterrows():
            source = str(row['source'])
            target = str(row['target'])
            G.add_edge(source, target)
            edge_count += 1
            if edge_count % 10000 == 0:
                print(f"已加载 {edge_count} 条边")
        
        print(f"网络构建完成，包含 {G.number_of_nodes()} 个节点和 {G.number_of_edges()} 条边")
        
        # 获取需要计算的用户列表
        users_to_process = set(popularity_df['user_id'].tolist())
        users_in_graph = set(str(node) for node in G.nodes)
        valid_users = users_to_process.intersection(users_in_graph)
        
        print(f"\n=== 用户匹配统计 ===")
        print(f"Popularity文件中用户总数: {len(users_to_process)}")
        print(f"图中节点总数: {len(G.nodes)}")
        print(f"有效匹配用户数: {len(valid_users)}")
        print(f"匹配率: {len(valid_users)/len(users_to_process)*100:.2f}%")
        print(f"明星用户总数: {len(celebrity_users)}")
        print(f"用户类别信息总数: {len(user_categories)}")
        
        # 检查断点续传
        has_existing_files = os.path.exists(metrics_output) or os.path.exists(ego_networks_output)
        
        if has_existing_files:
            print(f"\n发现已有的进度文件，选择运行模式:")
            print(f"1. 断点续传（推荐）")
            print(f"2. 重新开始")
            
            while True:
                choice = input("请选择 (1/2): ").strip()
                if choice in ['1', '2']:
                    break
                print("请输入有效选项 (1/2)")
            
            resume_mode = (choice == '1')
        else:
            resume_mode = False
        
        # 初始化数据
        all_metrics_data = {}
        all_ego_networks_info = {}
        
        if resume_mode:
            print(f"\n=== 断点续传模式 ===")
            completed_users, existing_metrics, existing_ego_info = load_existing_progress(metrics_output, ego_networks_output)
            
            remaining_users = valid_users - completed_users
            print(f"总用户数: {len(valid_users)}")
            print(f"已完成用户数: {len(completed_users)}")
            print(f"剩余用户数: {len(remaining_users)}")
            
            if len(remaining_users) == 0:
                print("所有用户已处理完成")
                all_metrics_data = existing_metrics
                all_ego_networks_info = existing_ego_info
            else:
                all_metrics_data = existing_metrics.copy()
                all_ego_networks_info = existing_ego_info.copy()
                users_to_calculate = remaining_users
        else:
            print(f"\n=== 全新开始模式 ===")
            if os.path.exists(metrics_output):
                os.remove(metrics_output)
            if os.path.exists(ego_networks_output):
                os.remove(ego_networks_output)
            users_to_calculate = valid_users
        
        # 计算网络指标
        if len(users_to_calculate) > 0:
            processed_count = len(valid_users) - len(users_to_calculate)
            total_users = len(valid_users)
            
            print(f"开始计算 {len(users_to_calculate)} 个用户的网络指标...")
            batch_metrics = {}
            batch_ego_info = {}
            
            for user_id in users_to_calculate:
                processed_count += 1
                completion = processed_count / total_users * 100
                print(f"\n处理用户 {user_id} (第{processed_count}/{total_users}个, 完成{completion:.1f}%):")
                
                # 创建二跳邻居网络
                ego_start_time = datetime.now()
                ego_graph = create_ego_network_fixed(G, user_id, radius=2)
                ego_time = datetime.now() - ego_start_time
                
                if ego_graph and ego_graph.number_of_nodes() > 1:
                    print(f"  - 双向二跳邻居网络创建完成，耗时: {ego_time}")
                else:
                    print(f"  - 网络创建失败或节点数过少，跳过此用户")
                    continue
                
                # 🔥 修改：计算网络指标，传入用户类别信息
                print(f"  - 开始计算选择的网络指标...")
                metrics_start_time = datetime.now()
                metrics = calculate_network_metrics_selected(ego_graph, user_id, selected_metrics, G, celebrity_users, user_categories)
                metrics_time = datetime.now() - metrics_start_time
                print(f"  - 网络指标计算完成, 总耗时: {metrics_time}")
                
                # 保存数据
                batch_metrics[user_id] = metrics
                all_metrics_data[user_id] = metrics
                
                # 存储ego网络信息，包含用户类别
                ego_info = {
                    'node_count': ego_graph.number_of_nodes(),
                    'edge_count': ego_graph.number_of_edges(),
                    'nodes': list(ego_graph.nodes),
                    'selected_metrics': selected_metrics,
                    'global_out_degree': metrics.get('global_out_degree', 0),
                    'global_in_degree': metrics.get('global_in_degree', 0),
                    'global_total_degree': metrics.get('global_total_degree', 0),
                    'is_celebrity': metrics.get('is_celebrity', False),
                    'user_category': metrics.get('user_category', 'Unknown'),  # 🔥 新增
                    'metrics': {k: v for k, v in metrics.items() if k not in ['node_count', 'edge_count', 'center_node', 'global_out_degree', 'global_in_degree', 'global_total_degree', 'is_celebrity', 'user_category']}
                }
                batch_ego_info[user_id] = ego_info
                all_ego_networks_info[user_id] = ego_info
                
                # 每10个用户保存一次
                if len(batch_metrics) >= 10:
                    append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                    append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
                    print(f"  - 已保存 {len(batch_metrics)} 个用户的结果")
                    batch_metrics.clear()
                    batch_ego_info.clear()
            
            # 保存剩余数据
            if batch_metrics:
                append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
        
        # 生成合并数据
        print("正在生成合并数据文件...")
        metrics_df = metrics_to_dataframe(all_metrics_data)
        
        if len(metrics_df) > 0:
            # 🔥 修改：合并两种影响力指标
            if has_total_popularity:
                # 合并两种影响力指标
                merged_df = pd.merge(metrics_df, popularity_df[['user_id', 'avg_popularity', 'avg_popularity_of_all']], 
                                    on="user_id", how="inner")
                print(f"✅ 已合并两种影响力指标: avg_popularity (最新10条) 和 avg_popularity_of_all (总体)")
            else:
                # 只有一种影响力指标
                merged_df = pd.merge(metrics_df, popularity_df[['user_id', 'avg_popularity']], 
                                    on="user_id", how="inner")
                print(f"⚠️ 只有一种影响力指标: avg_popularity (最新10条)")
            
            # 保存合并数据
            merged_output = os.path.join(output_dir, 'merged_metrics_popularity.csv')
            merged_df.to_csv(merged_output, index=False)
            print(f"合并数据已保存到: {merged_output}")
            print(f"包含 {len(merged_df)} 行数据")
            
            # 显示计算的指标
            calculated_metrics = [col for col in merged_df.columns if col not in ['user_id', 'center_node', 'avg_popularity', 'avg_popularity_of_all']]
            print(f"\n✅ 已计算的指标和信息: {calculated_metrics}")
            
            # 显示度数统计
            if 'global_out_degree' in merged_df.columns:
                print(f"\n📊 度数统计:")
                print(f"   平均出度: {merged_df['global_out_degree'].mean():.2f}")
                print(f"   平均入度: {merged_df['global_in_degree'].mean():.2f}")
                print(f"   平均总度数: {merged_df['global_total_degree'].mean():.2f}")
            
            # 显示明星用户统计
            if 'is_celebrity' in merged_df.columns:
                celebrity_count = merged_df['is_celebrity'].sum()
                print(f"\n🌟 明星用户统计:")
                print(f"   明星用户数量: {celebrity_count}")
                print(f"   明星用户比例: {celebrity_count/len(merged_df)*100:.2f}%")
            
            # 🔥 新增：显示用户类别统计
            if 'user_category' in merged_df.columns:
                category_counts = merged_df['user_category'].value_counts()
                print(f"\n📋 用户类别统计:")
                for category, count in category_counts.items():
                    print(f"   {category}类用户: {count} 个 ({count/len(merged_df)*100:.1f}%)")
            
            # 🔥 新增：显示影响力对比统计
            if has_total_popularity:
                print(f"\n📊 双重影响力对比:")
                # 有效数据（非零）的统计
                valid_recent = merged_df['avg_popularity'] > 0
                valid_total = merged_df['avg_popularity_of_all'] > 0
                
                print(f"   最新10条影响力 > 0: {valid_recent.sum()} 个用户 ({valid_recent.sum()/len(merged_df)*100:.1f}%)")
                print(f"   总体影响力 > 0: {valid_total.sum()} 个用户 ({valid_total.sum()/len(merged_df)*100:.1f}%)")
                
                if valid_recent.any():
                    print(f"   最新10条平均值: {merged_df.loc[valid_recent, 'avg_popularity'].mean():.2f}")
                if valid_total.any():
                    print(f"   总体平均值: {merged_df.loc[valid_total, 'avg_popularity_of_all'].mean():.2f}")
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n总耗时: {duration}")
        print(f"生成的文件:")
        print(f"  - 网络指标: {metrics_output}")
        print(f"  - 邻居网络信息: {ego_networks_output}")
        print(f"  - 合并数据: {merged_output}")
        print(f"\n🔥 新增功能已启用：")
        print(f"   ✅ 每个用户的出度、入度、总度数已记录")
        print(f"   ✅ 每个用户的明星用户标识已记录")
        print(f"   ✅ 每个用户的类别信息（A/B/C）已记录")
        if has_total_popularity:
            print(f"   ✅ 双重影响力指标：最新10条 + 总体平均")
            print(f"   ✅ 总计14个信息：用户ID + 6大网络指标 + 7大基础信息")
        else:
            print(f"   ⚠️ 单一影响力指标：仅最新10条")
            print(f"   ✅ 总计13个信息：用户ID + 6大网络指标 + 6大基础信息")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️ 程序被用户中断 (Ctrl+C)")
        print(f"📁 数据已保存到进度文件，可以稍后继续运行")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 程序发生异常: {e}")
        print(f"📁 请检查数据文件和路径配置")
        sys.exit(1)

if __name__ == "__main__":
    main()