import os
import json
import pandas as pd
import numpy as np
import easygraph as eg
import easygraph.functions as eg_f
from scipy import linalg
from datetime import datetime
from collections import defaultdict, deque

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        id_str = str(id_value).strip()
        if id_str == '-2147483648':
            return id_str
        return str(int(float(id_str)))
    except:
        return str(id_value).strip()

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

def calculate_spectral_radius(G):
    """计算图的谱半径（最大特征值的绝对值）"""
    adj_matrix = eg.to_numpy_array(G)
    eigenvalues = linalg.eigvals(adj_matrix)
    return float(np.max(np.abs(eigenvalues)))

def calculate_modularity(G):
    """计算图的模块度"""
    partition, modularity_value = louvain_communities_fixed(G, threshold=0.001)
    return modularity_value

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

def calculate_network_metrics_five_indicators(ego_graph, center_node):
    """计算网络的五个指标（去掉介数中心性）"""
    metrics = {}
    
    # 基本网络信息
    metrics['node_count'] = ego_graph.number_of_nodes()
    metrics['edge_count'] = ego_graph.number_of_edges()
    metrics['center_node'] = center_node
    
    # 1. 密度
    metrics['density'] = eg.density(ego_graph)
    print(f"  - 密度计算完成: {metrics['density']:.6f}")
    
    # 2. 聚类系数
    metrics['clustering_coefficient'] = eg_f.clustering(ego_graph, center_node)
    print(f"  - 聚类系数计算完成: {metrics['clustering_coefficient']:.6f}")
    
    # 3. 邻居平均度
    metrics['average_nearest_neighbor_degree'] = calculate_average_neighbor_degree(ego_graph, center_node)
    print(f"  - 邻居平均度计算完成: {metrics['average_nearest_neighbor_degree']:.6f}")
    
    # 跳过介数中心性（原第4个指标）
    
    # 4. 谱半径（原第5个指标）
    sr_start = datetime.now()
    metrics['spectral_radius'] = calculate_spectral_radius(ego_graph)
    sr_time = datetime.now() - sr_start
    print(f"  - 谱半径计算完成: {metrics['spectral_radius']:.6f}, 耗时: {sr_time}")
    
    # 5. 模块度（原第6个指标）
    mod_start = datetime.now()
    metrics['modularity'] = calculate_modularity(ego_graph)
    mod_time = datetime.now() - mod_start
    print(f"  - 模块度计算完成: {metrics['modularity']:.6f}, 耗时: {mod_time}")
    
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
    start_time = datetime.now()
    print(f"开始分析时间: {start_time}")
    print("使用修复版EasyGraph ego_graph，真正支持双向边")
    print("计算五大网络指标（去掉介数中心性以提升效率）")
    print("支持断点续传功能")
    
    # 设置路径
    base_dir = 'data/domain_networks/merged_network'
    edges_path = os.path.join(base_dir, 'edges.csv')
    popularity_path = os.path.join(base_dir, 'popularity.csv')
    output_dir = 'results/merged_network_result3'  # 新的输出目录
    metrics_output = os.path.join(output_dir, 'network_metrics.jsonl')
    ego_networks_output = os.path.join(output_dir, 'ego_networks_info.jsonl')
    
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 检查是否已有进度文件，提供选项
    has_existing_files = os.path.exists(metrics_output) or os.path.exists(ego_networks_output)
    
    if has_existing_files:
        print(f"\n发现已有的进度文件:")
        if os.path.exists(metrics_output):
            print(f"  - 网络指标文件: {metrics_output}")
        if os.path.exists(ego_networks_output):
            print(f"  - ego网络信息文件: {ego_networks_output}")
        
        print(f"\n请选择运行模式:")
        print(f"1. 断点续传（推荐）- 从上次中断的地方继续")
        print(f"2. 重新开始 - 删除已有进度，从头开始")
        print(f"3. 仅生成合并文件 - 使用现有数据生成merged_metrics_popularity.csv")
        
        while True:
            choice = input("请选择 (1/2/3): ").strip()
            if choice in ['1', '2', '3']:
                break
            print("请输入有效选项 (1/2/3)")
        
        if choice == '1':
            resume_mode = True
            recalculate = True
        elif choice == '2':
            resume_mode = False
            recalculate = True
        else:  # choice == '3'
            resume_mode = False
            recalculate = False
    else:
        resume_mode = False
        recalculate = True
    
    # 加载数据、构建图并计算网络指标
    if recalculate:
        print("正在加载网络数据...")
        edges_df = pd.read_csv(edges_path)
        popularity_df = pd.read_csv(popularity_path)
        
        # 预处理：规范化ID
        print("正在规范化用户ID...")
        source_id_map = {row['source']: normalize_id(row['source']) for _, row in edges_df.iterrows()}
        target_id_map = {row['target']: normalize_id(row['target']) for _, row in edges_df.iterrows()}
        edges_df['source'] = edges_df['source'].map(source_id_map)
        edges_df['target'] = edges_df['target'].map(target_id_map)
        
        popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
        
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
        
        # 加载已有进度（如果是断点续传模式）
        if resume_mode:
            print(f"\n=== 断点续传模式 ===")
            completed_users, existing_metrics, existing_ego_info = load_existing_progress(metrics_output, ego_networks_output)
            
            # 计算剩余用户
            remaining_users = valid_users - completed_users
            print(f"总用户数: {len(valid_users)}")
            print(f"已完成用户数: {len(completed_users)}")
            print(f"剩余用户数: {len(remaining_users)}")
            print(f"已完成进度: {len(completed_users)/len(valid_users)*100:.1f}%")
            
            if len(remaining_users) == 0:
                print("所有用户已处理完成，直接进入数据合并阶段")
                # 使用已有数据
                all_metrics_data = existing_metrics
                all_ego_networks_info = existing_ego_info
            else:
                print(f"将继续处理剩余的 {len(remaining_users)} 个用户")
                # 初始化总数据为已有数据
                all_metrics_data = existing_metrics.copy()
                all_ego_networks_info = existing_ego_info.copy()
                
                # 处理剩余用户
                processed_count = len(completed_users)  # 从已完成数量开始
                total_users = len(valid_users)
                
                print(f"开始处理剩余用户...")
                loop_start_time = datetime.now()
                batch_metrics = {}
                batch_ego_info = {}
                
                for user_id in remaining_users:
                    processed_count += 1
                    completion = processed_count / total_users * 100
                    print(f"\n处理用户 {user_id} (第{processed_count}/{total_users}个, 完成{completion:.1f}%):")
                    
                    # 创建用户的二跳邻居网络
                    ego_start_time = datetime.now()
                    ego_graph = create_ego_network_fixed(G, user_id, radius=2)
                    ego_time = datetime.now() - ego_start_time
                    
                    if ego_graph and ego_graph.number_of_nodes() > 1:
                        print(f"  - 双向二跳邻居网络创建完成: {ego_graph.number_of_nodes()} 节点, {ego_graph.number_of_edges()} 边")
                        print(f"  - 耗时: {ego_time}")
                    else:
                        print(f"  - 双向二跳邻居网络创建失败或节点数过少，跳过此用户")
                        continue
                    
                    # 计算网络指标
                    print(f"  - 开始计算五大网络指标...")
                    metrics_start_time = datetime.now()
                    metrics = calculate_network_metrics_five_indicators(ego_graph, user_id)
                    metrics_time = datetime.now() - metrics_start_time
                    print(f"  - 五大网络指标计算完成, 总耗时: {metrics_time}")
                    
                    # 添加到批处理数据
                    batch_metrics[user_id] = metrics
                    all_metrics_data[user_id] = metrics
                    
                    # 存储二跳邻居网络信息
                    ego_info = {
                        'node_count': ego_graph.number_of_nodes(),
                        'edge_count': ego_graph.number_of_edges(),
                        'nodes': list(ego_graph.nodes),
                        'metrics': {
                            'density': metrics['density'],
                            'clustering_coefficient': metrics['clustering_coefficient'],
                            'average_nearest_neighbor_degree': metrics['average_nearest_neighbor_degree'],
                            'spectral_radius': metrics['spectral_radius'],
                            'modularity': metrics['modularity']
                        }
                    }
                    batch_ego_info[user_id] = ego_info
                    all_ego_networks_info[user_id] = ego_info
                    
                    # 每处理10个用户追加保存一次（避免频繁IO）
                    if len(batch_metrics) >= 10:
                        append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                        append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
                        print(f"  - 已追加保存 {len(batch_metrics)} 个用户的结果")
                        batch_metrics.clear()
                        batch_ego_info.clear()
                
                # 保存剩余的批处理数据
                if batch_metrics:
                    append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                    append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
                    print(f"  - 已追加保存最后 {len(batch_metrics)} 个用户的结果")
                
                loop_duration = datetime.now() - loop_start_time
                print(f"\n剩余用户处理完成，耗时: {loop_duration}")
                print(f"平均每个用户处理时间: {loop_duration.total_seconds() / max(1, len(remaining_users)):.2f} 秒")
        else:
            # 全新开始模式
            print(f"\n=== 全新开始模式 ===")
            all_metrics_data = {}
            all_ego_networks_info = {}
            
            # 删除已有文件
            if os.path.exists(metrics_output):
                os.remove(metrics_output)
                print(f"已删除旧的网络指标文件")
            if os.path.exists(ego_networks_output):
                os.remove(ego_networks_output)
                print(f"已删除旧的ego网络信息文件")
            
            # 处理所有用户
            processed_count = 0
            total_users = len(valid_users)
            print(f"开始计算 {total_users} 个用户的五大网络指标...")
            loop_start_time = datetime.now()
            batch_metrics = {}
            batch_ego_info = {}
            
            for user_id in valid_users:
                processed_count += 1
                completion = processed_count / total_users * 100
                print(f"\n处理用户 {user_id} (第{processed_count}/{total_users}个, 完成{completion:.1f}%):")
                
                # 创建用户的二跳邻居网络
                ego_start_time = datetime.now()
                ego_graph = create_ego_network_fixed(G, user_id, radius=2)
                ego_time = datetime.now() - ego_start_time
                
                if ego_graph and ego_graph.number_of_nodes() > 1:
                    print(f"  - 双向二跳邻居网络创建完成: {ego_graph.number_of_nodes()} 节点, {ego_graph.number_of_edges()} 边")
                    print(f"  - 耗时: {ego_time}")
                else:
                    print(f"  - 双向二跳邻居网络创建失败或节点数过少，跳过此用户")
                    continue
                
                # 计算网络指标
                print(f"  - 开始计算五大网络指标...")
                metrics_start_time = datetime.now()
                metrics = calculate_network_metrics_five_indicators(ego_graph, user_id)
                metrics_time = datetime.now() - metrics_start_time
                print(f"  - 五大网络指标计算完成, 总耗时: {metrics_time}")
                
                # 添加到数据
                batch_metrics[user_id] = metrics
                all_metrics_data[user_id] = metrics
                
                # 存储二跳邻居网络信息
                ego_info = {
                    'node_count': ego_graph.number_of_nodes(),
                    'edge_count': ego_graph.number_of_edges(),
                    'nodes': list(ego_graph.nodes),
                    'metrics': {
                        'density': metrics['density'],
                        'clustering_coefficient': metrics['clustering_coefficient'],
                        'average_nearest_neighbor_degree': metrics['average_nearest_neighbor_degree'],
                        'spectral_radius': metrics['spectral_radius'],
                        'modularity': metrics['modularity']
                    }
                }
                batch_ego_info[user_id] = ego_info
                all_ego_networks_info[user_id] = ego_info
                
                # 每处理10个用户追加保存一次
                if len(batch_metrics) >= 10:
                    append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                    append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
                    print(f"  - 已追加保存 {len(batch_metrics)} 个用户的结果")
                    batch_metrics.clear()
                    batch_ego_info.clear()
            
            # 保存剩余的批处理数据
            if batch_metrics:
                append_to_jsonl(batch_metrics, metrics_output, is_metrics=True)
                append_to_jsonl(batch_ego_info, ego_networks_output, is_metrics=False)
                print(f"  - 已追加保存最后 {len(batch_metrics)} 个用户的结果")
            
            loop_duration = datetime.now() - loop_start_time
            print(f"\n用户处理循环完成，总耗时: {loop_duration}")
            print(f"平均每个用户处理时间: {loop_duration.total_seconds() / max(1, len(all_metrics_data)):.2f} 秒")
        
        print(f"已计算 {len(all_metrics_data)} 个用户的五大网络指标")
    else:
        # 仅生成合并文件模式 - 加载已有数据
        print("加载已有的网络指标...")
        completed_users, all_metrics_data, all_ego_networks_info = load_existing_progress(metrics_output, ego_networks_output)
        
        popularity_df = pd.read_csv(popularity_path)
        popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
        print(f"已加载 {len(all_metrics_data)} 个用户的五大网络指标")
    
    # 将指标转换为DataFrame格式并保存merged_metrics_popularity.csv
    print("正在生成合并数据文件...")
    metrics_df = metrics_to_dataframe(all_metrics_data)
    
    if len(metrics_df) > 0:
        # 规范化ID并合并数据
        metrics_df['user_id'] = metrics_df['user_id'].apply(normalize_id)
        popularity_df['user_id'] = popularity_df['user_id'].apply(normalize_id)
        
        merged_df = pd.merge(metrics_df, popularity_df[['user_id', 'avg_popularity']], 
                            on="user_id", how="inner")
        
        # 保存合并后的数据
        merged_output = os.path.join(output_dir, 'merged_metrics_popularity.csv')
        merged_df.to_csv(merged_output, index=False)
        print(f"合并后的数据已保存到: {merged_output}")
        print(f"合并后的数据包含 {len(merged_df)} 行")
        
        # 打印数据字段信息
        print(f"\n=== 生成的数据字段 ===")
        print(f"基本字段: user_id, node_count, edge_count, center_node, avg_popularity")
        print(f"五大网络指标: density, clustering_coefficient, average_nearest_neighbor_degree, spectral_radius, modularity")
        print(f"总字段数: {len(merged_df.columns)}")
    else:
        print("错误：没有有效的网络指标数据")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n数据生成完成，总耗时: {duration}")
    print(f"生成的文件:")
    print(f"  - 网络指标: {metrics_output}")
    print(f"  - 邻居网络信息: {ego_networks_output}")
    print(f"  - 合并数据: {merged_output}")
    print(f"\n现在可以使用correlation_analysis目录下的脚本进行相关性分析")

if __name__ == "__main__":
    main()