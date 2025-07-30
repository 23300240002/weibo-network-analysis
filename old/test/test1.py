import time
import json
import os
import easygraph as eg
import easygraph.functions as eg_f
from collections import defaultdict, deque

# 辅助函数：计算节点与邻居社区间的权重
def _neighbor_weights(nbrs, node2com):
    """计算节点与邻居社区间的权重"""
    weights = defaultdict(float)
    for nbr, wt in nbrs.items():
        weights[node2com[nbr]] += wt
    return weights

# 修复版：增加了最大迭代次数、社区索引映射更新、振荡检测
def _one_level_test(G, m, partition, is_directed=False, max_iterations=100):
    """修复版的_one_level函数，解决索引错误和振荡问题"""
    start_time = time.time()
    print(f"开始Louvain一轮迭代，图有 {len(G.nodes)} 个节点和 {G.size()} 条边")
    
    # 初始化每个节点为单独社区
    node2com = {u: i for i, u in enumerate(G.nodes)}
    inner_partition = [{u} for u in G.nodes]
    
    # 获取节点度数和总度
    degrees = dict(G.degree(weight="weight"))
    Stot = []
    for i in G:
        Stot.append(len(G[i]))
    
    # 获取邻居信息
    nbrs = {u: {v: data["weight"] for v, data in G[u].items() if v != u} for u in G}
    rand_nodes = list(G.nodes)
    
    # 主循环
    nb_moves = 1
    iteration = 0
    total_moves = 0
    
    # 添加振荡检测变量
    recent_moves = []  # 记录最近几次迭代的移动数量
    oscillation_threshold = 3  # 连续相同移动次数视为振荡
    
    while nb_moves > 0 and iteration < max_iterations:
        iter_start = time.time()
        iteration += 1
        nb_moves = 0
        
        print(f"  迭代 {iteration} 开始处理 {len(rand_nodes)} 个节点...")
        
        # 处理每个节点
        for node_idx, u in enumerate(rand_nodes):
            best_mod = 0
            best_com = node2com[u]
            
            # 计算到邻居社区的权重
            weights2com = _neighbor_weights(nbrs[u], node2com)
            
            # 移除节点的成本
            degree = degrees[u]
            Stot[best_com] -= degree
            remove_cost = -weights2com.get(best_com, 0) / m + (Stot[best_com] * degree) / (2 * m**2)
            
            # 计算最佳社区
            for nbr_com, wt in weights2com.items():
                gain = remove_cost + wt / m - (Stot[nbr_com] * degree) / (2 * m**2)
                if gain > best_mod:
                    best_mod = gain
                    best_com = nbr_com
            
            # 恢复Stot值
            Stot[best_com] += degree
            
            # 如果找到更好的社区，执行移动
            if best_com != node2com[u]:
                com = G.nodes[u].get("nodes", {u})
                partition[node2com[u]].difference_update(com)
                inner_partition[node2com[u]].remove(u)
                partition[best_com].update(com)
                inner_partition[best_com].add(u)
                nb_moves += 1
                total_moves += 1
                node2com[u] = best_com
            
            # 显示进度
            if (node_idx+1) % 50 == 0 or node_idx == len(rand_nodes)-1:
                print(f"    处理到节点 {node_idx+1}/{len(rand_nodes)} ({(node_idx+1)/len(rand_nodes)*100:.1f}%)")
        
        # 清理空社区
        old_partition = partition.copy()
        partition = list(filter(len, partition))
        inner_partition = list(filter(len, inner_partition))
        
        # 修复：重建node2com映射
        print(f"  过滤前社区数: {len(old_partition)}, 过滤后: {len(partition)}")
        if len(old_partition) != len(partition):
            print(f"  检测到社区数量变化，正在重建节点-社区映射...")
            new_node2com = {}
            for i, community in enumerate(partition):
                for node in community:
                    new_node2com[node] = i
            node2com = new_node2com
            print(f"  节点-社区映射已更新")
        
        iter_end = time.time()
        print(f"  迭代 {iteration} 完成: 移动了 {nb_moves} 个节点, 耗时: {iter_end - iter_start:.2f}秒")
        print(f"  当前社区数: {len(partition)}")
        
        # 振荡检测
        recent_moves.append(nb_moves)
        if len(recent_moves) > oscillation_threshold:
            recent_moves.pop(0)  # 保持固定长度
            
            # 检查最近几次迭代是否移动数量相同（振荡迹象）
            if len(set(recent_moves)) == 1 and recent_moves[0] > 0:
                print(f"  检测到振荡模式: 连续{oscillation_threshold}次迭代移动相同数量的节点")
                print(f"  强制终止迭代以避免无限循环")
                break
        
        # 如果迭代次数过多，发出警告
        if iteration > 50 and nb_moves > 0:
            print(f"  警告: 迭代次数已达{iteration}次，可能存在振荡")
    
    # 超出最大迭代次数时的处理
    if iteration >= max_iterations and nb_moves > 0:
        print(f"  已达到最大迭代次数({max_iterations})，强制终止")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Louvain一轮迭代完成，总耗时: {total_time:.2f}秒")
    print(f"共进行了 {iteration} 次迭代，移动了 {total_moves} 个节点")
    print(f"最终社区数: {len(partition)}")
    
    return partition, inner_partition, nb_moves > 0

# 模块度计算函数保持不变
def modularity_test(G, communities, weight="weight"):
    """测试版本的modularity函数，添加时间统计"""
    start_time = time.time()
    print(f"开始计算模块度，图有 {len(G.nodes)} 个节点和 {G.size()} 条边")
    print(f"社区数量: {len(communities)}")
    
    # 检查社区规模
    community_sizes = [len(c) for c in communities]
    print(f"社区大小: 最小={min(community_sizes)}, 最大={max(community_sizes)}, 平均={sum(community_sizes)/len(communities):.1f}")
    
    # 记录每个社区对的计算时间
    if not isinstance(communities, list):
        communities = list(communities)

    directed = G.is_directed()
    m = G.size(weight=weight)
    
    if directed:
        out_degree = dict(G.out_degree(weight=weight))
        in_degree = dict(G.in_degree(weight=weight))
        norm = 1 / m
    else:
        out_degree = dict(G.degree(weight=weight))
        in_degree = out_degree
        norm = 1 / (2 * m)
    
    # 记录val函数调用时间
    val_calls = 0
    val_time = 0
    
    def val(u, v):
        nonlocal val_calls, val_time
        val_calls += 1
        val_start = time.time()
        
        try:
            w = G[u][v].get(weight, 1)
        except KeyError:
            w = 0
        # Double count self-loops if the graph is undirected.
        if u == v and not directed:
            w *= 2
        
        result = w - in_degree[u] * out_degree[v] * norm
        
        val_time += time.time() - val_start
        return result
    
    # 分社区计算模块度
    community_times = []
    total_pairs = 0
    
    for i, community in enumerate(communities):
        comm_start = time.time()
        community_size = len(community)
        pairs = community_size * community_size  # 所有可能的节点对
        total_pairs += pairs
        
        print(f"  处理社区 {i+1}/{len(communities)}: {community_size} 节点，需计算 {pairs} 对")
        
        # 测量单个社区的计算时间
        pair_start = time.time()
        comm_q = sum(val(u, v) for u, v in [(u, v) for u in community for v in community])
        pair_time = time.time() - pair_start
        
        comm_end = time.time()
        community_times.append(comm_end - comm_start)
        
        # 只显示大社区的详细信息
        if community_size > 50 or i < 3:
            print(f"    社区 {i+1} 计算耗时: {pair_time:.2f}秒 (每对平均: {pair_time/max(1,pairs)*1000:.3f}毫秒)")
    
    # 应用归一化系数
    Q = sum(val(u, v) for c in communities for u, v in [(u, v) for u in c for v in c])
    Q = Q * norm
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"模块度计算完成: Q={Q:.6f}")
    print(f"总计算 {val_calls} 次节点对评估，总耗时: {total_time:.2f}秒")
    print(f"平均每对计算时间: {total_time/max(1,total_pairs)*1000:.3f}毫秒")
    
    if community_times:
        avg_comm_time = sum(community_times) / len(community_times)
        max_comm_time = max(community_times)
        print(f"社区计算统计: 平均耗时={avg_comm_time:.2f}秒, 最长耗时={max_comm_time:.2f}秒")
    
    return Q

# 修复版：添加最大层数和收敛阈值
def louvain_communities_test(G, weight="weight", threshold=0.001, max_levels=10):
    """修复版的louvain_communities函数，添加最大层数限制和提高收敛阈值"""
    overall_start = time.time()
    print(f"开始Louvain社区检测，阈值={threshold}，最大层数={max_levels}")
    
    # 初始化，每个节点作为一个社区
    partition = [{u} for u in G.nodes]
    m = G.size(weight="weight")
    is_directed = G.is_directed()
    
    # 计算初始模块度
    print("\n--- 计算初始模块度 ---")
    initial_mod = modularity_test(G, partition)
    
    # 第一轮迭代
    level = 1
    print(f"\n--- 开始第{level}轮迭代 ---")
    partition, inner_partition, improvement = _one_level_test(G, m, partition, is_directed)
    
    print("\n--- 计算第一轮后模块度 ---")
    new_mod = modularity_test(G, partition)
    mod_gain = new_mod - initial_mod
    print(f"第{level}轮后模块度: {new_mod:.6f} (增加了 {mod_gain:.6f})")
    
    # 继续迭代直到没有改进或达到最大层数
    while improvement and level < max_levels:
        level += 1
        
        # 构建新图 (简化版本)
        print(f"\n--- 生成第{level}轮压缩图 ---")
        gen_start = time.time()
        G_new = G.__class__()
        node2com = {}
        
        for i, part in enumerate(partition):
            G_new.add_node(i)
            for node in part:
                node2com[node] = i
        
        # 添加边
        edge_count = 0
        for u, v, wt in G.edges:
            if u in node2com and v in node2com:  # 确保节点在映射中
                com1 = node2com[u]
                com2 = node2com[v]
                edge_weight = wt.get(weight, 1)
                
                # 如果边已存在，增加权重
                if G_new.has_edge(com1, com2):
                    G_new[com1][com2][weight] += edge_weight
                else:
                    G_new.add_edge(com1, com2, **{weight: edge_weight})
                    edge_count += 1
        
        gen_end = time.time()
        print(f"压缩图生成完成: {len(G_new.nodes)} 节点, {edge_count} 边, 耗时: {gen_end-gen_start:.2f}秒")
        
        # 更新图
        G = G_new
        
        # 下一轮迭代
        print(f"\n--- 开始第{level}轮迭代 ---")
        partition = [{u} for u in G.nodes]
        partition, inner_partition, improvement = _one_level_test(G, m, partition, is_directed)
        
        # 计算新模块度
        print(f"\n--- 计算第{level}轮后模块度 ---")
        cur_mod = modularity_test(G, partition)
        mod_gain = cur_mod - new_mod
        print(f"第{level}轮后模块度: {cur_mod:.6f} (增加了 {mod_gain:.6f})")
        
        # 检查是否达到阈值
        if mod_gain <= threshold:
            print(f"模块度增加 {mod_gain:.6f} 低于阈值 {threshold}，停止迭代")
            improvement = False
        else:
            new_mod = cur_mod
    
    # 超出最大层数时的处理
    if level >= max_levels and improvement:
        print(f"已达到最大层数限制({max_levels})，强制结束")
    
    overall_end = time.time()
    print(f"\nLouvain算法完成，总耗时: {overall_end-overall_start:.2f}秒")
    print(f"总共进行了 {level} 轮迭代")
    print(f"最终模块度: {new_mod:.6f}")
    
    return partition

def test_with_small_csv():
    """使用CSV文件中的少量用户数据进行测试"""
    print("=== 使用CSV数据测试修复后的模块度计算 ===\n")
    
    # 创建小型测试网络
    print("创建小型测试网络...")
    G = eg.DiGraph()
    
    # 从CSV添加少量边
    try:
        import pandas as pd
        edges_path = 'C:/Tengfei/data/data/domain_networks/merged_network/edges.csv'
        if os.path.exists(edges_path):
            edges_df = pd.read_csv(edges_path)
            # 只取前500行以便快速测试
            edges_sample = edges_df.head(500)
            
            # 添加边到图
            for _, row in edges_sample.iterrows():
                source = str(row['source'])
                target = str(row['target'])
                G.add_edge(source, target, weight=1)
                
            print(f"从CSV加载了小型测试网络: {len(G.nodes)} 节点, {G.size()} 边")
        else:
            print(f"找不到CSV文件: {edges_path}")
            # 创建一个小的随机测试网络
            for i in range(100):
                G.add_node(str(i))
            
            import random
            for _ in range(300):
                source = str(random.randint(0, 99))
                target = str(random.randint(0, 99))
                if source != target:
                    G.add_edge(source, target, weight=1)
            
            print(f"创建了随机测试网络: {len(G.nodes)} 节点, {G.size()} 边")
    except Exception as e:
        print(f"加载CSV数据时出错: {e}")
        # 创建一个小的随机测试网络
        for i in range(100):
            G.add_node(str(i))
        
        import random
        for _ in range(300):
            source = str(random.randint(0, 99))
            target = str(random.randint(0, 99))
            if source != target:
                G.add_edge(source, target, weight=1)
        
        print(f"创建了随机测试网络: {len(G.nodes)} 节点, {G.size()} 边")
    
    # 运行测试
    print("\n=== 测试修复后的Louvain社区检测 ===")
    communities = louvain_communities_test(G)
    
    print(f"\n找到 {len(communities)} 个社区")
    sizes = [len(c) for c in communities]
    print(f"社区大小: 最小={min(sizes)}, 最大={max(sizes)}, 平均={sum(sizes)/len(communities):.1f}")

if __name__ == "__main__":
    test_with_small_csv()