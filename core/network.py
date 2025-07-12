import csv
import easygraph.classes as eg
import easygraph.functions as eg_f

# 根据 CSV 文件构建社交网络
def build_social_network_from_csv(csv_path):
    G = eg.Graph()  # 使用 EasyGraph 创建一个无向图
    with open(csv_path, 'r', encoding='gb18030') as f:
        reader = csv.DictReader(f)
        for row in reader:
            author_id = row.get("MD5-作者ID", "").strip()
            parent_author_id = row.get("MD5-父微博用户ID", "").strip()
            is_retweet = (row.get("原创/转发", "").strip() == "转发")
            
            # 无论是否形成边，都先将作者和父微博用户作为节点添加到图中
            if author_id:
                G.add_node(author_id)
            if parent_author_id:
                G.add_node(parent_author_id)
            
            # 排除自环：只有当作者与父微博不相同时才添加边
            if author_id and parent_author_id and author_id != parent_author_id:
                G.add_edge(author_id, parent_author_id)
    return G

def calculate_average_neighbor_degree(G):
    degree_map = G.degree()
    total_neighbor_degree = 0
    count = 0
    for node in G.nodes:
        neighbors = list(G.neighbors(node))
        if neighbors:
            neighbor_degs = [degree_map[neigh] for neigh in neighbors]
            avg_deg = sum(neighbor_degs) / len(neighbor_degs)
            total_neighbor_degree += avg_deg
            count += 1
    if count == 0:
        return 0
    return total_neighbor_degree / count

# 计算图的各项指标
def compute_graph_metrics(G):
    metrics = {}
    metrics["节点数"] = len(G.nodes)
    metrics["边数"] = len(G.edges)
    metrics["平均度"] = eg_f.average_degree(G)
    metrics["图密度"] = eg.density(G)
    metrics["聚类系数"] = eg_f.average_clustering(G)
    return metrics

# 在整个图上计算某个节点的介数中心性
def compute_global_centrality(G, node):
    # 得到的是列表，包含每个节点的介数中心性值
    bc_list = eg_f.betweenness_centrality(G)
    nodes_list = list(G.nodes) # 节点列表
    if node in nodes_list: # 如果节点存在于图中，则返回其介数中心性值
        idx = nodes_list.index(node)
        return bc_list[idx]
    else:
        return None

# 在整个图上计算某个节点的度中心性
def compute_global_degree_centrality(G, node):
    # 得到的字典包含每个节点的度中心性值
    dc_result = eg_f.degree_centrality(G)
    return dc_result.get(node, 0)

if __name__ == "__main__":
    # 构建社交网络
    csv_file = "./data/《网络直播营销活动行为规范》7月1日实施.csv"
    social_graph = build_social_network_from_csv(csv_file)
    
    # 计算整体社交网络的统计指标
    metrics = compute_graph_metrics(social_graph)
    print("整个社交网络指标：")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.6f}")
        else:
            print(f"{key}: {value}")
    
    # 根据介数中心性定义，选择一个具有至少一个邻居的候选节点（这里选邻居最多的）计算全局介数中心性
    candidate_nodes = [node for node in social_graph.nodes if len(list(social_graph.neighbors(node))) > 0]
    if candidate_nodes:
        central_node = max(candidate_nodes, key=lambda n: len(list(social_graph.neighbors(n))))
        print(f"\n选取的中心候选节点为：{central_node} (拥有 {len(list(social_graph.neighbors(central_node)))} 个邻居)")
        
        # 全局介数中心性
        node_bc = compute_global_centrality(social_graph, central_node)
        if node_bc is not None:
            print(f"节点 {central_node} 在全局网络中的介数中心性为：{node_bc:.6f}")
        else:
            print("无法计算全局介数中心性。")
        
        # 全局度中心性
        node_dc = compute_global_degree_centrality(social_graph, central_node)
        if node_dc is not None:
            print(f"节点 {central_node} 在全局网络中的度中心性为：{node_dc:.6f}")
        else:
            print("无法计算全局度中心性。")
    else:
        print("\n未找到满足条件的候选节点（至少拥有一个邻居）。")