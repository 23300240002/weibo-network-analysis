import csv
import easygraph as eg

# 根据 CSV 文件构建社交网络
def build_social_network_from_csv(csv_path):
    G = eg.Graph()  # 使用 EasyGraph 创建一个无向图
    with open(csv_path, 'r', encoding='gb18030') as f:
        reader = csv.DictReader(f)
        for row in reader:
            author_id = row.get("MD5-作者ID", "").strip()
            parent_author_id = row.get("MD5-父微博用户ID", "").strip()
            is_retweet = (row.get("原创/转发", "").strip() == "转发")

            if author_id and parent_author_id and is_retweet:
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
    metrics["图密度"] = eg.density(G)
    metrics["聚类系数"] = eg.average_clustering(G)
    metrics["平均最近邻度"] = calculate_average_neighbor_degree(G)
    return metrics

if __name__ == "__main__":
    csv_file = "《网络直播营销活动行为规范》7月1日实施.csv"
    social_graph = build_social_network_from_csv(csv_file)

    # 1. 计算整体社交网络的统计指标
    metrics = compute_graph_metrics(social_graph)
    print("整个社交网络指标：")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.6f}")
        else:
            print(f"{key}: {value}")

    # 2. 计算并可视化结构洞节点（示例代码参考 EasyGraph 官网示例）
    #    如果 CSV 数据较大，建议先在小规模图上进行演示
    #    common_greedy 计算结构洞候选，shs 是节点的列表
    shs = eg.common_greedy(social_graph, 5)

    # 3. 在图中高亮绘制结构洞节点
    eg.draw_SHS_center(social_graph, shs)

    # 4. 绘制结构洞节点与普通节点在粉丝数（或度数）的分布对比
    #    在有粉丝或关注的数据场景下才可视化；如仅有转发关系，仍可做对比
    eg.plot_Followers(social_graph, shs)