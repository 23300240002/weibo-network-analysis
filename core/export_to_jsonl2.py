import csv
import json
from network import build_social_network_from_csv, compute_global_centrality, compute_global_degree_centrality, calculate_average_neighbor_degree
import easygraph.classes as eg
import easygraph.functions as eg_f

def export_users_to_jsonl(csv_path, jsonl_path):
    # 预处理：统计每个用户的发帖总数
    post_count = {}
    with open(csv_path, 'r', encoding='gb18030', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("MD5-作者ID", "").strip()
            if user_id:
                post_count[user_id] = post_count.get(user_id, 0) + 1

    # 构建整个社交网络图
    social_graph = build_social_network_from_csv(csv_path)
    degree_map = social_graph.degree()
    
    user_info = {}
    
    with open(csv_path, 'r', encoding='gb18030', errors='ignore') as f_in, \
         open(jsonl_path, 'w', encoding='utf-8') as f_out:
        
        reader = csv.DictReader(f_in)
        for row in reader:
            user_id = row.get("MD5-作者ID", "").strip()
            # 确保遍历所有csv中出现的用户，如果已经记录过，则跳过
            if user_id not in user_info:
                # 记录用户的个人信息
                user_info[user_id] = {
                    "user_id": user_id,
                    "personal_info": {
                        "region": row.get("地域", ""),
                        "fans_count": int((row.get("粉丝数", "").strip() or "0")),
                        "total_posts": post_count.get(user_id, 0)
                    },
                    "network_info": {},
                    "ego_network_info": {}
                }
                # 如果用户在社交网络图中存在，则计算其网络指标
                if user_id in social_graph.nodes:
                    net_info = {}
                    net_info["degree"] = degree_map[user_id]
                    net_info["global_betweenness"] = compute_global_centrality(social_graph, user_id)
                    net_info["global_degree_centrality"] = compute_global_degree_centrality(social_graph, user_id)
                    user_info[user_id]["network_info"] = net_info
                    
                    # 构造ego network（中心结点及其一跳邻居形成的子图）
                    ego_net = eg_f.ego_graph(social_graph, user_id)
                    # print(f"ego_net nodes: {ego_net.nodes}")
                    # # 输出进度百分比
                    # total_users = len(social_graph.nodes)
                    # processed_users = len(user_info)
                    # progress = (processed_users / total_users) * 100
                    # print(f"Processing user {user_id}: {progress:.2f}% completed")

                    ego_info = {}
                    if len(ego_net.nodes) > 1: # 如果ego network中有超过一个节点
                        # 已有指标
                        ego_info["clustering_coefficient"] = eg_f.clustering(ego_net, user_id)
                        ego_info["density"] = eg.density(ego_net)
                        ego_info["average_nearest_neighbor_degree"] = calculate_average_neighbor_degree(ego_net, user_id)
                        # 新增局部介数中心性
                        bc_list = eg_f.betweenness_centrality(ego_net) # 这里计算的事实上是邻居网络中每个节点的介数中心性
                        ego_nodes = list(ego_net.nodes) # 获取ego network中的节点，set转为list
                        # 若中心节点存在于 ego network 中，则取其对应的介数中心性，否则记为0
                        if user_id in ego_nodes:
                            ego_info["ego_betweenness"] = bc_list[ego_nodes.index(user_id)]
                        else:
                            ego_info["ego_betweenness"] = 0
                    else:
                        ego_info["clustering_coefficient"] = 0
                        ego_info["density"] = 0
                        ego_info["average_nearest_neighbor_degree"] = 0
                        ego_info["ego_betweenness"] = 0
                    user_info[user_id]["ego_network_info"] = ego_info
                else:
                    user_info[user_id]["network_info"] = {}
                    user_info[user_id]["ego_network_info"] = {}
    
                f_out.write(json.dumps(user_info[user_id], ensure_ascii=False) + "\n")

if __name__ == "__main__":
    csv_file = "./data/《网络直播营销活动行为规范》7月1日实施.csv"   # 从 data 文件夹读取         
    output_file = "./results/result1.jsonl"
    export_users_to_jsonl(csv_file, output_file)
    print("JSONL 文件导出完成！")