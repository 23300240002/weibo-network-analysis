# 本文件为第一版导出JSONL文件的代码，主要实现了从CSV文件中读取数据，构建社交网络图，计算网络指标，并将用户信息导出为JSONL文件
# 并未涉及到因果推断的部分，因此无法直接与causal_forest_analysis.py进行比较
# 未计算popularity

import csv
import json
from network import build_social_network_from_csv, compute_global_centrality, compute_global_degree_centrality, calculate_average_neighbor_degree
import easygraph.classes as eg
import easygraph.functions as eg_f

def export_users_to_jsonl(csv_path, jsonl_path):
    # 先构建整个社交网络图
    social_graph = build_social_network_from_csv(csv_path)
    # 获取每个节点的度数映射
    degree_map = social_graph.degree()
    
    with open(csv_path, 'r', encoding='gb18030') as f_in, \
         open(jsonl_path, 'w', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in)
        for row in reader:
            user_id = row.get("MD5-作者ID", "").strip()
            is_retweet = (row.get("原创/转发", "").strip() == "转发")
            record = {
                "user_id": user_id,
                "is_retweet": is_retweet,
                "personal_info": {
                    "region": row.get("地区", "")
                },
                "tweet_info": {
                    "title": row.get("标题", ""),
                    "has_tag": "#" in row.get("标题", "")
                }
            }
            # 如果用户节点存在于社交网络中，则获取网络指标
            if user_id in social_graph.nodes:
                net_info = {}
                net_info["degree"] = degree_map[user_id]  # 节点度数
                net_info["global_betweenness"] = compute_global_centrality(social_graph, user_id)
                net_info["global_degree_centrality"] = compute_global_degree_centrality(social_graph, user_id)
                record["network_info"] = net_info
                
                # 生成该用户的ego network（由中心用户及其一跳邻居构成的子图）
                ego_net = eg_f.ego_graph(social_graph, user_id)
                ego_info = {}
                # 计算ego network的聚类系数，若ego网络只有单个点，则定义为0
                if len(ego_net.nodes) > 1:
                    ego_info["clustering_coefficient"] = eg_f.average_clustering(ego_net)
                    ego_info["density"] = eg.density(ego_net)
                    ego_info["average_nearest_neighbor_degree"] = calculate_average_neighbor_degree(ego_net)
                else:
                    ego_info["clustering_coefficient"] = 0
                    ego_info["density"] = 0
                    ego_info["average_nearest_neighbor_degree"] = 0
                record["ego_network_info"] = ego_info
            else:
                record["network_info"] = {}
                record["ego_network_info"] = {}
            
            # 写入文件，文件中的每一行代表社交网络中的一个用户
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    csv_file = "《网络直播营销活动行为规范》7月1日实施.csv"
    output_file = "result.jsonl"
    export_users_to_jsonl(csv_file, output_file)
    print("JSONL 文件导出完成！")