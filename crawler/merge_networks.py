import os
import json
import pandas as pd
from datetime import datetime

# 配置
BASE_DIR = 'C:/Tengfei/data/data/domain_networks'
OUTPUT_DIR = f'{BASE_DIR}/merged_network'

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def merge_networks(user_ids):
    """合并多个用户的网络数据"""
    print("开始合并网络...")
    
    # 初始化合并后的数据
    all_users = {}
    all_edges = set()
    all_popularity = {}
    all_categories = {"A": set(), "B": set(), "C": set()}
    
    # 遍历每个用户目录
    for user_id in user_ids:
        user_dir = f'{BASE_DIR}/user_{user_id}'
        
        print(f"处理用户 {user_id} 的网络数据...")
        
        # 加载用户数据
        users_file = f'{user_dir}/users.csv'
        if os.path.exists(users_file):
            users_df = pd.read_csv(users_file)
            for _, row in users_df.iterrows():
                uid = str(row['user_id'])
                # 构建用户数据字典
                user_data = {col: row[col] for col in users_df.columns if col != 'user_id'}
                # 更新或添加用户数据
                all_users[uid] = user_data
            print(f"  已加载 {len(users_df)} 个用户")
        
        # 加载边数据
        edges_file = f'{user_dir}/edges.csv'
        if os.path.exists(edges_file):
            edges_df = pd.read_csv(edges_file)
            for _, row in edges_df.iterrows():
                # 添加边(作为元组，确保唯一性)
                edge = (str(row['source']), str(row['target']))
                all_edges.add(edge)
            print(f"  已加载 {len(edges_df)} 条边")
        
        # 加载流行度数据
        popularity_file = f'{user_dir}/popularity.csv'
        if os.path.exists(popularity_file):
            pop_df = pd.read_csv(popularity_file)
            for _, row in pop_df.iterrows():
                uid = str(row['user_id'])
                # 构建流行度数据字典
                pop_data = {col: row[col] for col in pop_df.columns if col != 'user_id'}
                # 更新或添加流行度数据
                all_popularity[uid] = pop_data
            print(f"  已加载 {len(pop_df)} 条流行度数据")
        
        # 加载节点类别
        categories_file = f'{user_dir}/node_categories.json'
        if os.path.exists(categories_file):
            with open(categories_file, 'r', encoding='utf-8') as f:
                categories = json.load(f)
                # 合并类别
                for category, nodes in categories.items():
                    all_categories[category].update(nodes)
            print(f"  已加载节点类别数据")
    
    # 确保输出目录存在
    ensure_dir(OUTPUT_DIR)
    
    # 保存合并后的用户数据
    users_df = pd.DataFrame.from_dict(all_users, orient='index')
    users_df.index.name = 'user_id'
    users_df.reset_index(inplace=True)
    users_df.to_csv(f'{OUTPUT_DIR}/users.csv', index=False, encoding='utf-8-sig')
    
    # 保存合并后的边数据
    edges_list = list(all_edges)
    edges_df = pd.DataFrame(edges_list, columns=['source', 'target'])
    edges_df.to_csv(f'{OUTPUT_DIR}/edges.csv', index=False, encoding='utf-8-sig')
    
    # 保存合并后的流行度数据
    popularity_df = pd.DataFrame.from_dict(all_popularity, orient='index')
    popularity_df.index.name = 'user_id'
    popularity_df.reset_index(inplace=True)
    popularity_df.to_csv(f'{OUTPUT_DIR}/popularity.csv', index=False, encoding='utf-8-sig')
    
    # 保存合并后的节点类别
    with open(f'{OUTPUT_DIR}/node_categories.json', 'w', encoding='utf-8') as f:
        json.dump({k: list(v) for k, v in all_categories.items()}, f, ensure_ascii=False)
    
    # 保存合并网络信息
    with open(f'{OUTPUT_DIR}/network_info.json', 'w', encoding='utf-8') as f:
        info = {
            "原始网络": [f"user_{uid}" for uid in user_ids],
            "节点数": len(all_users),
            "边数": len(all_edges),
            "A类节点数": len(all_categories["A"]),
            "B类节点数": len(all_categories["B"]),
            "C类节点数": len(all_categories["C"]),
            "合并时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        json.dump(info, f, ensure_ascii=False, indent=2)
    
    print("\n网络合并完成!")
    print(f"合并后总节点数: {len(all_users)}")
    print(f"合并后总边数: {len(all_edges)}")
    print(f"节点分布: A类: {len(all_categories['A'])}, B类: {len(all_categories['B'])}, C类: {len(all_categories['C'])}")
    print(f"合并数据已保存到 {OUTPUT_DIR} 目录")
    
    return OUTPUT_DIR

def main():
    # 从配置文件加载种子用户ID
    config_path = 'C:/Tengfei/data/crawler/weiboSpider/config.json'
    try:
        with open(config_path) as f:
            config = json.load(f)
            user_ids = config.get('user_id_list', [])
        if not user_ids:
            print("错误: 配置文件中没有找到用户ID")
            return
        
        print(f"将合并以下用户的网络: {', '.join(user_ids)}")
        merge_networks(user_ids)
        
    except FileNotFoundError:
        print(f"错误: 找不到配置文件 {config_path}")
    except Exception as e:
        print(f"合并过程中出错: {e}")

if __name__ == "__main__":
    main()