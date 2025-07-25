import os
import json
import time
import random
import requests
import pandas as pd
import concurrent.futures
from datetime import datetime
from tqdm import tqdm

# 配置
USER_ID = '6027167937'  # 当前处理的种子用户ID
DATA_DIR = f'C:/Tengfei/data/data/domain_networks/user_{USER_ID}'
CONFIG_PATH = 'C:/Tengfei/data/crawler/weiboSpider/config.json'
SLEEP_MIN = 1.0  # 请求间隔最小秒数
SLEEP_MAX = 4.0  # 请求间隔最大秒数
MAX_RETRIES = 1  # 最大重试次数
MAX_CONNECTIONS_PER_USER = 300  # 每个用户最多爬取的关注数
BATCH_SIZE = 30  # 每批处理的用户数量
MAX_WORKERS = 5  # 最大并行线程数

def load_config(config_path=CONFIG_PATH):
    """加载配置文件"""
    try:
        with open(config_path) as f:
            config = json.load(f)
            cookie = config.get('cookie', '')
        print(f"成功从 {config_path} 加载配置")
        return cookie
    except FileNotFoundError:
        print(f"错误: 找不到配置文件 {config_path}")
        return ""

def load_network_data():
    """加载已有的网络数据"""
    # 加载用户数据
    users_df = pd.read_csv(f'{DATA_DIR}/users.csv')
    users_df['user_id'] = users_df['user_id'].astype(str)
    users_df.set_index('user_id', inplace=True)
    
    # 加载边数据
    edges_df = pd.read_csv(f'{DATA_DIR}/edges.csv')
    edges_df['source'] = edges_df['source'].astype(str)
    edges_df['target'] = edges_df['target'].astype(str)
    
    # 加载流行度数据
    popularity_df = pd.read_csv(f'{DATA_DIR}/popularity.csv')
    popularity_df['user_id'] = popularity_df['user_id'].astype(str)
    popularity_df.set_index('user_id', inplace=True)
    
    # 加载节点类别信息 (如果有)
    try:
        with open(f'{DATA_DIR}/node_categories.json', 'r', encoding='utf-8') as f:
            node_categories = json.load(f)
            # 将列表转换为集合以便快速查找
            for category in node_categories:
                node_categories[category] = set(map(str, node_categories[category]))
    except FileNotFoundError:
        print("警告: 找不到节点类别信息文件，将尝试从edges推断")
        # 如果没有节点类别信息，根据网络结构推断
        node_categories = {"A": {USER_ID}, "B": set(), "C": set()}
        
        # 种子用户的直接关注是B类
        b_nodes = set(edges_df[edges_df['source'] == USER_ID]['target'])
        node_categories["B"] = b_nodes
        
        # B类用户的关注但不是A或B类的是C类
        a_and_b = node_categories["A"].union(node_categories["B"])
        for _, row in edges_df.iterrows():
            source = row['source']
            target = row['target']
            if source in node_categories["B"] and target not in a_and_b:
                node_categories["C"].add(target)
    
    print(f"已加载 {len(users_df)} 个用户, {len(edges_df)} 条边, {len(popularity_df)} 条流行度记录")
    print(f"节点类别: A={len(node_categories['A'])}, B={len(node_categories['B'])}, C={len(node_categories['C'])}")
    
    return users_df, edges_df, popularity_df, node_categories

def get_following(user_id, page=1, max_retries=MAX_RETRIES, headers=None):
    """获取用户关注的人列表，包含重试机制和指数退避"""
    url = 'https://weibo.com/ajax/friendships/friends'
    params = {'page': page, 'uid': user_id}
    
    # 使用更简洁的Cookie
    essential_cookies = ['SUB=', 'SUBP=', 'SSOLoginState=']
    cookie_parts = []
    for c in headers['Cookie'].split('; '):
        if any(c.startswith(k) for k in essential_cookies):
            cookie_parts.append(c)
    
    minimal_headers = {
        'User-Agent': headers['User-Agent'],
        'Accept': 'application/json',
        'Cookie': '; '.join(cookie_parts)
    }
    
    for retry in range(max_retries):
        try:
            # 计算退避时间
            wait_time = (2 ** retry) * 3 if retry > 0 else 0
            if retry > 0:
                print(f"第{retry+1}次重试，等待{wait_time}秒...")
                time.sleep(wait_time)
            
            # 添加随机抖动避免请求过于规律
            jitter = random.uniform(0.3, 0.8)
            time.sleep(jitter)
            
            response = requests.get(url, params=params, headers=minimal_headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                users = data.get('users', [])
                next_cursor = data.get('next_cursor', 0)
                return users, next_cursor
            
            print(f"请求失败，状态码: {response.status_code}")
            
            # 如果是错误请求，进一步简化
            if response.status_code in [414, 400, 401, 403]:
                print(f"请求错误({response.status_code})，再次简化请求...")
                # 只提取SUB cookie
                sub_cookie = None
                for cookie in headers['Cookie'].split('; '):
                    if cookie.startswith('SUB='):
                        sub_cookie = cookie
                        break
                
                simplified_headers = {
                    'User-Agent': headers['User-Agent'],
                    'Cookie': sub_cookie if sub_cookie else ''
                }
                
                time.sleep(wait_time + 3)
                simple_response = requests.get(url, params=params, headers=simplified_headers, timeout=15)
                if simple_response.status_code == 200:
                    data = simple_response.json()
                    return data.get('users', []), data.get('next_cursor', 0)
        except Exception as e:
            print(f"请求异常: {e}")
    
    # 所有重试都失败
    return [], 0

def get_user_profile(user_id, max_retries=MAX_RETRIES, headers=None):
    """获取用户资料，包括转发、点赞、评论数和发帖总数"""
    url = f'https://weibo.com/ajax/profile/info?uid={user_id}'
    
    for retry in range(max_retries):
        try:
            wait_time = (2 ** retry) * 3 if retry > 0 else 0
            if retry > 0:
                print(f"获取用户资料第{retry+1}次重试，等待{wait_time}秒...")
                time.sleep(wait_time)
            
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                user_info = data.get('data', {}).get('user', {})
                return user_info
            
            print(f"获取用户资料失败，状态码: {response.status_code}")
        except Exception as e:
            print(f"获取用户资料异常: {e}")
    
    # 所有重试都失败
    return {}

def fetch_all_following_pages(user_id, headers):
    """获取用户的所有关注页"""
    all_users = []
    page = 1
    next_cursor = 1
    following_count = 0
    empty_page_count = 0  # 记录连续空页面数
    
    while next_cursor and following_count < MAX_CONNECTIONS_PER_USER:
        users, next_cursor = get_following(user_id, page, headers=headers)
        
        # 处理空结果
        if len(users) == 0:
            empty_page_count += 1
            if empty_page_count >= 3:  # 连续3页空结果
                print(f"警告: 用户 {user_id} 连续3页未获取到关注，尝试等待20秒后继续...")
                time.sleep(20)  # 较长等待时间
                empty_page_count = 0
            # 尝试额外重试
            print(f"页面 {page} 未获取到关注，进行额外重试...")
            time.sleep(5)
            retry_users, retry_cursor = get_following(user_id, page, headers=headers)
            if len(retry_users) > 0:
                users = retry_users
                next_cursor = retry_cursor
                empty_page_count = 0
        else:
            empty_page_count = 0  # 重置空页面计数
        
        # 添加用户到结果
        all_users.extend(users)
        following_count += len(users)
        
        # 页码递增
        page += 1
        
        # 短暂等待避免请求过快
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        
        # 如果已经达到最大获取数，停止
        if following_count >= MAX_CONNECTIONS_PER_USER:
            print(f"已达到最大获取数 {MAX_CONNECTIONS_PER_USER}，停止获取")
            break
    return all_users

def is_abc_user(user_id, node_categories):
    """检查用户是否属于ABC类"""
    user_id_str = str(user_id)
    return (user_id_str in node_categories["A"] or 
            user_id_str in node_categories["B"] or 
            user_id_str in node_categories["C"])

def process_single_user(user_id, users_df, edges_df, popularity_df, node_categories, headers):
    """处理单个用户的关注列表，并返回更新信息"""
    # 保存处理结果的字典
    result = {
        'new_edges': [],
        'updated_user': False,
        'new_popularity': 0
    }
    
    # 获取用户关注列表
    following_users = fetch_all_following_pages(user_id, headers)
    
    if not following_users:
        print(f"用户 {user_id} 确实没有关注任何人或者设置了隐私")
        return result
    
    print(f"用户 {user_id} 实际上关注了 {len(following_users)} 人，进行数据更新")
    
    # 更新用户表中的关注人数
    if user_id in users_df.index:
        users_df.at[user_id, 'friends_count'] = len(following_users)
    else:
        # 获取用户资料
        user_profile = get_user_profile(user_id, headers=headers)
        users_df.loc[user_id] = {
            'screen_name': user_profile.get('screen_name', f"用户{user_id}"),
            'followers_count': user_profile.get('followers_count', 0),
            'friends_count': len(following_users),
            'statuses_count': user_profile.get('statuses_count', 0),
            'verified': user_profile.get('verified', False),
            'description': user_profile.get('description', '')
        }
    
    result['updated_user'] = True
    
    # 处理新发现的关注关系
    new_edges = []
    for user in following_users:
        following_id = str(user.get('id'))
        if following_id and is_abc_user(following_id, node_categories):
            # 只添加ABC类用户之间的边
            edge_exists = ((edges_df['source'] == user_id) & (edges_df['target'] == following_id)).any()
            if not edge_exists:
                new_edges.append([user_id, following_id])
            
            # 如果关注的ABC类用户不在用户表中，添加该用户
            if following_id not in users_df.index:
                users_df.loc[following_id] = {
                    'screen_name': user.get('screen_name', ''),
                    'followers_count': user.get('followers_count', 0),
                    'friends_count': user.get('friends_count', 0),
                    'statuses_count': user.get('statuses_count', 0),
                    'verified': user.get('verified', False),
                    'description': user.get('description', '')
                }
            
            # 如果关注的ABC类用户不在流行度表中，获取并添加流行度信息
            if following_id not in popularity_df.index:
                # 获取用户资料和流行度信息
                user_profile = get_user_profile(following_id, headers=headers)
                
                # 提取转赞评数据和发帖总数
                statuses_count = user_profile.get('statuses_count', 0)
                status_counter = user_profile.get('status_total_counter', {})
                if status_counter:
                    # 获取字符串格式的数据，去除逗号后转为整数
                    reposts_count = int(str(status_counter.get('repost_cnt', '0')).replace(',', ''))
                    attitudes_count = int(str(status_counter.get('like_cnt', '0')).replace(',', ''))
                    comments_count = int(str(status_counter.get('comment_cnt', '0')).replace(',', ''))
                else:
                    reposts_count = 0
                    attitudes_count = 0
                    comments_count = 0
                
                # 计算转赞评总数和平均流行度
                interaction_count = reposts_count + attitudes_count + comments_count
                avg_popularity = interaction_count / statuses_count if statuses_count > 0 else 0
                
                # 添加流行度数据
                popularity_df.loc[following_id] = {
                    'statuses_count': statuses_count,
                    'reposts_count': reposts_count,
                    'attitudes_count': attitudes_count,
                    'comments_count': comments_count,
                    'interaction_count': interaction_count,
                    'avg_popularity': avg_popularity
                }
                result['new_popularity'] += 1
                
                # 等待一段时间避免请求过快
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    
    # 设置结果中的新边
    result['new_edges'] = new_edges
    
    return result

def process_batch(batch_users, users_df, edges_df, popularity_df, node_categories, headers):
    """并行处理一批用户"""
    print(f"\n开始处理批次，包含 {len(batch_users)} 个用户")
    
    batch_results = {
        'updated_users': 0,
        'new_edges': [],
        'new_popularity': 0
    }
    
    # 使用线程池并行处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch_users))) as executor:
        # 创建任务
        futures = {}
        for user_id in batch_users:
            future = executor.submit(
                process_single_user, 
                user_id, 
                users_df, 
                edges_df,
                popularity_df, 
                node_categories, 
                headers
            )
            futures[future] = user_id
        
        # 处理结果
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="处理进度"):
            user_id = futures[future]
            try:
                result = future.result()
                
                # 更新统计信息
                if result['updated_user']:
                    batch_results['updated_users'] += 1
                
                if result['new_edges']:
                    batch_results['new_edges'].extend(result['new_edges'])
                    print(f"为用户 {user_id} 添加了 {len(result['new_edges'])} 条新的关注关系")
                
                batch_results['new_popularity'] += result['new_popularity']
                
            except Exception as e:
                print(f"处理用户 {user_id} 时出错: {e}")
    
    # 将新边添加到边框架
    if batch_results['new_edges']:
        new_edges_df = pd.DataFrame(batch_results['new_edges'], columns=['source', 'target'])
        edges_df_updated = pd.concat([edges_df, new_edges_df], ignore_index=True)
        return edges_df_updated, batch_results
    
    return edges_df, batch_results

def patch_missing_following_users():
    """修复那些在edges表中没有出边的用户，可能是漏爬或设置了隐私"""
    start_time = datetime.now()
    print(f"开始修复时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 加载配置
    cookie = load_config()
    if not cookie:
        print("错误: 配置文件中缺少必要的cookie")
        return
    
    # 设置请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Cookie': cookie,
        'Accept': 'application/json'
    }
    
    # 加载网络数据
    users_df, edges_df, popularity_df, node_categories = load_network_data()
    
    # 从popularity表获取所有用户ID
    all_users = set(popularity_df.index.tolist())
    
    # 找出所有在edges表中有出边的用户
    users_with_outgoing_edges = set(edges_df['source'].unique())
    
    # 找出所有没有出边的用户
    users_without_outgoing_edges = all_users - users_with_outgoing_edges
    
    # 过滤只保留ABC类用户
    abc_users = set()
    for category in ["A", "B", "C"]:
        abc_users.update(node_categories[category])
    
    # 找出ABC类中没有出边的用户
    missing_following_abc_users = list(users_without_outgoing_edges.intersection(abc_users))
    
    print(f"网络中共有 {len(all_users)} 个用户")
    print(f"其中 {len(users_with_outgoing_edges)} 个用户有出边")
    print(f"共有 {len(users_without_outgoing_edges)} 个用户没有出边")
    print(f"ABC类用户中共有 {len(missing_following_abc_users)} 个没有出边的用户")
    
    # 统计变量
    total_updated_users = 0
    total_new_edges = 0
    total_new_popularity = 0
    
    # 创建备份目录
    backup_dir = f'{DATA_DIR}/patch_backup'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    # 分批处理用户
    for batch_start in range(0, len(missing_following_abc_users), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(missing_following_abc_users))
        batch_users = missing_following_abc_users[batch_start:batch_end]
        
        print(f"\n处理批次 {batch_start//BATCH_SIZE + 1}/{(len(missing_following_abc_users)-1)//BATCH_SIZE + 1}")
        print(f"批次范围: {batch_start+1}-{batch_end} / {len(missing_following_abc_users)}")
        
        # 并行处理批次
        edges_df, batch_results = process_batch(
            batch_users, users_df, edges_df, popularity_df, node_categories, headers
        )
        
        # 更新总统计
        total_updated_users += batch_results['updated_users']
        total_new_edges += len(batch_results['new_edges'])
        total_new_popularity += batch_results['new_popularity']
        
        # 保存批次结果
        print(f"批次结果: 更新了 {batch_results['updated_users']} 个用户, 添加了 {len(batch_results['new_edges'])} 条边, 添加了 {batch_results['new_popularity']} 条流行度记录")
        
        # 保存临时结果
        temp_users_df = users_df.copy()
        temp_users_df.reset_index(inplace=True)
        temp_users_df.to_csv(f'{backup_dir}/users_temp.csv', index=False, encoding='utf-8-sig')
        
        edges_df.to_csv(f'{backup_dir}/edges_temp.csv', index=False, encoding='utf-8-sig')
        
        temp_popularity_df = popularity_df.copy()
        temp_popularity_df.reset_index(inplace=True)
        temp_popularity_df.to_csv(f'{backup_dir}/popularity_temp.csv', index=False, encoding='utf-8-sig')
        
        print(f"已保存临时结果到 {backup_dir}")
    
    # 保存最终更新后的数据
    users_df.reset_index(inplace=True)
    users_df.to_csv(f'{DATA_DIR}/users_patched.csv', index=False, encoding='utf-8-sig')
    
    edges_df.to_csv(f'{DATA_DIR}/edges_patched.csv', index=False, encoding='utf-8-sig')
    
    popularity_df.reset_index(inplace=True)
    popularity_df.to_csv(f'{DATA_DIR}/popularity_patched.csv', index=False, encoding='utf-8-sig')
    
    # 打印修复统计信息
    print("\n===== 修复完成 =====")
    print(f"更新了 {total_updated_users} 个用户的关注人数")
    print(f"添加了 {total_new_edges} 条新的关注关系")
    print(f"添加了 {total_new_popularity} 条新的流行度记录")
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"总耗时: {duration}")
    
    # 提醒用户检查和应用更新
    print("\n修复后的数据已保存为 users_patched.csv, edges_patched.csv, popularity_patched.csv")
    print("请检查修复后的数据，如果确认无误，可以将它们重命名为原文件名以应用更新")

if __name__ == "__main__":
    patch_missing_following_users()