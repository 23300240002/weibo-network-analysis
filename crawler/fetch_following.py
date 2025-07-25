import os
import json
import time
import random
import requests
import pandas as pd
import concurrent.futures
from datetime import datetime
from tqdm import tqdm
import sys

# 配置
MAX_DEPTH = 2  # 爬取深度：0=只爬种子用户，1=爬取一跳网络，2=爬取二跳网络(C类用户)
MAX_CONNECTIONS_PER_USER = 300  # 每个用户最多爬取的关注数
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/domain_networks'
PROGRESS_FILE_TEMPLATE = 'C:/Tengfei/data/crawler/weiboSpider/progress_backup_{}.json'  # 进度文件模板
MAX_WORKERS = 5  # 最大并行线程数
SLEEP_MIN = 1.0  # 请求间隔最小秒数
SLEEP_MAX = 3.0  # 请求间隔最大秒数
BATCH_SIZE = 30  # 每批处理的用户数量
BATCH_INTERVAL_MIN = 5.0  # 批次间最小等待时间(秒)
BATCH_INTERVAL_MAX = 15.0  # 批次间最大等待时间(秒)
ZERO_FOLLOWING_THRESHOLD = 8  # 连续关注数为0的阈值

# 全局变量
processed_users = set()  # 已处理的用户ID集合
users_data = {}  # 用户信息字典
edges_data = []  # 边数据列表
popularity_data = {}  # 流行度数据字典
node_categories = {"A": set(), "B": set(), "C": set()}  # 用于跟踪节点类别
seed_user_id = None  # 种子用户ID，在main函数中设置

def load_config(config_path='C:/Tengfei/data/crawler/weiboSpider/config.json'):
    """加载配置文件"""
    try:
        with open(config_path) as f:
            config = json.load(f)
            cookie = config.get('cookie', '')
            seed_users = config.get('user_id_list', [])
        print(f"成功从 {config_path} 加载配置")
        return cookie, seed_users
    except FileNotFoundError:
        print(f"错误: 找不到配置文件 {config_path}")
        return "", []

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_progress(seed_user_id):
    """保存爬取进度，便于中断后继续"""
    progress_file = PROGRESS_FILE_TEMPLATE.format(seed_user_id)
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump({
            "users": users_data, 
            "edges": edges_data,
            "popularity": popularity_data,
            "processed": list(processed_users),
            "categories": {k: list(v) for k, v in node_categories.items()}
        }, f, ensure_ascii=False)
    print(f"进度已保存到 {progress_file}")

def load_progress(seed_user_id):
    """加载之前的爬取进度"""
    global processed_users, users_data, edges_data, popularity_data, node_categories
    progress_file = PROGRESS_FILE_TEMPLATE.format(seed_user_id)
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"已从 {progress_file} 加载之前的进度")
            processed_users = set(data.get("processed", []))
            users_data = data.get("users", {})
            edges_data = data.get("edges", [])
            popularity_data = data.get("popularity", {})
            
            # 加载节点类别
            if "categories" in data:
                for k, v in data["categories"].items():
                    node_categories[k] = set(v)
        except Exception as e:
            print(f"加载进度文件出错: {e}，将重新开始爬取")
            processed_users = set()
            users_data = {}
            edges_data = []
            popularity_data = {}
            node_categories = {"A": set(), "B": set(), "C": set()}

def get_following(user_id, page=1, max_retries=3, headers=None):
    """获取用户关注的人列表，包含重试机制和指数退避"""
    url = 'https://weibo.com/ajax/friendships/friends'
    params = {'page': page, 'uid': user_id}
    
    # 使用更完整的Cookie，包含关键的认证信息
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
            
            # 使用params参数而不是URL拼接
            response = requests.get(url, params=params, headers=minimal_headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                users = data.get('users', [])
                next_cursor = data.get('next_cursor', 0)
                return users, next_cursor
            
            print(f"请求失败，状态码: {response.status_code}")
            
            # 如果仍然是414错误，尝试进一步简化请求
            if response.status_code == 414:
                print("请求过长(414)，再次简化请求...")
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

def get_user_profile(user_id, max_retries=3, headers=None):
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
            break
            
        # 每3页随机增加额外等待，减少被检测风险
        if page % 3 == 0:
            extra_wait = random.uniform(3, 5)
            print(f"已获取{following_count}个关注，增加额外等待{extra_wait:.1f}秒...")
            time.sleep(extra_wait)
    
    return all_users

def process_user_following(user_id, category=None, headers=None):
    """处理单个用户的关注列表"""
    global processed_users, users_data, edges_data, popularity_data, node_categories
    
    # 检查是否已处理
    if user_id in processed_users:
        return 0
    processed_users.add(user_id)
    
    # 如果提供了类别，添加到相应类别集合
    if category and category in node_categories:
        node_categories[category].add(user_id)
    
    print(f"正在获取用户 {user_id} 的关注列表...")
    
    # 获取关注列表
    following_users = fetch_all_following_pages(user_id, headers)
    
    # 获取用户资料(包括转赞评和发帖数)
    user_profile = get_user_profile(user_id, headers=headers)
    
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
    
    # 计算转赞评总数
    interaction_count = reposts_count + attitudes_count + comments_count
    
    # 计算平均流行度(如果发帖数为0，则设为0)
    avg_popularity = interaction_count / statuses_count if statuses_count > 0 else 0
    
    # 更新数据
    # 初始化用户信息
    if user_id not in users_data:
        users_data[user_id] = {
            'screen_name': user_profile.get('screen_name', f"用户{user_id}"),
            'followers_count': user_profile.get('followers_count', 0),
            'friends_count': user_profile.get('friends_count', 0) or len(following_users),
            'statuses_count': statuses_count,
            'verified': user_profile.get('verified', False),
            'description': user_profile.get('description', '')
        }
    
    # 添加流行度数据
    popularity_data[user_id] = {
        'statuses_count': statuses_count,
        'reposts_count': reposts_count,
        'attitudes_count': attitudes_count,
        'comments_count': comments_count,
        'interaction_count': interaction_count,
        'avg_popularity': avg_popularity
    }
    
    # 处理每个关注的用户
    for user in following_users:
        following_id = str(user.get('id'))
        if following_id:
            # 添加用户信息
            if following_id not in users_data:
                users_data[following_id] = {
                    'screen_name': user.get('screen_name', ''),
                    'followers_count': user.get('followers_count', 0),
                    'friends_count': user.get('friends_count', 0),
                    'statuses_count': user.get('statuses_count', 0),
                    'verified': user.get('verified', False),
                    'description': user.get('description', '')
                }
            
            # 添加关注关系 - C类用户只保留指向A和B的边
            if category != "C" or following_id in node_categories["A"] or following_id in node_categories["B"]:
                edge = (user_id, following_id)
                if edge not in edges_data:
                    edges_data.append(edge)
    
    return len(following_users)

def process_batch(users_to_process, category=None, batch_size=BATCH_SIZE, desc="并行爬取进度", headers=None):
    """处理一批用户"""
    total_batches = (len(users_to_process) - 1) // batch_size + 1
    
    for i in range(0, len(users_to_process), batch_size):
        batch = users_to_process[i:i+batch_size]
        batch_index = i//batch_size+1
        print(f"\n处理批次 {batch_index}/{total_batches}，包含 {len(batch)} 个用户")
        
        # 防御性操作：检测连续关注数为0的情况
        retry_attempt = 0
        
        while retry_attempt < 2:  # 最多重试一次
            if retry_attempt > 0:
                print(f"\n开始第{retry_attempt+1}次尝试爬取批次 {batch_index}...")
            
            # 跟踪连续出现关注数为0的情况
            recent_results = []  # 存储最近的关注数结果
            consecutive_zeros = 0  # 当前连续0的计数
            max_consecutive_zeros = 0  # 最大连续0计数
            
            # 使用线程池并行处理
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
                futures = {executor.submit(process_user_following, user_id, category, headers): user_id for user_id in batch}
                
                # 使用tqdm显示进度
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=desc):
                    user_id = futures[future]
                    try:
                        following_count = future.result()
                        print(f"用户 {user_id} 成功获取 {following_count} 个关注")
                        
                        # 更新连续0计数
                        recent_results.append((user_id, following_count))
                        
                        # 更新最大连续0计数（注意：这里的"连续"可能不是严格按照处理顺序）
                        if following_count == 0:
                            consecutive_zeros += 1
                            max_consecutive_zeros = max(max_consecutive_zeros, consecutive_zeros)
                        else:
                            consecutive_zeros = 0
                        
                    except Exception as e:
                        print(f"用户 {user_id} 处理出错: {e}")
                        consecutive_zeros = 0  # 发生错误，重置连续计数
            
            # 检查最终的连续0数量
            # 此外，对recent_results再做一次按顺序的检查，因为线程完成顺序可能和提交顺序不同
            sorted_results = sorted(recent_results, key=lambda x: batch.index(x[0]))
            consecutive_zeros = 0
            for _, count in sorted_results:
                if count == 0:
                    consecutive_zeros += 1
                    max_consecutive_zeros = max(max_consecutive_zeros, consecutive_zeros)
                else:
                    consecutive_zeros = 0
            
            # 如果有连续5个或以上关注数为0，认为触发了反爬
            if max_consecutive_zeros >= ZERO_FOLLOWING_THRESHOLD:
                # 创建日志目录
                log_dir = f'{BASE_OUTPUT_DIR}/user_{seed_user_id}/logs'
                ensure_dir(log_dir)
                log_file = f'{log_dir}/anti_crawl_log.txt'
                
                # 记录日志
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 批次 {batch_index}/{total_batches}: ")
                    f.write(f"检测到连续{max_consecutive_zeros}个用户关注数为0，可能触发了反爬机制。重试次数: {retry_attempt + 1}\n")
                    
                    # 记录所有关注数为0的用户
                    zero_users = [uid for uid, count in sorted_results if count == 0]
                    f.write(f"关注数为0的用户IDs: {', '.join(zero_users)}\n")
                
                print(f"\n警告: 检测到连续{max_consecutive_zeros}个用户关注数为0，可能触发了反爬机制")
                
                # 如果是第二次重试仍然触发，则终止程序
                if retry_attempt > 0:
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 二次爬取仍触发反爬机制，程序终止\n")
                    
                    print("\n错误: 二次爬取仍然连续出现关注数为0，程序终止")
                    # 终止程序
                    sys.exit(1)
                
                # 第一次触发，休息后重试
                print(f"休息60秒后将重新爬取当前批次...")
                time.sleep(60)
                retry_attempt += 1
            else:
                # 正常完成，跳出循环
                break
        
        # 批次处理完保存进度
        save_progress(seed_user_id)
        print(f"批次 {batch_index}/{total_batches} 完成")
        
        # 批次间增加随机等待时间
        if i + batch_size < len(users_to_process):
            wait_time = random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
            print(f"等待 {wait_time:.1f} 秒后处理下一批...")
            time.sleep(wait_time)

def save_network_data(output_dir):
    """保存网络数据到指定目录"""
    ensure_dir(output_dir)
    
    # 保存用户数据
    users_df = pd.DataFrame.from_dict(users_data, orient='index')
    users_df.index.name = 'user_id'
    users_df.reset_index(inplace=True)
    users_df.to_csv(f'{output_dir}/users.csv', index=False, encoding='utf-8-sig')
    
    # 保存边数据
    edges_df = pd.DataFrame(edges_data, columns=['source', 'target'])
    edges_df.to_csv(f'{output_dir}/edges.csv', index=False, encoding='utf-8-sig')
    
    # 保存流行度数据
    popularity_df = pd.DataFrame.from_dict(popularity_data, orient='index')
    popularity_df.index.name = 'user_id'
    popularity_df.reset_index(inplace=True)
    popularity_df.to_csv(f'{output_dir}/popularity.csv', index=False, encoding='utf-8-sig')
    
    # 保存节点类别信息
    with open(f'{output_dir}/node_categories.json', 'w', encoding='utf-8') as f:
        json.dump({k: list(v) for k, v in node_categories.items()}, f, ensure_ascii=False)
    
    # 保存网络信息摘要
    with open(f'{output_dir}/network_info_{seed_user_id}.json', 'w', encoding='utf-8') as f:
        info = {
            "节点数": len(users_data),
            "边数": len(edges_data),
            "A类节点数": len(node_categories["A"]),
            "B类节点数": len(node_categories["B"]),
            "C类节点数": len(node_categories["C"]),
            "爬取时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        json.dump(info, f, ensure_ascii=False, indent=2)

def main():
    global seed_user_id
    
    start_time = datetime.now()
    print(f"开始爬取时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 加载配置
    cookie, seed_users = load_config()
    
    if not cookie or not seed_users:
        print("错误: 配置文件中缺少必要的cookie或种子用户ID")
        return
    
    # 确保基础输出目录存在
    ensure_dir(BASE_OUTPUT_DIR)
    
    # 获取要爬取的用户ID
    seed_user_id = seed_users[0]  # 只获取第一个用户
    print(f"\n准备爬取用户 {seed_user_id} 的二跳网络")
    
    # 设置输出目录
    output_dir = f'{BASE_OUTPUT_DIR}/user_{seed_user_id}'
    ensure_dir(output_dir)
    
    # 重置全局变量
    global processed_users, users_data, edges_data, popularity_data, node_categories
    processed_users = set()
    users_data = {}
    edges_data = []
    popularity_data = {}
    node_categories = {"A": set(), "B": set(), "C": set()}
    
    # 加载进度(如果有)
    load_progress(seed_user_id)
    
    # 设置请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Cookie': cookie,
        'Accept': 'application/json'
    }
    
    print(f"\n===== 开始爬取种子用户 {seed_user_id} 的二跳网络 =====")
    
    # 将种子用户标记为A类
    node_categories["A"].add(seed_user_id)
    
    # 爬取种子用户的关注
    if seed_user_id not in processed_users:
        following_count = process_user_following(seed_user_id, "A", headers)
        print(f"种子用户 {seed_user_id} 爬取完成，获取到 {following_count} 个关注用户")
    else:
        print(f"种子用户 {seed_user_id} 已处理，跳过")
    
    # 保存进度
    save_progress(seed_user_id)
    
    # 获取一跳邻居(B类)
    first_neighbors = set()
    for source, target in edges_data:
        if source == seed_user_id:
            first_neighbors.add(target)
            # 将一跳邻居标记为B类
            node_categories["B"].add(target)
    
    print(f"\n===== 开始分批爬取一跳邻居(B类) (共{len(first_neighbors)}个用户) =====")
    
    # 过滤掉已处理的用户
    neighbors_to_process = [n for n in first_neighbors if n not in processed_users]
    print(f"需要处理的一跳邻居: {len(neighbors_to_process)}个")
    
    # 分批处理B类用户
    if neighbors_to_process:
        process_batch(neighbors_to_process, "B", desc=f"爬取 {seed_user_id} 的B类", headers=headers)
    
    # 获取二跳邻居(C类)
    second_neighbors = set()
    a_and_b = node_categories["A"].union(node_categories["B"])
    
    for source, target in edges_data:
        # 如果source是B类用户，且target不是A类或B类，则target是C类
        if source in node_categories["B"] and target not in a_and_b:
            second_neighbors.add(target)
            # 将二跳邻居标记为C类
            node_categories["C"].add(target)
    
    print(f"\n===== 开始分批爬取二跳邻居(C类) (共{len(second_neighbors)}个用户) =====")
    
    # 过滤掉已处理的用户
    c_neighbors_to_process = [n for n in second_neighbors if n not in processed_users]
    print(f"需要处理的二跳邻居: {len(c_neighbors_to_process)}个")
    
    # 分批处理C类用户
    if c_neighbors_to_process:
        process_batch(c_neighbors_to_process, "C", desc=f"爬取 {seed_user_id} 的C类", headers=headers)
    
    # 保存最终网络数据
    save_network_data(output_dir)
    
    # 打印网络信息
    a_count = len(node_categories["A"])
    b_count = len(node_categories["B"])
    c_count = len(node_categories["C"])
    
    print(f"\n用户 {seed_user_id} 的二跳网络爬取完成!")
    print(f"总节点数: {len(users_data)}，总边数: {len(edges_data)}")
    print(f"节点分布: A类(种子用户): {a_count}，B类(一跳邻居): {b_count}，C类(二跳邻居): {c_count}")
    print(f"数据已保存到 {output_dir} 目录")
    
    end_time = datetime.now()
    total_duration = end_time - start_time
    print(f"\n总耗时: {total_duration}")
    print("\n爬取完成后，请修改config.json中的用户ID，继续爬取下一个用户")
    print("待所有用户爬取完成后，运行merge_networks.py合并这些网络")

if __name__ == "__main__":
    main()