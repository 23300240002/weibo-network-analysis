import os
import json
import time
import random
import math
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import re

# 配置参数
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/domain_network2'
PROGRESS_FILE_TEMPLATE = 'C:/Tengfei/data/crawler/fetch/progress_fans_{}.json'

# 无放缩参数
MAX_FANS_LIMIT = 500  # 每个用户最多爬500个粉丝
HIGH_FANS_THRESHOLD = 500  # 超过500粉丝的用户记录为高粉丝用户

# 优化后的参数
SLEEP_MIN = 0.5
SLEEP_MAX = 1.5
BATCH_INTERVAL_MIN = 1.0
BATCH_INTERVAL_MAX = 3.0
ZERO_FANS_THRESHOLD = 10

# 🔥 新的优化参数
MAX_PAGES_LIMIT = 50  # 统一最大页数限制
CONSECUTIVE_EMPTY_THRESHOLD = 2  # 连续空页面阈值：从8改为2

# ===== 配置部分：修改这里的用户ID =====
TARGET_USER_ID = "6027167937"
# =========================================

# 全局变量
processed_users = set()
users_data = {}
edges_data = []
popularity_data = {}
node_categories = {"A": set(), "B": set(), "C": set()}
seed_user_id = None
high_fans_users = {}  # 记录超过上限的用户实际粉丝数

class WeiboFansCrawler:
    def __init__(self, cookie_path='C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'):
        self.driver = None
        self.cookie_path = cookie_path
        
    def setup_driver(self):
        """设置Chrome浏览器"""
        print("正在设置浏览器...")
        
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 性能优化选项
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--disable-javascript')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-extensions')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # 设置更短的超时时间
            self.driver.set_page_load_timeout(8)
            self.driver.implicitly_wait(2)
            
            print("浏览器设置成功")
            return True
        except Exception as e:
            print(f"浏览器设置失败: {e}")
            return False
    
    def load_cookies(self):
        """加载cookie"""
        if not os.path.exists(self.cookie_path):
            print(f"❌ 未找到cookie文件: {self.cookie_path}")
            return False
        
        try:
            with open(self.cookie_path, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            
            self.driver.get('https://weibo.cn')
            time.sleep(1.5)
            
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    pass
            
            self.driver.refresh()
            time.sleep(1.5)
            print("Cookie加载成功")
            return True
            
        except Exception as e:
            print(f"❌ Cookie加载失败: {e}")
            return False

    def test_login_status(self):
        """测试登录状态"""
        try:
            self.driver.get('https://weibo.cn')
            time.sleep(1.5)
            page_source = self.driver.page_source
            
            if '登录' in page_source and '密码' in page_source:
                print("❌ 需要重新获取Cookie")
                return False
            else:
                print("✅ 登录状态正常")
                return True
                
        except Exception as e:
            print(f"❌ 登录状态检查异常: {e}")
            return False
    
    def get_user_fans_count(self, user_id):
        """获取用户的真实粉丝数"""
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            self.driver.get(profile_url)
            time.sleep(random.uniform(0.8, 1.5))
            
            page_source = self.driver.page_source
            
            # 简化的粉丝数提取模式
            patterns = [
                (r'粉丝\[(\d+\.?\d*)[万]?\]', lambda x: int(float(x[:-1]) * 10000) if x.endswith('万') else int(float(x))),
                (r'粉丝\((\d+\.?\d*)[万]?\)', lambda x: int(float(x[:-1]) * 10000) if x.endswith('万') else int(float(x))),
                (r'(\d+\.?\d*)[万]?粉丝', lambda x: int(float(x[:-1]) * 10000) if x.endswith('万') else int(float(x))),
            ]
            
            for pattern, converter in patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    num_str = match.group(1)
                    full_match = match.group(0)
                    
                    if '万' in full_match:
                        fans_count = int(float(num_str) * 10000)
                    else:
                        fans_count = int(float(num_str))
                    
                    return fans_count
            
            return 0
            
        except Exception as e:
            return 0
    
    def determine_crawl_strategy(self, total_fans, user_id):
        """确定爬取策略：无放缩，但有上限"""
        global high_fans_users
        
        if total_fans <= 0:
            return 0, "粉丝数为0，跳过"
        
        if total_fans <= MAX_FANS_LIMIT:
            # 完整爬取
            target_sample_size = total_fans
            strategy = f"完整爬取: {total_fans} 个粉丝"
        else:
            # 超过上限，只爬500个，并记录
            target_sample_size = MAX_FANS_LIMIT
            strategy = f"超过上限，爬取前 {MAX_FANS_LIMIT} 个粉丝"
            
            # 记录高粉丝用户
            high_fans_users[user_id] = {
                'actual_fans_count': total_fans,
                'crawled_count': MAX_FANS_LIMIT,
                'coverage_ratio': MAX_FANS_LIMIT / total_fans,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            print(f"  ⚠️ 用户 {user_id} 粉丝数 {total_fans:,} 超过上限 {HIGH_FANS_THRESHOLD}，已记录")
        
        print(f"  策略: {strategy}")
        return target_sample_size, strategy
    
    def crawl_user_fans(self, user_id):
        """爬取用户的粉丝列表（优化版：连续2页无数据就停止，最大50页）"""
        # 获取用户真实粉丝数
        total_fans = self.get_user_fans_count(user_id)
        
        if total_fans == 0:
            return []
        
        # 确定爬取策略（无放缩，但有上限）
        target_sample_size, strategy = self.determine_crawl_strategy(total_fans, user_id)
        
        if target_sample_size == 0:
            return []
        
        print(f"  需要爬取 {target_sample_size} 个粉丝（总粉丝数: {total_fans:,}）")
        print(f"  最大页数限制: {MAX_PAGES_LIMIT}, 连续空页阈值: {CONSECUTIVE_EMPTY_THRESHOLD}")
        
        try:
            fans_url = f'https://weibo.cn/{user_id}/fans'
            self.driver.get(fans_url)
            time.sleep(0.8)
            
            page_source = self.driver.page_source
            if '用户不存在' in page_source or '登录' in page_source:
                return []
            
            fans_data = []
            consecutive_empty_pages = 0
            
            # 🔥 优化：统一使用50页限制
            for page in range(1, MAX_PAGES_LIMIT + 1):
                if page > 1:
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/fans?page={page}'
                        self.driver.get(next_page_url)
                        time.sleep(random.uniform(0.3, 1.0))
                    except Exception as e:
                        break
                
                # 快速查找粉丝链接
                try:
                    fan_elements = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/u/')]")
                    
                    page_fans = []
                    processed_ids = set()
                    
                    for element in fan_elements:
                        try:
                            fan_href = element.get_attribute('href')
                            fan_name = element.text.strip()
                            
                            if fan_href and '/u/' in fan_href:
                                fan_id = fan_href.split('/u/')[-1].split('?')[0].split('/')[0]
                                
                                if fan_id.isdigit() and fan_id not in processed_ids and fan_name:
                                    page_fans.append({
                                        'id': fan_id,
                                        'screen_name': fan_name,
                                        'followers_count': 0,
                                        'friends_count': 0,
                                        'statuses_count': 0,
                                        'verified': False,
                                        'description': ''
                                    })
                                    processed_ids.add(fan_id)
                        except Exception as e:
                            continue
                    
                    if len(page_fans) == 0:
                        consecutive_empty_pages += 1
                        # 🔥 优化：连续2页无数据就停止（从8改为2）
                        if consecutive_empty_pages >= CONSECUTIVE_EMPTY_THRESHOLD:
                            print(f"    连续 {consecutive_empty_pages} 页无数据，停止爬取")
                            break
                    else:
                        consecutive_empty_pages = 0
                        fans_data.extend(page_fans)
                    
                    # 🔥 重要：只有真正达到目标数量才停止
                    if len(fans_data) >= target_sample_size:
                        print(f"    已获取 {len(fans_data)} 个粉丝，达到目标数量，停止爬取")
                        break
                        
                except Exception as e:
                    consecutive_empty_pages += 1
                    # 🔥 优化：连续2页异常也停止
                    if consecutive_empty_pages >= CONSECUTIVE_EMPTY_THRESHOLD:
                        break
                
                # 大幅减少等待时间
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                # 减少额外等待的频率和时间
                if page % 25 == 0:
                    time.sleep(random.uniform(0.5, 2.0))
                    print(f"    已爬取 {page} 页，获得 {len(fans_data)} 个粉丝")
            
            # 截取到目标数量
            if len(fans_data) > target_sample_size:
                fans_data = fans_data[:target_sample_size]
            
            print(f"  ✅ 最终获取 {len(fans_data)} 个粉丝 (目标: {target_sample_size})")
            
            return fans_data
            
        except Exception as e:
            return []
    
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()

def ensure_dir(directory):
    """确保目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_progress(seed_user_id):
    """保存爬取进度（增强版，更频繁保存）"""
    progress_file = PROGRESS_FILE_TEMPLATE.format(seed_user_id)
    ensure_dir(os.path.dirname(progress_file))
    
    # 保存完整进度
    progress_data = {
        "users": users_data,
        "edges": edges_data,
        "popularity": popularity_data,
        "processed": list(processed_users),
        "categories": {k: list(v) for k, v in node_categories.items()},
        "high_fans_users": high_fans_users,
        "save_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_users": len(users_data),
        "total_edges": len(edges_data)
    }
    
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 进度已保存: 用户 {len(users_data)} 个, 边 {len(edges_data)} 条")

def save_high_fans_report(output_dir):
    """保存超过上限的用户报告"""
    if not high_fans_users:
        print(f"\n📊 无超过上限 {HIGH_FANS_THRESHOLD} 的用户")
        return
    
    ensure_dir(output_dir)
    
    # 保存JSON格式
    report_file = f'{output_dir}/high_fans_users_report.json'
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(high_fans_users, f, ensure_ascii=False, indent=2)
    
    # 保存CSV格式便于查看
    df_data = []
    for user_id, info in high_fans_users.items():
        df_data.append({
            'user_id': user_id,
            'actual_fans_count': info['actual_fans_count'],
            'crawled_count': info['crawled_count'],
            'coverage_ratio': f"{info['coverage_ratio']:.4f}",
            'timestamp': info['timestamp']
        })
    
    df = pd.DataFrame(df_data)
    df.to_csv(f'{output_dir}/high_fans_users_report.csv', index=False, encoding='utf-8-sig')
    
    # 保存TXT格式（便于快速查看）
    with open(f'{output_dir}/high_fans_users_summary.txt', 'w', encoding='utf-8') as f:
        f.write(f"高粉丝用户总结报告\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"上限阈值: {HIGH_FANS_THRESHOLD} 粉丝\n")
        f.write(f"超过上限用户数: {len(high_fans_users)}\n\n")
        
        f.write("详细列表:\n")
        for user_id, info in sorted(high_fans_users.items(), key=lambda x: x[1]['actual_fans_count'], reverse=True):
            f.write(f"用户 {user_id}: {info['actual_fans_count']:,} 粉丝 → 爬取 {info['crawled_count']} 个 ({info['coverage_ratio']:.2%})\n")
    
    print(f"\n📊 高粉丝用户报告已保存:")
    print(f"   JSON: {report_file}")
    print(f"   CSV: {output_dir}/high_fans_users_report.csv")
    print(f"   TXT: {output_dir}/high_fans_users_summary.txt")
    print(f"   共 {len(high_fans_users)} 个用户粉丝数超过 {HIGH_FANS_THRESHOLD}")

def load_progress(seed_user_id):
    """加载爬取进度（增强版）"""
    global processed_users, users_data, edges_data, popularity_data, node_categories, high_fans_users
    
    progress_file = PROGRESS_FILE_TEMPLATE.format(seed_user_id)
    
    if not os.path.exists(progress_file):
        print("未找到进度文件，从头开始爬取")
        reset_global_data()
        return
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        processed_users = set(data.get("processed", []))
        users_data = data.get("users", {})
        edges_data = data.get("edges", [])
        popularity_data = data.get("popularity", {})
        high_fans_users = data.get("high_fans_users", {})
        
        if "categories" in data:
            for k, v in data["categories"].items():
                node_categories[k] = set(v)
        
        save_time = data.get("save_timestamp", "未知")
        print(f"✅ 已加载进度 (保存于 {save_time}):")
        print(f"   用户: {len(users_data)} 个")
        print(f"   边: {len(edges_data)} 条") 
        print(f"   已处理用户: {len(processed_users)} 个")
        if high_fans_users:
            print(f"   高粉丝用户: {len(high_fans_users)} 个")
        
    except Exception as e:
        print(f"❌ 加载进度文件出错: {e}，从头开始")
        reset_global_data()

def reset_global_data():
    """重置全局数据"""
    global processed_users, users_data, edges_data, popularity_data, node_categories, high_fans_users
    processed_users = set()
    users_data = {}
    edges_data = []
    popularity_data = {}
    node_categories = {"A": set(), "B": set(), "C": set()}
    high_fans_users = {}

def process_user_fans(crawler, user_id, category=None):
    """处理单个用户的粉丝列表（正确的C类边过滤逻辑）"""
    global processed_users, users_data, edges_data, popularity_data, node_categories
    
    if user_id in processed_users:
        return 0
    
    processed_users.add(user_id)
    
    if category and category in node_categories:
        node_categories[category].add(user_id)
    
    # 爬取粉丝列表
    fans_users = crawler.crawl_user_fans(user_id)
    
    if not fans_users:
        return 0
    
    # 🔥 关键：获取当前所有ABC类用户（用于C类边过滤）
    all_abc_users = node_categories["A"].union(node_categories["B"]).union(node_categories["C"])
    
    # 添加用户数据和边数据
    valid_edges_added = 0
    total_fans_found = len(fans_users)
    
    for fan in fans_users:
        fan_id = str(fan.get('id'))
        
        # 添加粉丝用户信息
        if fan_id not in users_data:
            users_data[fan_id] = {
                'screen_name': fan.get('screen_name', ''),
                'followers_count': fan.get('followers_count', 0),
                'friends_count': fan.get('friends_count', 0),
                'statuses_count': fan.get('statuses_count', 0),
                'verified': fan.get('verified', False),
                'description': fan.get('description', '')
            }
        
        # 🔥 关键修复：C类用户边过滤逻辑
        edge = (fan_id, user_id)
        
        if category == "C":
            # 🎯 C类用户：只有当粉丝是ABC类用户时才添加边
            if fan_id in all_abc_users:
                if edge not in edges_data:
                    edges_data.append(edge)
                    valid_edges_added += 1
            # 否则忽略这条边（D类用户，三跳以外）
        else:
            # A类和B类用户：添加所有边
            if edge not in edges_data:
                edges_data.append(edge)
                valid_edges_added += 1
    
    # 添加当前用户信息（如果还没有）
    if user_id not in users_data:
        users_data[user_id] = {
            'screen_name': f'用户{user_id}',
            'followers_count': 0,
            'friends_count': 0,
            'statuses_count': 0,
            'verified': False,
            'description': ''
        }
    
    if category == "C":
        print(f"    C类用户 {user_id}: 爬取 {total_fans_found} 个粉丝，有效边(指向ABC) {valid_edges_added} 条")
    
    return total_fans_found

def process_batch_fans(crawler, users_to_process, category=None):
    """批量处理用户粉丝（更频繁保存进度）"""
    if not users_to_process:
        return
    
    print(f"\n开始批量处理 {len(users_to_process)} 个{category}类用户（优化模式：连续{CONSECUTIVE_EMPTY_THRESHOLD}页无数据即停止）")
    if category == "C":
        print("⚠️  C类用户边过滤：只保留指向ABC类用户的边，忽略D类用户边")
    
    consecutive_zeros = 0
    
    for i, user_id in enumerate(users_to_process):
        try:
            print(f"\n处理 [{i+1}/{len(users_to_process)}] 用户 {user_id}:")
            fans_count = process_user_fans(crawler, user_id, category)
            
            print(f"  ✅ 用户 {user_id}: {fans_count} 粉丝")
            
            # 检查反爬机制
            if fans_count == 0:
                consecutive_zeros += 1
            else:
                consecutive_zeros = 0
            
            if consecutive_zeros >= ZERO_FANS_THRESHOLD:
                print(f"\n⚠️ 检测到连续 {consecutive_zeros} 个用户粉丝数为0，休息20秒...")
                time.sleep(20)
                consecutive_zeros = 0
        
        except Exception as e:
            print(f"处理用户 {user_id} 时出错: {e}")
        
        # 🔥 更频繁的进度保存：每10个用户保存一次
        if (i + 1) % 10 == 0:
            save_progress(seed_user_id)
            print(f"  进度已保存 ({i+1}/{len(users_to_process)})")
        
        # 进一步减少批次间等待
        if i < len(users_to_process) - 1:
            wait_time = random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
            if wait_time > 2.5:
                print(f"等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)

def save_network_data(output_dir):
    """保存网络数据"""
    ensure_dir(output_dir)
    
    # 保存用户数据
    users_df = pd.DataFrame.from_dict(users_data, orient='index')
    users_df.index.name = 'user_id'
    users_df.reset_index(inplace=True)
    users_df.to_csv(f'{output_dir}/users.csv', index=False, encoding='utf-8-sig')
    
    # 保存边数据（粉丝关系）
    edges_df = pd.DataFrame(edges_data, columns=['source', 'target'])
    edges_df.to_csv(f'{output_dir}/edges.csv', index=False, encoding='utf-8-sig')
    
    # 保存流行度数据（如果有）
    if popularity_data:
        popularity_df = pd.DataFrame.from_dict(popularity_data, orient='index')
        popularity_df.index.name = 'user_id'
        popularity_df.reset_index(inplace=True)
        popularity_df.to_csv(f'{output_dir}/popularity.csv', index=False, encoding='utf-8-sig')
    
    # 保存节点类别
    with open(f'{output_dir}/node_categories.json', 'w', encoding='utf-8') as f:
        json.dump({k: list(v) for k, v in node_categories.items()}, f, ensure_ascii=False)
    
    # 保存高粉丝用户报告
    save_high_fans_report(output_dir)

def main():
    """主函数"""
    global seed_user_id
    
    start_time = datetime.now()
    print(f"粉丝网络爬取开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("微博粉丝网络爬取器 v14.0 (速度优化版)")
    print("- 无放缩策略: 完整爬取所有粉丝，上限500")
    print(f"- 速度优化: 连续{CONSECUTIVE_EMPTY_THRESHOLD}页无数据即停止，最大{MAX_PAGES_LIMIT}页")
    print("- 确保C类用户边过滤逻辑正确：只保留ABC边")
    print("- 增强断点续传功能，每10个用户保存一次")
    print("- 完整的ABC类二跳粉丝网络")
    print("=" * 80)
    
    # 使用配置中的目标用户ID
    seed_user_id = TARGET_USER_ID
    print(f"\n目标用户ID: {seed_user_id}")
    print(f"策略: 粉丝数≤{MAX_FANS_LIMIT}完整爬取，>{MAX_FANS_LIMIT}爬取前{MAX_FANS_LIMIT}个")
    print(f"高粉丝用户阈值: {HIGH_FANS_THRESHOLD} 粉丝")
    print(f"🎯 二跳网络要求: 节点只包含ABC类，边只包含ABC之间的有向边")
    print(f"⚡ 速度优化: 最大{MAX_PAGES_LIMIT}页，连续{CONSECUTIVE_EMPTY_THRESHOLD}页无数据即停止")
    
    # 确保输出目录存在
    ensure_dir(BASE_OUTPUT_DIR)
    
    # 初始化爬虫
    crawler = WeiboFansCrawler()
    if not crawler.setup_driver():
        return
    
    if not crawler.load_cookies():
        print("请先运行 get_cookie.py 获取cookie")
        crawler.cleanup()
        return
    
    # 测试登录状态
    if not crawler.test_login_status():
        print("登录状态检查失败，请检查cookie是否有效")
        crawler.cleanup()
        return
    
    try:
        # 设置输出目录
        output_dir = f'{BASE_OUTPUT_DIR}/user_{seed_user_id}'
        
        # 加载进度或重置数据
        load_progress(seed_user_id)
        
        # 将种子用户标记为A类
        node_categories["A"].add(seed_user_id)
        
        # 第一阶段：爬取种子用户的粉丝(B类)
        if seed_user_id not in processed_users:
            print(f"\n=== 第一阶段：爬取种子用户 {seed_user_id} 的粉丝(B类) ===")
            fans_count = process_user_fans(crawler, seed_user_id, "A")
            print(f"种子用户 {seed_user_id} 获得 {fans_count} 个粉丝")
            save_progress(seed_user_id)
        else:
            print(f"\n=== 第一阶段：种子用户 {seed_user_id} 已处理过，跳过 ===")
        
        # 动态更新B类用户（种子用户的粉丝）
        b_users = set()
        for source, target in edges_data:
            if target == seed_user_id:
                b_users.add(source)
                node_categories["B"].add(source)
        
        print(f"\n=== 第二阶段：爬取B类用户的粉丝(C类) ===")
        print(f"发现B类用户: {len(b_users)} 个")
        
        # 过滤已处理的B类用户
        b_users_to_process = [u for u in b_users if u not in processed_users]
        print(f"需要处理的B类用户: {len(b_users_to_process)} 个")
        
        if b_users_to_process:
            process_batch_fans(crawler, b_users_to_process, "B")
            save_progress(seed_user_id)
        else:
            print("所有B类用户已处理完成")
        
        # 动态更新C类用户（B类用户的粉丝，但不是A或B类）
        a_and_b_users = node_categories["A"].union(node_categories["B"])
        c_users = set()
        
        for source, target in edges_data:
            if target in node_categories["B"] and source not in a_and_b_users:
                c_users.add(source)
                node_categories["C"].add(source)
        
        print(f"\n=== 第三阶段：爬取C类用户的粉丝（补全边，只保留ABC边） ===")
        print(f"发现C类用户: {len(c_users)} 个")
        print(f"⚠️ 重要：C类用户的粉丝边只有指向ABC类用户时才会被保留")
        
        # 过滤已处理的C类用户
        c_users_to_process = [u for u in c_users if u not in processed_users]
        print(f"需要处理的C类用户: {len(c_users_to_process)} 个")
        
        if c_users_to_process:
            process_batch_fans(crawler, c_users_to_process, "C")
        else:
            print("所有C类用户已处理完成")
        
        # 保存最终数据
        save_network_data(output_dir)
        
        # 统计最终结果
        final_a_count = len(node_categories["A"])
        final_b_count = len(node_categories["B"])
        final_c_count = len(node_categories["C"])
        
        print(f"\n{'='*80}")
        print(f"用户 {seed_user_id} 的粉丝网络爬取完成!")
        print(f"{'='*80}")
        print(f"总节点数: {len(users_data)}, 总边数: {len(edges_data)}")
        print(f"节点分布: A类(种子): {final_a_count}, B类(粉丝): {final_b_count}, C类(粉丝的粉丝): {final_c_count}")
        print(f"数据已保存到: {output_dir}")
        
        # 验证边的完整性
        print(f"\n=== 边完整性验证 ===")
        edge_stats = {"B→A": 0, "C→B": 0, "C→A": 0, "C→C": 0, "其他": 0}
        
        for source, target in edges_data:
            if source in node_categories["B"] and target in node_categories["A"]:
                edge_stats["B→A"] += 1
            elif source in node_categories["C"] and target in node_categories["B"]:
                edge_stats["C→B"] += 1
            elif source in node_categories["C"] and target in node_categories["A"]:
                edge_stats["C→A"] += 1
            elif source in node_categories["C"] and target in node_categories["C"]:
                edge_stats["C→C"] += 1
            else:
                edge_stats["其他"] += 1
        
        for edge_type, count in edge_stats.items():
            print(f"{edge_type} 边数: {count}")
        
        # 显示无放缩效果
        if high_fans_users:
            print(f"\n📊 高粉丝用户统计:")
            print(f"   超过上限用户数: {len(high_fans_users)}")
            print(f"   详细报告: {output_dir}/high_fans_users_report.csv")
            
            # 显示几个示例
            sample_users = list(high_fans_users.items())[:3]
            for user_id, info in sample_users:
                actual = info['actual_fans_count']
                crawled = info['crawled_count']
                ratio = info['coverage_ratio']
                print(f"   用户 {user_id}: {actual:,} 粉丝 → 爬取{crawled} ({ratio:.2%})")
        
        print(f"✅ 完整的ABC类二跳粉丝网络构建完成！")
        print(f"✅ C类边过滤已正确实现：只保留指向ABC类用户的边")
        print(f"✅ 断点续传功能已增强：每10个用户保存一次进度")
        print(f"⚡ 速度优化已应用：连续{CONSECUTIVE_EMPTY_THRESHOLD}页无数据即停止，节省大量时间")
        
    finally:
        crawler.cleanup()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n总耗时: {duration}")

if __name__ == "__main__":
    main()