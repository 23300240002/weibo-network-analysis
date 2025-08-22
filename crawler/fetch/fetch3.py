import os
import json
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import re

# 配置参数
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/domain_network3'
PROGRESS_FILE_TEMPLATE = 'C:/Tengfei/data/crawler/fetch/progress_fans3_{}.json'

# 爬取限制参数
MAX_FANS_PER_PAGE = 10
MAX_PAGES_LIMIT = 20

# 高粉丝用户判断标准
HIGH_FANS_ACTUAL_THRESHOLD = 100
HIGH_FANS_DISPLAY_THRESHOLD = 400

# 速度参数
SLEEP_MIN = 0.4
SLEEP_MAX = 0.6
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.0
CONSECUTIVE_EMPTY_THRESHOLD = 2

# 反爬检测参数
CONSECUTIVE_ZERO_FANS_THRESHOLD = 3

# 流行度计算参数
MAX_POSTS_FOR_POPULARITY = 10

# ===== 配置部分：修改这里的用户ID =====
TARGET_USER_ID = "3855570307"
# =========================================

# 全局变量
processed_users = set()
users_data = {}
edges_data = []
edges_set = set()
node_categories = {"A": set(), "B": set(), "C": set()}  # 🔥 修改：移除D类
high_fans_users = set()
seed_user_id = None
consecutive_zero_fans_count = 0
popularity_data = {}

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
            time.sleep(2)
            
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    pass
            
            self.driver.refresh()
            time.sleep(2)
            print("Cookie加载成功")
            return True
            
        except Exception as e:
            print(f"❌ Cookie加载失败: {e}")
            return False

    def test_login_status(self):
        """测试登录状态"""
        try:
            self.driver.get('https://weibo.cn')
            time.sleep(2)
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
        """获取用户在微博显示的真实粉丝数（用于判断是否为高粉丝用户）"""
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            self.driver.get(profile_url)
            time.sleep(random.uniform(0.2, 0.8))
            
            page_source = self.driver.page_source
            
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
    
    def crawl_user_fans(self, user_id):
        """爬取用户的粉丝列表（受微博20页限制）"""
        print(f"  开始爬取用户 {user_id} 的粉丝...")
        
        try:
            fans_url = f'https://weibo.cn/{user_id}/fans'
            self.driver.get(fans_url)
            time.sleep(0.5)
            
            page_source = self.driver.page_source
            if '用户不存在' in page_source or '登录' in page_source:
                print(f"  ❌ 用户 {user_id} 不存在或需要登录")
                return []
            
            fans_data = []
            consecutive_empty_pages = 0
            
            for page in range(1, MAX_PAGES_LIMIT + 1):
                if page > 1:
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/fans?page={page}'
                        self.driver.get(next_page_url)
                        time.sleep(random.uniform(0.7, 1.0))
                    except Exception as e:
                        break
                
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
                                        'screen_name': fan_name
                                    })
                                    processed_ids.add(fan_id)
                        except Exception as e:
                            continue
                    
                    if len(page_fans) == 0:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= CONSECUTIVE_EMPTY_THRESHOLD:
                            break
                    else:
                        consecutive_empty_pages = 0
                        fans_data.extend(page_fans)
                        
                except Exception as e:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= CONSECUTIVE_EMPTY_THRESHOLD:
                        break
                
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                if page % 10 == 0 and page > 0:
                    extra_wait = random.uniform(0.6, 1.0)
                    print(f"    已爬取 {page} 页，额外等待 {extra_wait:.1f} 秒防反爬...")
                    time.sleep(extra_wait)
            
            actual_fans_count = len(fans_data)
            print(f"  ✅ 用户 {user_id} 实际爬取到 {actual_fans_count} 个粉丝")
            
            return fans_data
            
        except Exception as e:
            print(f"  ❌ 爬取用户 {user_id} 粉丝时出错: {e}")
            return []
    
    # 🔥 修复版流行度计算相关方法
    def extract_interactions_from_html(self, html_text):
        """从HTML文本中提取转赞评数据"""
        interactions = {'reposts': 0, 'likes': 0, 'comments': 0}
        
        patterns = {
            'likes': [r'赞\[(\d+)\]'],
            'reposts': [r'转发\[(\d+)\]'],
            'comments': [r'评论\[(\d+)\]']
        }
        
        for interaction_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                matches = re.findall(pattern, html_text)
                if matches:
                    try:
                        # 取最后一个匹配（用户自己的数据）
                        num = int(matches[-1])
                        interactions[interaction_type] = num
                    except:
                        continue
        
        return interactions
    
    def extract_time_from_html(self, html_text):
        """从HTML文本中提取时间"""
        time_patterns = [
            r'(\d{2}月\d{2}日 \d{2}:\d{2})',
            r'(\d+分钟前)',
            r'(\d+小时前)',
            r'(\d+天前)',
            r'(昨天 \d{1,2}:\d{2})',
            r'(今天 \d{1,2}:\d{2})',
        ]
        
        for pattern in time_patterns:
            matches = re.findall(pattern, html_text)
            if matches:
                return matches[-1]  # 取最后一个匹配
        
        return "时间未找到"
    
    def process_single_weibo_div(self, weibo_div_element):
        """🔥 修正版：使用最后一个div提取用户自己的转赞评数据"""
        try:
            # 获取微博div的HTML
            div_html = weibo_div_element.get_attribute('outerHTML')
            
            # 查找微博div内的子div元素
            child_divs = weibo_div_element.find_elements(By.XPATH, "./div")
            
            # 提取微博内容
            content = "内容未提取"
            try:
                ctt_element = weibo_div_element.find_element(By.CLASS_NAME, "ctt")
                content = ctt_element.text.strip()
                content = content[:100] + ('...' if len(content) > 100 else '')
            except:
                pass
            
            # 判断是否为转发微博
            is_repost = '转发了' in div_html
            
            interactions = {'reposts': 0, 'likes': 0, 'comments': 0}
            post_time = "时间未找到"
            
            # 🔥 关键修正：始终使用最后一个div（无论转发还是原创）
            if len(child_divs) > 0:
                last_div = child_divs[-1]  # 取最后一个div
                last_div_html = last_div.get_attribute('outerHTML')
                
                # 从最后一个div提取用户自己的转赞评数据
                interactions = self.extract_interactions_from_html(last_div_html)
                
                # 从最后一个div提取时间
                post_time = self.extract_time_from_html(last_div_html)
            else:
                # 没有子div，使用整个div
                interactions = self.extract_interactions_from_html(div_html)
                post_time = self.extract_time_from_html(div_html)
            
            return {
                'content': content,
                'time': post_time,
                'interactions': interactions,
                'total_interactions': sum(interactions.values()),
                'is_repost': is_repost
            }
            
        except Exception as e:
            return None
    
    def calculate_user_popularity(self, user_id, max_posts=MAX_POSTS_FOR_POPULARITY):
        """🔥 修正版：计算用户流行度 - 基于最新max_posts条微博"""
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            self.driver.get(profile_url)
            time.sleep(2)
            
            # 使用Selenium直接查找微博div元素
            weibo_divs = self.driver.find_elements(By.XPATH, "//div[@class='c' and contains(@id, 'M_')]")
            
            if not weibo_divs:
                return 0.0
            
            posts_data = []
            
            for i, weibo_div in enumerate(weibo_divs):
                if len(posts_data) >= max_posts:
                    break
                
                # 🔥 使用修正版处理方法
                post_data = self.process_single_weibo_div(weibo_div)
                
                if post_data and post_data['content'] != "内容未提取":
                    posts_data.append(post_data)
            
            if not posts_data:
                return 0.0
            
            # 计算平均流行度
            total_interactions = 0
            valid_posts = len(posts_data)
            
            for post in posts_data:
                interactions = post['interactions']
                post_total = interactions['likes'] + interactions['reposts'] + interactions['comments']
                total_interactions += post_total
            
            avg_popularity = total_interactions / valid_posts if valid_posts > 0 else 0.0
            
            return avg_popularity
                
        except Exception as e:
            print(f"  ⚠️ 计算用户 {user_id} 流行度失败: {e}")
            return 0.0
    
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()

def ensure_dir(directory):
    """确保目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_progress(seed_user_id):
    """保存爬取进度"""
    progress_file = PROGRESS_FILE_TEMPLATE.format(seed_user_id)
    ensure_dir(os.path.dirname(progress_file))
    
    progress_data = {
        "users": users_data,
        "edges": edges_data,
        "processed": list(processed_users),
        "categories": {k: list(v) for k, v in node_categories.items()},
        "high_fans_users": list(high_fans_users),
        "consecutive_zero_fans_count": consecutive_zero_fans_count,
        "popularity": popularity_data,
        "save_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_users": len(users_data),
        "total_edges": len(edges_data)
    }
    
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    current_size = get_current_network_size()
    print(f"✅ 进度已保存: 用户 {len(users_data)} 个, 边 {len(edges_data)} 条, 网络规模 {current_size}")

def load_progress(seed_user_id):
    """加载爬取进度"""
    global processed_users, users_data, edges_data, edges_set, node_categories, high_fans_users, consecutive_zero_fans_count, popularity_data
    # 🔥 修改：移除D类相关全局变量
    
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
        high_fans_users = set(data.get("high_fans_users", []))
        # 🔥 修改：移除D类数据加载
        # reserved_d_users = set(data.get("reserved_d_users", []))
        # d_users_quota = data.get("d_users_quota", 0)
        consecutive_zero_fans_count = data.get("consecutive_zero_fans_count", 0)
        popularity_data = data.get("popularity", {})
        
        # 重建边集合用于快速去重
        edges_set = set(tuple(edge) if isinstance(edge, list) else edge for edge in edges_data)
        
        if "categories" in data:
            for k, v in data["categories"].items():
                if k in node_categories:  # 🔥 修改：只加载ABC类
                    node_categories[k] = set(v)
        
        save_time = data.get("save_timestamp", "未知")
        current_size = get_current_network_size()
        print(f"✅ 已加载进度 (保存于 {save_time}): 用户 {len(users_data)} 个, 边 {len(edges_data)} 条, 网络规模 {current_size}")
        if consecutive_zero_fans_count > 0:
            print(f"⚠️ 反爬检测状态: 已连续遇到 {consecutive_zero_fans_count} 个0粉丝用户")
        if popularity_data:
            print(f"✅ 已加载 {len(popularity_data)} 个用户的流行度数据")
        
    except Exception as e:
        print(f"❌ 加载进度文件出错: {e}，从头开始")
        reset_global_data()

def reset_global_data():
    """重置全局数据"""
    global processed_users, users_data, edges_data, edges_set, node_categories, high_fans_users, consecutive_zero_fans_count, popularity_data
    # 🔥 修改：移除D类相关变量重置
    processed_users = set()
    users_data = {}
    edges_data = []
    edges_set = set()
    node_categories = {"A": set(), "B": set(), "C": set()}  # 只保留ABC类
    high_fans_users = set()
    # reserved_d_users = set()
    # d_users_quota = 0
    consecutive_zero_fans_count = 0
    popularity_data = {}

def check_anti_crawl_status(fans_count, user_id):
    """检查反爬状态"""
    global consecutive_zero_fans_count
    
    if fans_count == 0:
        consecutive_zero_fans_count += 1
        print(f"  ⚠️ 连续0粉丝用户计数: {consecutive_zero_fans_count}/{CONSECUTIVE_ZERO_FANS_THRESHOLD}")
        
        if consecutive_zero_fans_count >= CONSECUTIVE_ZERO_FANS_THRESHOLD:
            print(f"\n🚨 检测到反爬机制！")
            print(f"🚨 连续 {consecutive_zero_fans_count} 个用户粉丝数为0，可能被微博反爬")
            print(f"🚨 为了安全起见，程序将自动终止")
            print(f"🚨 最后处理的用户: {user_id}")
            print(f"🚨 请稍后重新启动程序，或更换Cookie后继续")
            
            # 保存当前进度
            save_progress(seed_user_id)
            
            # 抛出异常以终止程序
            raise Exception(f"ANTI_CRAWL_DETECTED: 连续{consecutive_zero_fans_count}个用户0粉丝")
    else:
        # 重置计数器
        consecutive_zero_fans_count = 0

def process_user_fans(crawler, user_id, category):
    """🔥 修改版：处理单个用户的粉丝列表 - 移除D类逻辑"""
    global processed_users, users_data, edges_data, edges_set, node_categories, popularity_data
    
    if user_id in processed_users:
        return 0, set()
    
    processed_users.add(user_id)
    
    if category and category in node_categories:
        node_categories[category].add(user_id)
    
    # 🔥 计算用户流行度
    if user_id not in popularity_data:
        print(f"  🎯 计算用户 {user_id} 的流行度...")
        avg_popularity = crawler.calculate_user_popularity(user_id, MAX_POSTS_FOR_POPULARITY)
        popularity_data[user_id] = avg_popularity
        print(f"  ✅ 用户 {user_id} 平均流行度: {avg_popularity:.2f}")
    
    # 爬取粉丝列表
    fans_users = crawler.crawl_user_fans(user_id)
    
    # 反爬检测
    fans_count = len(fans_users) if fans_users else 0
    check_anti_crawl_status(fans_count, user_id)
    
    if not fans_users:
        return 0, set()
    
    # 获取微博显示的粉丝数，用于高粉丝用户判断
    true_fans_count = crawler.get_user_fans_count(user_id)
    actual_fans_count = len(fans_users)
    
    # 高粉丝用户判断逻辑
    if actual_fans_count > HIGH_FANS_ACTUAL_THRESHOLD and true_fans_count > HIGH_FANS_DISPLAY_THRESHOLD:
        high_fans_users.add(user_id)
        print(f"  ⚠️ 用户 {user_id} 满足高粉丝标准：实际爬取{actual_fans_count}>100 且 微博显示{true_fans_count:,}>400")
    
    # 🔥 修改：确保当前处理用户的完整信息被正确更新，并添加类别信息
    existing_screen_name = users_data.get(user_id, {}).get('screen_name', f'用户{user_id}')
    
    users_data[user_id] = {
        'screen_name': existing_screen_name,
        'display_fans_count': true_fans_count,
        'actual_fans_count': actual_fans_count,
        'avg_popularity': popularity_data.get(user_id, 0.0),
        'category': category  # 🔥 新增：用户类别信息
    }
    
    valid_edges_added = 0
    new_users_discovered = set()
    ca_edges_added = 0
    cb_edges_added = 0
    cc_edges_added = 0
    
    for fan in fans_users:
        fan_id = str(fan.get('id'))
        
        # 🔥 修改：边的添加逻辑 - 移除D类相关逻辑
        edge = (user_id, fan_id)  # user_id（博主）→ fan_id（粉丝）
        should_add_edge = False
        
        if category == "A":
            # A类用户：添加所有边（这些将成为A→B边）
            should_add_edge = True
            # 将新发现的粉丝标记为B类
            if fan_id not in node_categories["A"]:
                node_categories["B"].add(fan_id)
                # 🔥 修改：只为ABC类用户创建用户数据
                if fan_id not in users_data:
                    users_data[fan_id] = {
                        'screen_name': fan.get('screen_name', ''),
                        'display_fans_count': 0,
                        'actual_fans_count': 0,
                        'avg_popularity': 0.0,
                        'category': 'B'  # 🔥 新增：类别信息
                    }
        elif category == "B":
            # B类用户：添加所有边，新发现的用户成为C类
            should_add_edge = True
            # 将新发现的粉丝标记为C类（如果不是A或B类）
            if fan_id not in node_categories["A"] and fan_id not in node_categories["B"]:
                node_categories["C"].add(fan_id)
                new_users_discovered.add(fan_id)
                # 🔥 修改：只为ABC类用户创建用户数据
                if fan_id not in users_data:
                    users_data[fan_id] = {
                        'screen_name': fan.get('screen_name', ''),
                        'display_fans_count': 0,
                        'actual_fans_count': 0,
                        'avg_popularity': 0.0,
                        'category': 'C'  # 🔥 新增：类别信息
                    }
        elif category == "C":
            # 🔥 修改：C类用户处理逻辑 - 移除D类相关逻辑
            if fan_id in node_categories["A"]:
                # C→A边：直接添加
                should_add_edge = True
                ca_edges_added += 1
            elif fan_id in node_categories["B"]:
                # C→B边：直接添加
                should_add_edge = True
                cb_edges_added += 1
            elif fan_id in node_categories["C"]:
                # C→C边：直接添加
                should_add_edge = True
                cc_edges_added += 1
            # 🔥 修改：完全移除C→D边的逻辑
            # 对于指向不在ABC类中的粉丝，直接忽略，不添加边，也不创建用户数据
        
        # 使用集合进行快速边去重
        if should_add_edge and edge not in edges_set:
            edges_data.append(edge)
            edges_set.add(edge)
            valid_edges_added += 1
    
    total_fans_found = len(fans_users)
    
    if category == "C":
        print(f"    C类用户 {user_id}: 爬取 {total_fans_found} 个粉丝（微博显示: {true_fans_count:,}），有效边 {valid_edges_added} 条，流行度 {popularity_data.get(user_id, 0.0):.2f}")
        if ca_edges_added > 0 or cb_edges_added > 0 or cc_edges_added > 0:
            print(f"      └─ C→A: {ca_edges_added}, C→B: {cb_edges_added}, C→C: {cc_edges_added}")
    else:
        print(f"    {category}类用户 {user_id}: 爬取 {total_fans_found} 个粉丝（微博显示: {true_fans_count:,}），有效边 {valid_edges_added} 条，新用户 {len(new_users_discovered)} 个，流行度 {popularity_data.get(user_id, 0.0):.2f}")
    
    return total_fans_found, new_users_discovered

def get_current_network_size():
    """获取当前网络总人数"""
    return len(users_data)

def print_network_status():
    """打印当前网络状态"""
    current_size = get_current_network_size()
    a_count = len(node_categories["A"])
    b_count = len(node_categories["B"])
    c_count = len(node_categories["C"])
    
    # 🔥 修改：移除D类显示
    print(f"📊 网络状态: 总人数 {current_size}, A类 {a_count}, B类 {b_count}, C类 {c_count}, 边数 {len(edges_data)}")

def save_final_data(output_dir):
    """保存最终数据"""
    ensure_dir(output_dir)
    
    # 🔥 修改：过滤users_data，确保只包含ABC类用户
    abc_users = node_categories["A"].union(node_categories["B"]).union(node_categories["C"])
    filtered_users_data = {user_id: data for user_id, data in users_data.items() if user_id in abc_users}
    
    # 1. 保存用户数据（包含两个粉丝数字段、流行度和类别信息）
    users_df = pd.DataFrame.from_dict(filtered_users_data, orient='index')
    users_df.index.name = 'user_id'
    users_df.reset_index(inplace=True)
    
    # 🔥 新增：确保列顺序正确
    column_order = ['user_id', 'screen_name', 'display_fans_count', 'actual_fans_count', 'avg_popularity', 'category']
    users_df = users_df.reindex(columns=column_order)
    
    users_df.to_csv(f'{output_dir}/users.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 用户数据已保存: {len(users_df)} 个用户（只包含ABC类）")
    
    # 2. 保存边数据
    edges_df = pd.DataFrame(edges_data, columns=['source', 'target'])
    edges_df.to_csv(f'{output_dir}/edges.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 边数据已保存: {len(edges_df)} 条边")
    
    # 3. 保存流行度数据（只包含ABC类用户）
    abc_popularity_data = {user_id: popularity for user_id, popularity in popularity_data.items() if user_id in abc_users}
    popularity_df = pd.DataFrame.from_dict(abc_popularity_data, orient='index', columns=['avg_popularity'])
    popularity_df.index.name = 'user_id'
    popularity_df.reset_index(inplace=True)
    popularity_df.to_csv(f'{output_dir}/popularity.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 流行度数据已保存: {len(popularity_df)} 个用户（只包含ABC类）")
    
    # 4. 保存高粉丝用户数据
    high_fans_df = pd.DataFrame({'user_id': list(high_fans_users)})
    high_fans_df.to_csv(f'{output_dir}/high_fans_users.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 高粉丝用户数据已保存: {len(high_fans_df)} 个用户")
    
    # 🔥 新增：验证数据一致性
    print(f"\n📊 数据一致性验证:")
    print(f"  users.csv用户数: {len(users_df)}")
    print(f"  popularity.csv用户数: {len(popularity_df)}")
    print(f"  ABC类用户总数: {len(abc_users)}")
    if len(users_df) == len(popularity_df) == len(abc_users):
        print(f"  ✅ 数据一致性验证通过")
    else:
        print(f"  ⚠️ 数据一致性验证失败")

def main():
    """主函数"""
    global seed_user_id
    
    start_time = datetime.now()
    print(f"粉丝网络爬取开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("微博粉丝网络爬取器 v4.0 (ABC类专版)")
    print(f"- 边方向：博主→粉丝（体现影响力）")
    print(f"- 🔥 网络结构：只构建ABC类网络，移除D类用户逻辑")
    print(f"- 🔥 用户数据：users.csv只包含ABC类用户，与popularity.csv一致")
    print(f"- 🔥 新增字段：users.csv新增category列（A/B/C）")
    print(f"- 🔥 流行度计算：(转+赞+评)总数/微博数，基于最新{MAX_POSTS_FOR_POPULARITY}条微博")
    print(f"- 🔥 反爬检测：连续{CONSECUTIVE_ZERO_FANS_THRESHOLD}个用户0粉丝将自动终止程序")
    print("=" * 80)
    
    # 使用配置中的目标用户ID
    seed_user_id = TARGET_USER_ID
    print(f"\n目标用户ID: {seed_user_id}")
    
    # 确保输出目录存在
    ensure_dir(BASE_OUTPUT_DIR)
    
    # 初始化爬虫
    crawler = WeiboFansCrawler()
    if not crawler.setup_driver():
        return
    
    if not crawler.load_cookies():
        print("请先获取有效的cookie文件")
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
        discovered_b_users = set()
        if seed_user_id not in processed_users:
            print(f"\n=== 第一阶段：从A出发，发现所有B ===")
            fans_count, new_users = process_user_fans(crawler, seed_user_id, "A")
            discovered_b_users = new_users
            print(f"种子用户 {seed_user_id} 获得 {fans_count} 个粉丝")
            print_network_status()
            save_progress(seed_user_id)
        else:
            print(f"\n=== 第一阶段：种子用户 {seed_user_id} 已处理过，跳过 ===")
            # 从现有边中重新发现B类用户
            for source, target in edges_data:
                if source == seed_user_id:
                    discovered_b_users.add(target)
                    node_categories["B"].add(target)
        
        print(f"\n=== 第二阶段：遍历B，发现B→A、B→B边，记录C类用户 ===")
        print(f"发现B类用户: {len(node_categories['B'])} 个")
        print_network_status()
        
        # 处理B类用户
        b_users_to_process = [u for u in node_categories["B"] if u not in processed_users]
        print(f"需要处理的B类用户: {len(b_users_to_process)} 个")
        
        for i, user_id in enumerate(b_users_to_process):
            print(f"\n处理B类用户 [{i+1}/{len(b_users_to_process)}] {user_id}:")
            fans_count, new_users = process_user_fans(crawler, user_id, "B")
            
            if (i + 1) % 5 == 0:
                print_network_status()
                save_progress(seed_user_id)
                
                extra_wait = random.uniform(3.0, 6.0)
                print(f"  已处理{i+1}个B类用户，额外等待 {extra_wait:.1f} 秒防反爬...")
                time.sleep(extra_wait)
            
            time.sleep(random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))
        
        save_progress(seed_user_id)
        print_network_status()
        
        # 🔥 修改：第三阶段 - 移除D类配额计算
        print(f"\n=== 第三阶段：遍历C，发现C→A、C→B、C→C边 ===")
        print(f"发现C类用户: {len(node_categories['C'])} 个")
        print(f"⚠️ 注意：只构建ABC类网络，忽略指向D类的边")
        
        # 处理C类用户
        c_users_to_process = [u for u in node_categories["C"] if u not in processed_users]
        print(f"需要处理的C类用户: {len(c_users_to_process)} 个")
        
        for i, user_id in enumerate(c_users_to_process):
            print(f"\n处理C类用户 [{i+1}/{len(c_users_to_process)}] {user_id}:")
            fans_count, new_users = process_user_fans(crawler, user_id, "C")
            
            if (i + 1) % 5 == 0:
                print_network_status()
                save_progress(seed_user_id)
                
                extra_wait = random.uniform(3.0, 6.0)
                print(f"  已处理{i+1}个C类用户，额外等待 {extra_wait:.1f} 秒防反爬...")
                time.sleep(extra_wait)
            
            time.sleep(random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))
        
        save_progress(seed_user_id)
        print_network_status()
        
        # 🔥 修改：移除D类处理阶段
        print(f"\n=== ABC类网络构建完成 ===")
        print(f"无需处理D类用户，ABC类网络已完整")
        
        # 保存最终数据
        save_final_data(output_dir)
        
        # 统计最终结果
        final_a_count = len(node_categories["A"])
        final_b_count = len(node_categories["B"])
        final_c_count = len(node_categories["C"])
        final_network_size = get_current_network_size()
        
        print(f"\n{'='*80}")
        print(f"用户 {seed_user_id} 的ABC类粉丝网络爬取完成!")
        print(f"{'='*80}")
        print(f"最终网络规模: {final_network_size} 人（只包含ABC类）")
        print(f"总边数: {len(edges_data)} 条")
        print(f"节点分布:")
        print(f"  A类(种子): {final_a_count} 人")
        print(f"  B类(粉丝): {final_b_count} 人")
        print(f"  C类(粉丝的粉丝): {final_c_count} 人")
        print(f"高粉丝用户数量: {len(high_fans_users)} 个")
        print(f"🔥 流行度数据: {len(popularity_data)} 个用户")
        
        # 流行度统计
        if popularity_data:
            popularity_values = list(popularity_data.values())
            print(f"\n🔥 流行度统计:")
            print(f"  平均流行度: {sum(popularity_values)/len(popularity_values):.2f}")
            print(f"  最高流行度: {max(popularity_values):.2f}")
            print(f"  最低流行度: {min(popularity_values):.2f}")
        
        print(f"\n数据已保存到: {output_dir}")
        print(f"\n✅ ABC类粉丝网络构建完成！")
        print(f"✅ 边方向正确：博主→粉丝")
        print(f"✅ 🔥 网络精简：只包含ABC类用户，移除D类冗余")
        print(f"✅ 🔥 数据一致：users.csv与popularity.csv用户数一致")
        print(f"✅ 🔥 类别标识：users.csv新增category列（A/B/C）")
        print(f"✅ 🔥 数据完整性：同时获得网络结构和准确影响力")
        
    except Exception as e:
        if "ANTI_CRAWL_DETECTED" in str(e):
            print(f"\n程序因反爬检测而安全终止")
            print(f"当前进度已自动保存，可稍后重新启动程序继续")
        else:
            print(f"\n程序异常终止: {e}")
            save_progress(seed_user_id)
        
    finally:
        crawler.cleanup()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n总耗时: {duration}")

if __name__ == "__main__":
    main()