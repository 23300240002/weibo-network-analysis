import os
import json
import time
import random
import requests
import pandas as pd
import signal
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import re

# 配置参数
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/topic_networks'
COOKIE_PATH = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'
TARGET_TOPIC = "孙颖莎"  # 默认值，仅用于打印；实际运行由用户输入替代
MAX_NETWORK_SIZE = 5000  # 默认值，仅用于打印；实际运行由用户输入替代
CELEBRITY_FANS_THRESHOLD = 2000  # 保留但不再用于A/B跳过逻辑

# 新增阈值（按要求）
A_FANS_THRESHOLD_SKIP = 2000   # A类粉丝数>2000 跳过
B_FANS_THRESHOLD_SKIP = 1500   # B类粉丝数>1500 跳过扩边（第一阶段用于过滤拟入B；第二阶段不再判断）

MAX_PAGES_PER_USER = 20

# 速度参数
SLEEP_MIN = 0.4
SLEEP_MAX = 0.6
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.0

# 流行度计算参数
MAX_POSTS_FOR_POPULARITY = 10

# 全局变量（合并大网络）
node_categories = {"A": set(), "B": set()}
edges_data = []
edges_set = set()
users_data = {}
popularity_data = {}
processed_users = set()

# 运行态
crawler = None
output_dir = None            # 合并大网络目录
should_exit = False
topics_processed = []        # 记录已处理关键词（用于info）
topic_nodes_map = {}         # 每个关键词的用户集合（用于人数判断，现已持久化）
topic_plan = []              # 新增：保存每个关键词的计划与进度 [{topic, target, finished_first_phase, count_A, count_B, count_total}]

def signal_handler(signum, frame):
    """处理Ctrl+C信号"""
    global should_exit
    print("\n⚠️ 捕获到中断信号，准备安全退出...")
    should_exit = True
    if output_dir:
        save_progress(output_dir)
    if crawler:
        crawler.cleanup()
    print("✅ 进度已保存，资源已释放。")

class TopicNetworkCrawler:
    def __init__(self):
        self.driver_com = None
        self.driver_cn = None
        
    def setup_drivers(self):
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        try:
            self.driver_com = webdriver.Chrome(options=chrome_options)
            self.driver_com.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            chrome_options_cn = Options()
            chrome_options_cn.add_argument('--no-sandbox')
            chrome_options_cn.add_argument('--disable-dev-shm-usage')
            chrome_options_cn.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options_cn.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options_cn.add_argument('--user-data-dir=C:/temp/chrome_profile_cn')
            
            self.driver_cn = webdriver.Chrome(options=chrome_options_cn)
            self.driver_cn.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return True
        except Exception as e:
            print(f"❌ 浏览器设置失败: {e}")
            return False
    
    def load_cookies_cn(self):
        try:
            with open(COOKIE_PATH, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            
            self.driver_cn.get('https://weibo.cn')
            time.sleep(2)
            
            for cookie in cookies:
                try:
                    self.driver_cn.add_cookie(cookie)
                except:
                    pass
            
            self.driver_cn.refresh()
            time.sleep(2)
            return True
        except Exception as e:
            print(f"❌ Cookie加载失败: {e}")
            return False
    
    def get_topic_users(self, topic, max_users=200):
        """动态翻页直到获得足够用户或无法继续翻页（保留方法，供需要时使用）"""
        global should_exit
        print(f"获取话题 #{topic}# 的用户，目标数量: {max_users}")
        
        topic_encoded = requests.utils.quote(f"#{topic}#")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://weibo.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        cookies = {}
        try:
            with open(COOKIE_PATH, 'r', encoding='utf-8') as f:
                cookies_list = json.load(f)
            for cookie in cookies_list:
                cookies[cookie['name']] = cookie['value']
        except:
            pass
        
        all_user_ids = []
        seen_users = set()
        consecutive_empty_pages = 0
        max_consecutive_empty = 3
        
        page = 1
        while len(all_user_ids) < max_users:
            if should_exit:
                print("检测到中断信号，提前退出 get_topic_users")
                break
            print(f"  正在请求第{page}页...")
            
            if page == 1:
                page_url = f"https://s.weibo.com/weibo/{topic_encoded}"
            else:
                page_url = f"https://s.weibo.com/weibo/{topic_encoded}&page={page}"
            
            try:
                time.sleep(random.uniform(2, 4))
                response = requests.get(page_url, headers=headers, cookies=cookies, timeout=15)
                
                if response.status_code == 200:
                    page_user_ids = self.extract_users_from_page(response.text, seen_users)
                    if page_user_ids:
                        consecutive_empty_pages = 0
                        all_user_ids.extend(page_user_ids)
                        seen_users.update(page_user_ids)
                        print(f"    第{page}页新增 {len(page_user_ids)} 个，累计 {len(all_user_ids)}")
                    else:
                        consecutive_empty_pages += 1
                        print(f"    第{page}页无新用户，连续空页: {consecutive_empty_pages}")
                        if consecutive_empty_pages >= max_consecutive_empty:
                            print(f"    连续{max_consecutive_empty}页无新用户，停止翻页")
                            break
                else:
                    print(f"    状态码: {response.status_code}，继续重试")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_consecutive_empty:
                        break
                        
            except Exception as e:
                print(f"    请求异常: {e}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
            
            page += 1
            if page > 200:
                print("    达到最大页数200，停止翻页")
                break
        
        print(f"  共获取 {len(all_user_ids)} 个用户")
        return all_user_ids
    
    def get_users_by_browser_scroll_unlimited(self, topic, max_users, existing_users):
        """浏览器滚动补充用户"""
        try:
            topic_encoded = requests.utils.quote(f"#{topic}#")
            search_url = f"https://s.weibo.com/weibo/{topic_encoded}"
            self.driver_com.get(search_url)
            time.sleep(3)
            
            all_user_ids = []
            consecutive_no_new = 0
            max_consecutive_no_new = 5
            
            scroll_round = 0
            while len(all_user_ids) < max_users and consecutive_no_new < max_consecutive_no_new:
                scroll_round += 1
                print(f"    浏览器滚动第{scroll_round}轮...")
                
                current_page_source = self.driver_com.page_source
                current_users = self.extract_users_from_page(current_page_source, existing_users)
                
                new_users = [uid for uid in current_users if uid not in existing_users and uid not in all_user_ids]
                
                if new_users:
                    all_user_ids.extend(new_users)
                    existing_users.update(new_users)
                    consecutive_no_new = 0
                    print(f"      本轮新增 {len(new_users)} 个，累计 {len(all_user_ids)}")
                else:
                    consecutive_no_new += 1
                    print(f"      本轮无新增，连续无新增轮数: {consecutive_no_new}")
                
                self.driver_com.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3, 5))
                
                try:
                    more_buttons = self.driver_com.find_elements(By.XPATH, 
                        "//a[contains(text(), '更多') or contains(text(), 'more') or contains(@class, 'more')]")
                    if more_buttons:
                        more_buttons[0].click()
                        time.sleep(3)
                except:
                    pass
                
                if scroll_round >= 50:
                    print("      达到最大滚动轮数50，停止滚动")
                    break
            
            print(f"  浏览器滚动新增 {len(all_user_ids)} 个用户")
            return all_user_ids
            
        except Exception as e:
            print(f"  浏览器滚动模式失败: {e}")
            return []
    
    def extract_users_from_page(self, html_content, seen_users):
        """从HTML页面提取用户ID"""
        user_ids = []
        
        patterns = [
            r'href="//weibo\.com/(\d+)/[^"]*"',
            r'href="https?://weibo\.com/(\d+)/[^"]*"',
            r'href="//weibo\.com/u/(\d+)[^"]*"',
            r'href="https?://weibo\.com/u/(\d+)[^"]*"',
            r'"idstr":"(\d+)"',
            r'"user":\s*{\s*"id":(\d+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                if len(match) >= 8 and match.isdigit() and match not in seen_users:
                    user_ids.append(match)
        
        # 去重
        unique_users = []
        page_seen = set()
        for uid in user_ids:
            if uid not in page_seen:
                unique_users.append(uid)
                page_seen.add(uid)
        
        return unique_users
    
    def get_users_by_browser_scroll(self, topic, max_users):
        """（保留）浏览器滚动获取更多用户"""
        try:
            topic_encoded = requests.utils.quote(f"#{topic}#")
            search_url = f"https://s.weibo.com/weibo/{topic_encoded}"
            
            self.driver_com.get(search_url)
            time.sleep(3)
            
            all_user_ids = []
            seen_users = set()
            
            for scroll_round in range(8):
                current_page_source = self.driver_com.page_source
                current_users = self.extract_users_from_page(current_page_source, seen_users)
                
                for uid in current_users:
                    if uid not in seen_users and len(all_user_ids) < max_users:
                        all_user_ids.append(uid)
                        seen_users.add(uid)
                
                self.driver_com.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3, 5))
                
                try:
                    more_buttons = self.driver_com.find_elements(By.XPATH, 
                        "//a[contains(text(), '更多') or contains(text(), 'more') or contains(@class, 'more')]")
                    if more_buttons:
                        more_buttons[0].click()
                        time.sleep(3)
                except:
                    pass
                
                if len(current_users) == 0 and scroll_round > 2:
                    break
            
            return all_user_ids
        except:
            return []
    
    def check_user_fans_count(self, user_id):
        """检查用户粉丝数 - 基于weibo.com页面"""
        try:
            profile_url = f'https://weibo.com/u/{user_id}'
            self.driver_com.get(profile_url)
            time.sleep(random.uniform(2, 4))
            
            page_source = self.driver_com.page_source
            
            patterns = [
                r'<span[^>]*>([0-9]+\.?[0-9]*[万]?)</span>\s*粉丝',
                r'>([0-9]+\.?[0-9]*[万]?)</span>\s*粉丝',
                r'([0-9]+\.?[0-9]*[万]?)\s*粉丝',
                r'粉丝[^>]*>([0-9]+\.?[0-9]*[万]?)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_source)
                if matches:
                    fans_str = matches[0].strip()
                    if '万' in fans_str:
                        num_str = fans_str.replace('万', '')
                        fans_count = int(float(num_str) * 10000)
                    else:
                        fans_count = int(float(fans_str))
                    return fans_count
        except:
            pass
        return 0
    
    def crawl_user_fans_cn(self, user_id):
        """爬取用户粉丝（weibo.cn），并直接解析每个粉丝的粉丝数（粉丝X人 / 粉丝Y万人）"""
        import re
        try:
            def parse_cn_fans_count(text: str):
                # 仅接受“粉丝<数字>(万)?人”两种格式，其他一律视为无效（返回None）
                # 例：粉丝1人 / 粉丝2.3万人
                m = re.search(r'粉丝\s*([0-9]+(?:\.[0-9]+)?)\s*(万)?\s*人', text)
                if not m:
                    return None
                num = float(m.group(1))
                if m.group(2):  # 有“万”
                    return int(num * 10000)
                return int(num)

            fans_url = f'https://weibo.cn/{user_id}/fans'
            self.driver_cn.get(fans_url)
            time.sleep(0.5)

            page_source = self.driver_cn.page_source
            if '用户不存在' in page_source or '登录' in page_source:
                return []

            fans_data = []
            consecutive_empty_pages = 0

            for page in range(1, MAX_PAGES_PER_USER + 1):
                if page > 1:
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/fans?page={page}'
                        self.driver_cn.get(next_page_url)
                        time.sleep(random.uniform(0.5, 1.0))
                    except:
                        break

                try:
                    # 仅解析有昵称文本的<a href="/u/...">，其父级td包含“粉丝X人/万人”
                    fan_elements = self.driver_cn.find_elements(By.XPATH, "//a[contains(@href, '/u/')]")

                    page_fans = []
                    processed_ids = set()

                    for element in fan_elements:
                        try:
                            fan_href = element.get_attribute('href')
                            fan_name = element.text.strip()
                            if not fan_href or '/u/' not in fan_href or not fan_name:
                                # 跳过头像链接等无文本的<a>
                                continue

                            fan_id = fan_href.split('/u/')[-1].split('?')[0].split('/')[0]
                            if not (fan_id.isdigit() and fan_id not in processed_ids):
                                continue

                            # 取该链接所在的右侧td文本，解析“粉丝X人/万人”
                            try:
                                td_text = element.find_element(By.XPATH, "./ancestor::td[1]").text
                            except:
                                td_text = ""

                            fans_count_cn = parse_cn_fans_count(td_text)

                            page_fans.append({
                                'id': fan_id,
                                'screen_name': fan_name,
                                'fans_count_cn': fans_count_cn  # 可能为None（格式不符时）
                            })
                            processed_ids.add(fan_id)

                        except:
                            continue

                    if len(page_fans) == 0:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= 2:
                            break
                    else:
                        consecutive_empty_pages = 0
                        fans_data.extend(page_fans)

                except:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        break

                time.sleep(random.uniform(0.5, 1.0))

            return fans_data
        except:
            return []
    
    def calculate_user_popularity(self, user_id, max_posts=MAX_POSTS_FOR_POPULARITY):
        """计算用户近10条微博的平均转赞评（保留；你当前主用fetch3_helper的总体影响力）"""
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            self.driver_cn.get(profile_url)
            time.sleep(2)
            
            weibo_divs = self.driver_cn.find_elements(By.XPATH, "//div[@class='c' and contains(@id, 'M_')]")
            if not weibo_divs:
                return 0.0
            
            posts_data = []
            for i, weibo_div in enumerate(weibo_divs):
                if len(posts_data) >= max_posts:
                    break
                post_data = self.process_single_weibo_div(weibo_div)
                if post_data and post_data['content'] != "内容未提取":
                    posts_data.append(post_data)
            
            if not posts_data:
                return 0.0
            
            total_interactions = 0
            valid_posts = len(posts_data)
            for post in posts_data:
                interactions = post['interactions']
                post_total = interactions['likes'] + interactions['reposts'] + interactions['comments']
                total_interactions += post_total
            
            avg_popularity = total_interactions / valid_posts if valid_posts > 0 else 0.0
            return avg_popularity
        except:
            return 0.0
    
    def process_single_weibo_div(self, weibo_div_element):
        """处理单个微博div"""
        try:
            div_html = weibo_div_element.get_attribute('outerHTML')
            child_divs = weibo_div_element.find_elements(By.XPATH, "./div")
            
            content = "内容未提取"
            try:
                ctt_element = weibo_div_element.find_element(By.CLASS_NAME, "ctt")
                content = ctt_element.text.strip()
                content = content[:100] + ('...' if len(content) > 100 else '')
            except:
                pass
            
            interactions = {'reposts': 0, 'likes': 0, 'comments': 0}
            if len(child_divs) > 0:
                last_div = child_divs[-1]
                last_div_html = last_div.get_attribute('outerHTML')
                interactions = self.extract_interactions_from_html(last_div_html)
            else:
                interactions = self.extract_interactions_from_html(div_html)
            
            return {
                'content': content,
                'interactions': interactions,
                'total_interactions': sum(interactions.values())
            }
        except:
            return None
    
    def extract_interactions_from_html(self, html_text):
        """提取转赞评数据"""
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
                        num = int(matches[-1])
                        interactions[interaction_type] = num
                    except:
                        continue
        return interactions
    
    def cleanup(self):
        """清理资源"""
        try:
            if self.driver_com:
                self.driver_com.quit()
                self.driver_com = None
        except Exception as e:
            print(f"关闭weibo.com浏览器时出错: {e}")
        try:
            if self.driver_cn:
                self.driver_cn.quit()
                self.driver_cn = None
        except Exception as e:
            print(f"关闭weibo.cn浏览器时出错: {e}")

def get_current_network_size():
    return len(node_categories["A"]) + len(node_categories["B"])

def get_topic_entry(topic):
    """获取或创建topic_plan中的条目"""
    global topic_plan
    for entry in topic_plan:
        if entry.get("topic") == topic:
            return entry
    # 未找到则创建一个默认条目（仅在需要时）
    entry = {
        "topic": topic,
        "target": 0,
        "finished_first_phase": False,
        "count_A": 0,
        "count_B": 0,
        "count_total": 0
    }
    topic_plan.append(entry)
    return entry

def ensure_topic_bucket(topic):
    """确保每个关键词有自己的计数集合（用于判断，现持久化）"""
    if topic not in topic_nodes_map:
        topic_nodes_map[topic] = set()

def update_topic_counts(topic, finished_flag=None):
    """更新某个关键词的A/B/总计数，并可选更新完成标记"""
    ensure_topic_bucket(topic)
    bucket = topic_nodes_map.get(topic, set())
    entry = get_topic_entry(topic)
    count_total = len(bucket)
    # 计算A/B计数（按当前全局分类交集）
    count_a = sum(1 for uid in bucket if uid in node_categories["A"])
    count_b = sum(1 for uid in bucket if uid in node_categories["B"])
    entry["count_A"] = count_a
    entry["count_B"] = count_b
    entry["count_total"] = count_total
    if finished_flag is not None:
        entry["finished_first_phase"] = bool(finished_flag)

def save_progress(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    progress_file = os.path.join(output_dir, 'progress.json')
    # 保存前先刷新每个topic的计数
    for entry in topic_plan:
        update_topic_counts(entry["topic"])
    progress_data = {
        "users": users_data,
        "edges": edges_data,
        "processed": list(processed_users),
        "categories": {k: list(v) for k, v in node_categories.items()},
        "popularity": popularity_data,
        "save_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_users": len(users_data),
        "total_edges": len(edges_data),
        "topics_processed": topics_processed,
        # 新增：关键词计划与进度
        "topic_plan": topic_plan,
        # 新增：每个关键词的已计入用户集合（用于精准续跑）
        "topic_nodes_map": {k: list(v) for k, v in topic_nodes_map.items()}
    }
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    print(f"💾 进度已保存 -> 文件: {progress_file}")
    print(f"   当前总用户: {len(users_data)}，总边数: {len(edges_data)}，时间: {progress_data['save_timestamp']}")

def load_progress(output_dir):
    global processed_users, users_data, edges_data, edges_set, node_categories, popularity_data, topics_processed, topic_plan, topic_nodes_map
    progress_file = os.path.join(output_dir, 'progress.json')
    if not os.path.exists(progress_file):
        return False
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        processed_users = set(data.get("processed", []))
        users_data = data.get("users", {})
        edges_data = data.get("edges", [])
        popularity_data = data.get("popularity", {})
        edges_set = set(tuple(edge) if isinstance(edge, list) else edge for edge in edges_data)
        if "categories" in data:
            for k, v in data["categories"].items():
                if k in node_categories:
                    node_categories[k] = set(v)
        topics_processed = data.get("topics_processed", [])
        # 新增：加载关键词计划与计数
        topic_plan = data.get("topic_plan", [])
        # 新增：加载每个关键词的bucket集合
        loaded_map = data.get("topic_nodes_map", {})
        topic_nodes_map = {k: set(v) for k, v in loaded_map.items()}
        print(f"📥 已加载进度: 用户 {len(users_data)} 个，边 {len(edges_data)} 条，已处理用户 {len(processed_users)} 个")
        if topic_plan:
            print(f"   关键词计划: {len(topic_plan)} 个（含目标与完成状态）")
        return True
    except Exception as e:
        print(f"❌ 加载进度失败: {e}")
        return False

def save_final_data(output_dir, topic_label):
    os.makedirs(output_dir, exist_ok=True)
    
    users_df = pd.DataFrame.from_dict(users_data, orient='index')
    users_df.index.name = 'user_id'
    users_df.reset_index(inplace=True)
    column_order = ['user_id', 'screen_name', 'fans_count', 'category']
    users_df = users_df.reindex(columns=column_order)
    users_df.to_csv(f'{output_dir}/users.csv', index=False, encoding='utf-8-sig')
    
    edges_df = pd.DataFrame(edges_data, columns=['source', 'target'])
    edges_df.to_csv(f'{output_dir}/edges.csv', index=False, encoding='utf-8-sig')
    
    popularity_df = pd.DataFrame.from_dict(popularity_data, orient='index', columns=['avg_popularity'])
    popularity_df.index.name = 'user_id'
    popularity_df.reset_index(inplace=True)
    popularity_df.to_csv(f'{output_dir}/popularity.csv', index=False, encoding='utf-8-sig')
    
    with open(f'{output_dir}/network_info.json', 'w', encoding='utf-8') as f:
        info = {
            "topic": topic_label,
            "节点数": len(users_df),
            "边数": len(edges_df),
            "A类节点数": len(node_categories["A"]),
            "B类节点数": len(node_categories["B"]),
            "关键词列表": topics_processed,
            "爬取时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        json.dump(info, f, ensure_ascii=False, indent=2)
    print("🧾 最终文件已输出：users.csv, edges.csv, popularity.csv, network_info.json")
    print(f"   总用户: {len(users_df)}，总边数: {len(edges_df)}，A类: {len(node_categories['A'])}，B类: {len(node_categories['B'])}")

def prompt_keyword_targets(max_items=5):
    """交互式输入最多5个关键词和目标人数"""
    items = []
    print("\n请输入最多5个关键词及目标人数（回车跳过结束）：")
    for idx in range(1, max_items + 1):
        topic = input(f"- 关键词{idx}: ").strip()
        if not topic:
            break
        while True:
            t = input(f"  目标人数（整数，例如 5000）: ").strip()
            if not t:
                print("  目标人数不能为空")
                continue
            try:
                target = int(t)
                if target <= 0:
                    print("  请输入正整数")
                    continue
                break
            except:
                print("  请输入有效的整数")
        items.append((topic, target))
    return items

def initialize_topic_plan_from_items(items):
    """根据用户输入初始化 topic_plan 与 topic_nodes_map"""
    global topic_plan, topic_nodes_map, topics_processed
    topic_plan = []
    topics_processed = []
    topic_nodes_map = {}
    for topic, target in items:
        topic_plan.append({
            "topic": topic,
            "target": target,
            "finished_first_phase": False,
            "count_A": 0,
            "count_B": 0,
            "count_total": 0
        })
        ensure_topic_bucket(topic)

def run_first_phase_for_topic(topic, target_size):
    """第一阶段（仅A→B）：严格过滤
       - A粉丝数>1500跳过（weibo.com）
       - 拟入B：用 weibo.cn 解析到的 fans_count_cn 过滤，>1000 或解析不到则跳过，不建边不计数
       - 已在A的粉丝：允许加边（不受B阈值限制）
    """
    global should_exit

    ensure_topic_bucket(topic)
    topic_bucket = topic_nodes_map[topic]

    print("\n" + "="*80)
    print(f"开始第一阶段（A→B）：#{topic}#，目标人数: {target_size}（仅用于该关键词计数）")
    print("="*80)

    try:
        topic_encoded = requests.utils.quote(f"#{topic}#")
        page = 1
        seen_users = set()
        reach_target = False
        a_processed_for_topic = 0
        no_more_users = False  # 标记是否因无新用户而结束

        while not reach_target and not should_exit:
            print(f"  请求第{page}页...")
            if page == 1:
                page_url = f"https://s.weibo.com/weibo/{topic_encoded}"
            else:
                page_url = f"https://s.weibo.com/weibo/{topic_encoded}&page={page}"

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8',
                'Referer': 'https://weibo.com/',
                'Connection': 'keep-alive'
            }
            cookies = {}
            try:
                with open(COOKIE_PATH, 'r', encoding='utf-8') as f:
                    cookies_list = json.load(f)
                for cookie in cookies_list:
                    cookies[cookie['name']] = cookie['value']
            except:
                pass

            try:
                time.sleep(random.uniform(2, 4))
                response = requests.get(page_url, headers=headers, cookies=cookies, timeout=15)
                if response.status_code != 200:
                    print(f"  状态码 {response.status_code}，停止该页")
                    break

                user_ids = crawler.extract_users_from_page(response.text, seen_users)
                if not user_ids:
                    print("  本页未提取到新用户，结束该关键词的第一阶段")
                    no_more_users = True
                    break

                for user_id in user_ids:
                    if should_exit or reach_target:
                        break
                    if user_id in processed_users:
                        continue

                    # 先获取A的粉丝数，超过1500直接跳过
                    fans_count = crawler.check_user_fans_count(user_id)
                    if fans_count is None:
                        fans_count = 0
                    if fans_count > A_FANS_THRESHOLD_SKIP:
                        print(f"  跳过A类用户 {user_id}（粉丝 {fans_count} > {A_FANS_THRESHOLD_SKIP}）")
                        continue

                    # 记录A（全局）
                    node_categories["A"].add(user_id)
                    users_data[user_id] = {
                        'screen_name': f'用户{user_id}',
                        'fans_count': fans_count,
                        'category': 'A'
                    }
                    processed_users.add(user_id)
                    a_processed_for_topic += 1
                    print(f"  [A {a_processed_for_topic}] 处理A类用户 {user_id} | A粉丝数 {fans_count}（该关键词累计 {len(topic_bucket)} / {target_size} | 全局 {get_current_network_size()}）")

                    # 记录近10条影响力（保留）
                    if user_id not in popularity_data:
                        popularity_data[user_id] = crawler.calculate_user_popularity(user_id)

                    # 将A计入该关键词桶
                    topic_bucket.add(user_id)

                    # 爬A的粉丝（weibo.cn），并用 fans_count_cn 做B阈值过滤
                    fans_users = crawler.crawl_user_fans_cn(user_id)
                    new_b = 0
                    new_edges = 0
                    skipped_high = 0
                    skipped_format = 0

                    for fan in fans_users:
                        fan_id = str(fan.get('id'))
                        fan_screen_name = fan.get('screen_name', '')
                        cn_count = fan.get('fans_count_cn', None)

                        # 若粉丝已是A：允许添加边（不适用B阈值），并计入桶
                        if fan_id in node_categories["A"]:
                            edge = (user_id, fan_id)
                            if edge not in edges_set:
                                edges_data.append(edge)
                                edges_set.add(edge)
                                new_edges += 1
                            topic_bucket.add(fan_id)
                            continue

                        # 已在B：直接补边，不重复阈值判断
                        if fan_id in node_categories["B"]:
                            edge = (user_id, fan_id)
                            if edge not in edges_set:
                                edges_data.append(edge)
                                edges_set.add(edge)
                                new_edges += 1
                            topic_bucket.add(fan_id)
                            continue

                        # 拟入B：必须有可解析的粉丝数，且 <=1000；否则跳过
                        if cn_count is None:
                            skipped_format += 1
                            continue
                        if cn_count > B_FANS_THRESHOLD_SKIP:
                            skipped_high += 1
                            print(f"    -> 跳过粉丝 {fan_id}（粉丝 {cn_count} > {B_FANS_THRESHOLD_SKIP}）")
                            continue

                        # 到这里：加入网络，写入边与B类
                        edge = (user_id, fan_id)
                        if edge not in edges_set:
                            edges_data.append(edge)
                            edges_set.add(edge)
                            new_edges += 1

                        node_categories["B"].add(fan_id)
                        if fan_id not in users_data:
                            users_data[fan_id] = {
                                'screen_name': fan_screen_name,
                                'fans_count': int(cn_count),
                                'category': 'B'
                            }
                        else:
                            users_data[fan_id]['fans_count'] = int(cn_count)

                        topic_bucket.add(fan_id)
                        new_b += 1

                    print(f"    -> 本A新增B类 {new_b} 个，跳过超标 {skipped_high} 个，跳过格式不明 {skipped_format} 个，新增边 {new_edges} 条 | 当前全局：用户 {len(users_data)}，边 {len(edges_data)}")

                    # 更新并保存该关键词计数（不每次都落盘，只更新内存）
                    update_topic_counts(topic)

                    # 达标判定
                    if len(topic_bucket) >= target_size:
                        reach_target = True
                        update_topic_counts(topic, finished_flag=True)
                        print(f"  ✅ 关键词 #{topic}# 已达目标人数 {target_size}（该关键词累计），结束该关键词的第一阶段")
                        break

                    if len(processed_users) % 10 == 0:
                        save_progress(output_dir)

                    time.sleep(random.uniform(1.0, 2.0))

                seen_users.update(user_ids)
                page += 1

            except Exception as e:
                print(f"  第一阶段请求异常: {e}")
                break

        # 若因无新用户结束，也标记完成（避免重复扫描）
        if not reach_target and no_more_users:
            update_topic_counts(topic, finished_flag=True)
        else:
            update_topic_counts(topic)

        entry = get_topic_entry(topic)
        print(f"关键词 #{topic}# 第一阶段完成状态: {'已完成' if entry.get('finished_first_phase') else '未完成'} | "
              f"该关键词累计: {entry.get('count_total', 0)} | 全局规模: {get_current_network_size()}（A: {len(node_categories['A'])}, B: {len(node_categories['B'])}）")

    except KeyboardInterrupt:
        should_exit = True
        save_progress(output_dir)
        print("✅ 已保存进度（用户中断）")
    except Exception as e:
        print(f"程序异常: {e}")
        save_progress(output_dir)

def run_second_phase_global():
    """全局第二阶段：完善B类用户的边（只在A/B之间加边）"""
    global should_exit

    print("\n=== 全局第二阶段：完善B类用户的边（无需再判断阈值） ===")
    b_users_to_process = [u for u in node_categories["B"] if u not in processed_users]
    total_b = len(b_users_to_process)
    print(f"需要处理的B类用户: {total_b} 个")

    for i, user_id in enumerate(b_users_to_process, start=1):
        if should_exit:
            break

        print(f"[B {i}/{total_b}] 正在处理B类用户 {user_id} ...")

        # 标记处理并可选刷新粉丝数（不用于阈值判断）
        b_fans_cnt = crawler.check_user_fans_count(user_id)
        if user_id not in users_data:
            users_data[user_id] = {
                'screen_name': f'用户{user_id}',
                'fans_count': b_fans_cnt,
                'category': 'B'
            }
        else:
            users_data[user_id]['fans_count'] = b_fans_cnt

        processed_users.add(user_id)
        if user_id not in popularity_data:
            popularity_data[user_id] = crawler.calculate_user_popularity(user_id)

        fans_users = crawler.crawl_user_fans_cn(user_id)
        valid_edge_count = 0
        for fan in fans_users:
            fan_id = str(fan.get('id'))
            if fan_id in node_categories["A"] or fan_id in node_categories["B"]:
                edge = (user_id, fan_id)
                if edge not in edges_set:
                    edges_data.append(edge)
                    edges_set.add(edge)
                    valid_edge_count += 1

        print(f"  -> 本B新增有效边 {valid_edge_count} 条 | 当前全局：用户 {len(users_data)}，边 {len(edges_data)}")

        if (i % 10) == 0:
            save_progress(output_dir)
        time.sleep(random.uniform(0.5, 1.0))

def main():
    global crawler, should_exit, output_dir, topic_plan, topic_nodes_map, topics_processed

    signal.signal(signal.SIGINT, signal_handler)

    print("微博话题网络爬取器（合并多关键词为一个大网络）")
    print("- 支持最多5个关键词，每个关键词设定独立目标人数（仅用于该关键词计数）")
    print(f"- A类跳过阈值: 粉丝数 > {A_FANS_THRESHOLD_SKIP}")
    print(f"- B类跳过阈值: 粉丝数 > {B_FANS_THRESHOLD_SKIP}（仅第一阶段过滤拟入B；第二阶段不再判断）")
    print("按Ctrl+C可随时安全中断")

    crawler = TopicNetworkCrawler()
    if not crawler.setup_drivers():
        return
    if not crawler.load_cookies_cn():
        crawler.cleanup()
        return

    try:
        # 合并网络输出目录（统一保存/续跑）
        output_dir = f'{BASE_OUTPUT_DIR}/topic_combined'
        os.makedirs(output_dir, exist_ok=True)
        has_prev = load_progress(output_dir)

        # 若存在旧进度，询问是否继续
        items = []
        if has_prev and topic_plan:
            print("\n检测到已有的关键词进度：")
            for idx, entry in enumerate(topic_plan, 1):
                print(f"  {idx}. #{entry.get('topic')}# | target={entry.get('target')} | "
                      f"完成: {entry.get('finished_first_phase')} | 计数: A={entry.get('count_A')}, B={entry.get('count_B')}, 总={entry.get('count_total')}")
            choice = input("\n是否在上述进度上继续？(y/n): ").strip().lower()
            if choice == 'y':
                # 使用已保存的topic_plan（不再询问）
                items = [(e["topic"], e["target"]) for e in topic_plan]
            else:
                # 不继续，则重新输入关键词与目标，并重置与覆盖topic相关的持久数据
                items = prompt_keyword_targets(max_items=5)
                if not items:
                    print("未输入任何关键词，程序退出。")
                    return
                # 初始化新的topic_plan与bucket集合（保留已有网络数据不清空，以防用户希望追加）
                initialize_topic_plan_from_items(items)
                save_progress(output_dir)
        else:
            # 无进度则正常输入
            items = prompt_keyword_targets(max_items=5)
            if not items:
                print("未输入任何关键词，程序退出。")
                return
            initialize_topic_plan_from_items(items)
            save_progress(output_dir)

        start_time = datetime.now()

        # 第一阶段：仅处理未完成的关键词
        unfinished = [e for e in topic_plan if not e.get("finished_first_phase", False)]
        if unfinished:
            print(f"\n共有 {len(unfinished)} 个关键词未完成第一阶段，将继续处理：")
            for e in unfinished:
                print(f"  - #{e['topic']}# (target={e['target']}, 当前总={e.get('count_total',0)})")
            for entry in unfinished:
                if should_exit:
                    break
                topic = entry["topic"]
                target = entry["target"]
                print(f"\n[续跑] 处理关键词: {topic}，目标人数: {target}")
                run_first_phase_for_topic(topic, target)
                if topic not in topics_processed:
                    topics_processed.append(topic)
                save_progress(output_dir)
        else:
            print("\n所有关键词的第一阶段均已完成，将直接进入第二阶段。")

        if should_exit:
            print("\n⚠️ 中断于第一阶段，已保存合并进度。")
        else:
            # 全局第二阶段（统一补边）
            run_second_phase_global()
            save_final_data(output_dir, topic_label="combined_" + "_".join([e["topic"] for e in topic_plan]))
            print("\n=== 全部关键词处理完成，已统一补边并保存合并网络 ===")
            print(f"最终统计: A={len(node_categories['A'])}, B={len(node_categories['B'])}, 总用户={get_current_network_size()}, 总边={len(edges_data)}")
            print(f"合并网络目录: {output_dir}")

        end_time = datetime.now()
        print(f"\n总耗时: {end_time - start_time}")

    except KeyboardInterrupt:
        should_exit = True
        if output_dir:
            save_progress(output_dir)
        print("✅ 已保存当前进度（用户中断）")
    finally:
        if crawler:
            crawler.cleanup()

if __name__ == "__main__":
    main()