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

# 配置参数（与fetch4保持一致）
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/topic_networks'
COOKIE_PATH = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'

# 翻页与粉丝页设置
MAX_PAGES_PER_USER = 20

# 速度参数（与fetch4一致）
SLEEP_MIN = 0.4
SLEEP_MAX = 0.6
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.0

# 流行度计算参数
MAX_POSTS_FOR_POPULARITY = 10

# 全局数据（仅A类）
node_categories = {"A": set()}
edges_data = []
edges_set = set()
users_data = {}
popularity_data = {}

# 运行态
crawler = None
output_dir = None
should_exit = False

# 进度/计划
topics_processed = []    # 已处理关键词（用于info）
topic_nodes_map = {}     # 每个关键词的用户集合（用于人数判断，持久化）
topic_plan = []          # [{topic, target, finished_first_phase, count_A, count_total}]
processed_users = set()  # 第二阶段已处理过的A类用户（持久化）
run_config = {           # 运行配置（持久化）
    "high_fans_threshold": 0
}

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
        self.driver_com = None  # 用于 weibo.com 获取粉丝数（A阈值过滤）
        self.driver_cn = None   # 用于 weibo.cn 爬粉丝页

    def setup_drivers(self):
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        try:
            # weibo.com 驱动（用于获取A类粉丝数）
            self.driver_com = webdriver.Chrome(options=chrome_options)
            self.driver_com.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # weibo.cn 驱动（用于粉丝页）
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

    def extract_users_from_page(self, html_content, seen_users):
        """从HTML页面提取用户ID（与fetch4一致）"""
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

    def check_user_fans_count(self, user_id):
        """检查用户粉丝数 - 基于weibo.com页面（用于A类阈值过滤）"""
        try:
            profile_url = f'https://weibo.com/u/{user_id}'
            self.driver_com.get(profile_url)
            time.sleep(random.uniform(0.5, 1.0))
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
        """爬取用户粉丝（weibo.cn），仅解析粉丝ID与昵称（用于第二阶段补边）"""
        try:
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
                    fan_elements = self.driver_cn.find_elements(By.XPATH, "//a[contains(@href, '/u/')]")
                    page_fans = []
                    processed_ids = set()

                    for element in fan_elements:
                        try:
                            fan_href = element.get_attribute('href')
                            fan_name = element.text.strip()
                            if not fan_href or '/u/' not in fan_href or not fan_name:
                                continue
                            fan_id = fan_href.split('/u/')[-1].split('?')[0].split('/')[0]
                            if not (fan_id.isdigit() and fan_id not in processed_ids):
                                continue
                            page_fans.append({
                                'id': fan_id,
                                'screen_name': fan_name,
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
        """计算用户近10条微博的平均转赞评（与fetch4一致，可选）"""
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            self.driver_cn.get(profile_url)
            time.sleep(1)
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
        """处理单个微博div（与fetch4一致）"""
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
        """提取转赞评数据（与fetch4一致）"""
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

def ensure_topic_bucket(topic):
    """确保每个关键词有自己的集合"""
    if topic not in topic_nodes_map:
        topic_nodes_map[topic] = set()

def get_topic_entry(topic):
    """获取或创建topic_plan中的条目"""
    global topic_plan
    for entry in topic_plan:
        if entry.get("topic") == topic:
            return entry
    entry = {
        "topic": topic,
        "target": 0,
        "finished_first_phase": False,
        "count_A": 0,
        "count_total": 0
    }
    topic_plan.append(entry)
    return entry

def update_topic_counts(topic, finished_flag=None):
    """更新某个关键词的A/总计数，并可选更新完成标记"""
    ensure_topic_bucket(topic)
    bucket = topic_nodes_map.get(topic, set())
    entry = get_topic_entry(topic)
    count_total = len(bucket)
    count_a = sum(1 for uid in bucket if uid in node_categories["A"])
    entry["count_A"] = count_a
    entry["count_total"] = count_total
    if finished_flag is not None:
        entry["finished_first_phase"] = bool(finished_flag)

def get_current_network_size():
    return len(node_categories["A"])

def save_progress(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    progress_file = os.path.join(output_dir, 'progress_equal.json')
    # 保存前刷新每个topic计数
    for entry in topic_plan:
        update_topic_counts(entry["topic"])
    progress_data = {
        "users": users_data,
        "edges": edges_data,
        "processed_phase2": list(processed_users),  # 第二阶段已处理A
        "categories": {k: list(v) for k, v in node_categories.items()},
        "popularity": popularity_data,
        "save_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_users": len(users_data),
        "total_edges": len(edges_data),
        "topics_processed": topics_processed,
        "topic_plan": topic_plan,
        "topic_nodes_map": {k: list(v) for k, v in topic_nodes_map.items()},
        "run_config": run_config
    }
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    print(f"💾 进度已保存 -> 文件: {progress_file}")
    print(f"   当前总用户: {len(users_data)}，总边数: {len(edges_data)}，时间: {progress_data['save_timestamp']}")

def load_progress(output_dir):
    global processed_users, users_data, edges_data, edges_set, node_categories, popularity_data
    global topics_processed, topic_plan, topic_nodes_map, run_config
    progress_file = os.path.join(output_dir, 'progress_equal.json')
    if not os.path.exists(progress_file):
        return False
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        processed_users = set(data.get("processed_phase2", []))
        users_data = data.get("users", {})
        edges_data = data.get("edges", [])
        popularity_data = data.get("popularity", {})
        edges_set = set(tuple(edge) if isinstance(edge, list) else edge for edge in edges_data)
        if "categories" in data:
            for k, v in data["categories"].items():
                if k in node_categories:
                    node_categories[k] = set(v)
        topics_processed = data.get("topics_processed", [])
        topic_plan = data.get("topic_plan", [])
        loaded_map = data.get("topic_nodes_map", {})
        topic_nodes_map = {k: set(v) for k, v in loaded_map.items()}
        run_config = data.get("run_config", run_config)
        print(f"📥 已加载进度: 用户 {len(users_data)} 个，边 {len(edges_data)} 条，第二阶段已处理 {len(processed_users)} 个A类")
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

    info_path = f'{output_dir}/network_info.json'
    with open(info_path, 'w', encoding='utf-8') as f:
        info = {
            "topic": topic_label,
            "模式": "equal_A_only",
            "A类节点数": len(node_categories["A"]),
            "节点数": len(users_df),
            "边数": len(edges_df),
            "关键词列表": topics_processed,
            "高影响力阈值": run_config.get("high_fans_threshold", 0),
            "爬取时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        json.dump(info, f, ensure_ascii=False, indent=2)
    print("🧾 最终文件已输出：users.csv, edges.csv, popularity.csv, network_info.json")
    print(f"   总用户: {len(users_df)}，总边数: {len(edges_df)}，A类: {len(node_categories['A'])}")

def prompt_keyword_targets(max_items=20):
    """交互式输入最多20个关键词和目标人数"""
    items = []
    print("\n请输入最多20个关键词及目标人数（回车跳过结束）：")
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
            "count_total": 0
        })
        ensure_topic_bucket(topic)

def update_or_create_user(user_id, fans_count, screen_name, category='A'):
    """创建/更新用户字典"""
    if user_id not in users_data:
        users_data[user_id] = {
            'screen_name': screen_name if screen_name else f'用户{user_id}',
            'fans_count': int(fans_count) if fans_count is not None else 0,
            'category': category
        }
    else:
        # 保留已有昵称，如有新的粉丝数则更新
        if fans_count is not None:
            users_data[user_id]['fans_count'] = int(fans_count)
        if 'category' not in users_data[user_id]:
            users_data[user_id]['category'] = category

def run_first_phase_for_topic_equal(topic, target_size, high_threshold):
    """第一阶段：仅收集A类（发过该tag的人），并按阈值过滤高粉丝A"""
    global should_exit

    ensure_topic_bucket(topic)
    topic_bucket = topic_nodes_map[topic]

    print("\n" + "="*80)
    print(f"开始第一阶段（仅A）：#{topic}#，目标人数: {target_size}，高粉丝阈值: {high_threshold}（0表示不设）")
    print("="*80)

    try:
        topic_encoded = requests.utils.quote(f"#{topic}#")
        page = 1
        seen_users = set()
        reach_target = False
        a_added_for_topic = 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 3

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
                time.sleep(random.uniform(0.7, 1.4))
                response = requests.get(page_url, headers=headers, cookies=cookies, timeout=15)
                if response.status_code != 200:
                    print(f"  状态码 {response.status_code}，停止该页")
                    break

                user_ids = crawler.extract_users_from_page(response.text, seen_users)
                if not user_ids:
                    consecutive_empty_pages += 1
                    print(f"  本页未提取到新用户，连续空页: {consecutive_empty_pages}")
                    if consecutive_empty_pages >= max_consecutive_empty:
                        print("  连续空页达到阈值，结束该关键词的第一阶段扫描")
                        break
                else:
                    consecutive_empty_pages = 0

                for user_id in user_ids:
                    if should_exit or reach_target:
                        break

                    # 如已在A，直接计入该关键词桶（不重复查阈值）
                    if user_id in node_categories["A"]:
                        if user_id not in topic_bucket:
                            topic_bucket.add(user_id)
                            a_added_for_topic += 1
                            update_topic_counts(topic)
                            print(f"  [A+] 关键词桶新增已有A {user_id} | 该关键词累计 {len(topic_bucket)}/{target_size} | 全局A {len(node_categories['A'])}")
                        if len(topic_bucket) >= target_size:
                            reach_target = True
                            break
                        continue

                    # 获取A的粉丝数并进行阈值过滤（0=不设阈值）
                    fans_count = crawler.check_user_fans_count(user_id)
                    if high_threshold and fans_count > high_threshold:
                        print(f"  跳过A类用户 {user_id}（粉丝 {fans_count} > 阈值 {high_threshold}）")
                        continue

                    # 记录A（全局）
                    node_categories["A"].add(user_id)
                    update_or_create_user(user_id, fans_count, screen_name=f'用户{user_id}', category='A')
                    topic_bucket.add(user_id)
                    a_added_for_topic += 1

                    # 输出A信息
                    print(f"  [A {a_added_for_topic}] 收录A类用户 {user_id} | 粉丝数 {fans_count} | 关键词累计 {len(topic_bucket)}/{target_size} | 全局A {len(node_categories['A'])}")

                    # 可选：计算近10条影响力（与fetch4一致）
                    if user_id not in popularity_data:
                        popularity_data[user_id] = crawler.calculate_user_popularity(user_id)

                    # 达标判定
                    if len(topic_bucket) >= target_size:
                        reach_target = True
                        break

                    # 定期保存
                    if (a_added_for_topic % 10) == 0:
                        save_progress(output_dir)

                    time.sleep(random.uniform(0.5, 1.5))

                seen_users.update(user_ids)
                page += 1

            except Exception as e:
                print(f"  第一阶段请求异常: {e}")
                break

        # 标记完成状态
        update_topic_counts(topic, finished_flag=True if reach_target else False)
        entry = get_topic_entry(topic)
        print(f"关键词 #{topic}# 第一阶段完成状态: {'已完成' if entry.get('finished_first_phase') else '未完成'} | "
              f"该关键词累计: {entry.get('count_total', 0)} | 全局A: {len(node_categories['A'])}")

        # 保存一次
        save_progress(output_dir)

    except KeyboardInterrupt:
        should_exit = True
        save_progress(output_dir)
        print("✅ 已保存进度（用户中断）")
    except Exception as e:
        print(f"程序异常: {e}")
        save_progress(output_dir)

def run_second_phase_global_equal():
    """第二阶段：仅为A类之间补边（A→A）"""
    global should_exit

    print("\n=== 第二阶段：补全A类之间的边（A→A） ===")
    a_users_to_process = [u for u in node_categories["A"] if u not in processed_users]
    total_a = len(a_users_to_process)
    print(f"需要处理的A类用户: {total_a} 个")

    for i, user_id in enumerate(a_users_to_process, start=1):
        if should_exit:
            break

        print(f"[A {i}/{total_a}] 处理A类用户 {user_id} 的粉丝列表，补A→A边...")
        fans_users = crawler.crawl_user_fans_cn(user_id)
        new_edges = 0

        for fan in fans_users:
            fan_id = str(fan.get('id'))
            if fan_id in node_categories["A"]:
                edge = (user_id, fan_id)
                if edge not in edges_set:
                    edges_data.append(edge)
                    edges_set.add(edge)
                    new_edges += 1

        processed_users.add(user_id)
        print(f"  -> 本A新增A→A边 {new_edges} 条 | 当前全局：边 {len(edges_data)}")

        if (i % 10) == 0:
            save_progress(output_dir)
        time.sleep(random.uniform(0.5, 1.0))

def main():
    global crawler, should_exit, output_dir, topic_plan, topic_nodes_map, topics_processed, run_config

    signal.signal(signal.SIGINT, signal_handler)

    print("微博话题网络爬取器（Equal模式：仅A类，二阶段补A→A边）")
    print("- 支持最多20个关键词，每个关键词设定独立目标人数（仅用于该关键词计数）")
    print("- 第二阶段：只为A类之间补边（A→A）")
    print("按Ctrl+C可随时安全中断")

    crawler = TopicNetworkCrawler()
    if not crawler.setup_drivers():
        return
    if not crawler.load_cookies_cn():
        crawler.cleanup()
        return

    try:
        # Equal模式输出目录
        output_dir = f'{BASE_OUTPUT_DIR}/topic_equal'
        os.makedirs(output_dir, exist_ok=True)
        has_prev = load_progress(output_dir)

        # 输入关键词与目标
        items = []
        if has_prev and topic_plan:
            print("\n检测到已有的关键词进度：")
            for idx, entry in enumerate(topic_plan, 1):
                print(f"  {idx}. #{entry.get('topic')}# | target={entry.get('target')} | "
                      f"完成: {entry.get('finished_first_phase')} | 计数: A={entry.get('count_A')}, 总={entry.get('count_total')}")
            choice = input("\n是否在上述进度上继续？(y/n): ").strip().lower()
            if choice == 'y':
                items = [(e["topic"], e["target"]) for e in topic_plan]
            else:
                items = prompt_keyword_targets(max_items=20)
                if not items:
                    print("未输入任何关键词，程序退出。")
                    return
                initialize_topic_plan_from_items(items)
        else:
            items = prompt_keyword_targets(max_items=20)
            if not items:
                print("未输入任何关键词，程序退出。")
                return
            initialize_topic_plan_from_items(items)

        # 设置高影响力阈值（0表示不设）
        if not has_prev or ("high_fans_threshold" not in run_config):
            while True:
                t = input("请输入高影响力阈值（粉丝数，整数；0表示不设置）: ").strip()
                try:
                    v = int(t)
                    if v < 0:
                        print("请输入≥0的整数")
                        continue
                    run_config["high_fans_threshold"] = v
                    break
                except:
                    print("请输入有效的整数")
        print(f"高影响力阈值: {run_config['high_fans_threshold']}（0表示不设）")

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
                run_first_phase_for_topic_equal(topic, target, run_config["high_fans_threshold"])
                if topic not in topics_processed:
                    topics_processed.append(topic)
                save_progress(output_dir)
        else:
            print("\n所有关键词的第一阶段均已完成，将直接进入第二阶段。")

        if should_exit:
            print("\n⚠️ 中断于第一阶段，已保存进度。")
        else:
            # 第二阶段（统一补A→A边）
            run_second_phase_global_equal()
            # 保存最终
            label = "equal_" + "_".join([e["topic"] for e in topic_plan]) if topic_plan else "equal"
            save_final_data(output_dir, topic_label=label)
            print("\n=== 所有关键词处理完成，已统一补边并保存网络 ===")
            print(f"最终统计: A={len(node_categories['A'])}, 总用户={get_current_network_size()}, 总边={len(edges_data)}")
            print(f"输出目录: {output_dir}")

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