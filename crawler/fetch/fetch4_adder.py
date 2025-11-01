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

# 基本配置
BASE_OUTPUT_DIR = 'C:/Tengfei/data/data/topic_networks'
COOKIE_PATH = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'

# 翻页限制（weibo.cn）
MAX_PAGES_PER_USER = 20

# 速度参数（与fetch4一致）
SLEEP_MIN = 0.4
SLEEP_MAX = 0.6
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.0

# 近十条影响力
MAX_POSTS_FOR_POPULARITY = 10

# 运行态
should_exit = False

def signal_handler(signum, frame):
    global should_exit
    print("\n⚠️ 捕获到中断信号，准备安全退出...")
    should_exit = True

class TagAdderCrawler:
    def __init__(self):
        self.driver_com = None  # 用于 weibo.com 获取粉丝数（阈值过滤）
        self.driver_cn = None   # 用于 weibo.cn 粉丝/关注/影响力
        self.existing_nodes = set()
        self.edges_data = []    # 现有网络的边（用于追加后写回）
        self.edges_set = set()
        self.popularity_map = {}  # 现有 popularity 映射 user_id -> avg_popularity
        self.users_df = None      # 现有 users.csv（若存在）
        self.network_dir = None

    def setup_drivers(self):
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        try:
            # weibo.com（仅用于粉丝阈值）
            self.driver_com = webdriver.Chrome(options=chrome_options)
            self.driver_com.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # weibo.cn（粉丝/关注/影响力）
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
            print(f"❌ 浏览器初始化失败: {e}")
            return False

    def load_cookies_cn(self):
        try:
            with open(COOKIE_PATH, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            self.driver_cn.get('https://weibo.cn')
            time.sleep(1.5)
            for cookie in cookies:
                try:
                    self.driver_cn.add_cookie(cookie)
                except:
                    pass
            self.driver_cn.refresh()
            time.sleep(1.0)
            return True
        except Exception as e:
            print(f"❌ Cookie加载失败: {e}")
            return False

    def extract_users_from_search(self, html_content, seen):
        """从s.weibo.com的HTML中提取用户ID"""
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
            for m in re.findall(pattern, html_content):
                if m.isdigit() and len(m) >= 6 and m not in seen:
                    user_ids.append(m)
        # 去重
        out, page_seen = [], set()
        for uid in user_ids:
            if uid not in page_seen:
                out.append(uid)
                page_seen.add(uid)
        return out

    def check_user_fans_count(self, user_id):
        """weibo.com档案页解析粉丝数（用于阈值过滤）"""
        try:
            self.driver_com.get(f'https://weibo.com/u/{user_id}')
            time.sleep(random.uniform(1.0, 1.5))
            html = self.driver_com.page_source
            patterns = [
                r'<span[^>]*>([0-9]+\.?[0-9]*[万]?)</span>\s*粉丝',
                r'>([0-9]+\.?[0-9]*[万]?)</span>\s*粉丝',
                r'([0-9]+\.?[0-9]*[万]?)\s*粉丝',
                r'粉丝[^>]*>([0-9]+\.?[0-9]*[万]?)',
            ]
            for p in patterns:
                ms = re.findall(p, html)
                if ms:
                    s = ms[0].strip()
                    if '万' in s:
                        return int(float(s.replace('万', '')) * 10000)
                    return int(float(s))
        except:
            pass
        return 0

    def calculate_user_popularity(self, user_id, max_posts=MAX_POSTS_FOR_POPULARITY):
        """weibo.cn上计算近10条平均转赞评"""
        try:
            self.driver_cn.get(f'https://weibo.cn/u/{user_id}')
            time.sleep(1.0)
            weibo_divs = self.driver_cn.find_elements(By.XPATH, "//div[@class='c' and contains(@id, 'M_')]")
            if not weibo_divs:
                return 0.0
            def extract_interactions(html_text):
                res = {'reposts': 0, 'likes': 0, 'comments': 0}
                pats = {'likes': [r'赞\[(\d+)\]'], 'reposts': [r'转发\[(\d+)\]'], 'comments': [r'评论\[(\d+)\]']}
                for k, ps in pats.items():
                    for p in ps:
                        ms = re.findall(p, html_text)
                        if ms:
                            try:
                                res[k] = int(ms[-1])
                            except:
                                pass
                return res
            posts = []
            for div in weibo_divs:
                if len(posts) >= max_posts:
                    break
                try:
                    last_div = div.find_elements(By.XPATH, "./div")[-1] if div.find_elements(By.XPATH, "./div") else div
                    interactions = extract_interactions(last_div.get_attribute('outerHTML'))
                    posts.append(interactions)
                except:
                    continue
            if not posts:
                return 0.0
            tot = sum(p['likes'] + p['reposts'] + p['comments'] for p in posts)
            return tot / len(posts)
        except:
            return 0.0

    def crawl_cn_ids(self, url_template):
        """通用 weibo.cn 翻页抓取 /fans 或 /follow 的用户ID集合"""
        try:
            ids = []
            consecutive_empty = 0
            for page in range(1, MAX_PAGES_PER_USER + 1):
                url = f"{url_template}?page={page}" if page > 1 else url_template
                self.driver_cn.get(url)
                time.sleep(random.uniform(0.5, 1.0))
                elems = self.driver_cn.find_elements(By.XPATH, "//a[contains(@href, '/u/')]")
                page_ids, seen = [], set()
                for a in elems:
                    try:
                        href = a.get_attribute('href')
                        name = a.text.strip()
                        if not href or '/u/' not in href or not name:
                            continue
                        uid = href.split('/u/')[-1].split('?')[0].split('/')[0]
                        if uid.isdigit() and uid not in seen:
                            page_ids.append(uid); seen.add(uid)
                    except:
                        continue
                if not page_ids:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                else:
                    consecutive_empty = 0
                    ids.extend(page_ids)
                if should_exit:
                    break
            return set(ids)
        except:
            return set()

    def crawl_user_fans_ids_cn(self, user_id):
        return self.crawl_cn_ids(f'https://weibo.cn/{user_id}/fans')

    def crawl_user_following_ids_cn(self, user_id):
        return self.crawl_cn_ids(f'https://weibo.cn/{user_id}/follow')

    def cleanup(self):
        try:
            if self.driver_com:
                self.driver_com.quit()
        except Exception as e:
            print(f"关闭weibo.com浏览器出错: {e}")
        try:
            if self.driver_cn:
                self.driver_cn.quit()
        except Exception as e:
            print(f"关闭weibo.cn浏览器出错: {e}")

def prompt_keywords_targets(max_items=20):
    items = []
    print("\n请输入最多20个关键词及目标人数（回车结束）：")
    for i in range(1, max_items + 1):
        topic = input(f"- 关键词{i}: ").strip()
        if not topic:
            break
        while True:
            t = input("  目标人数（整数，例如 5000）: ").strip()
            try:
                v = int(t)
                if v <= 0:
                    print("  请输入正整数"); continue
                items.append((topic, v))
                break
            except:
                print("  请输入有效整数")
    return items

def prompt_threshold():
    while True:
        t = input("请输入粉丝阈值（整数；0表示不设）: ").strip()
        try:
            v = int(t)
            if v < 0:
                print("请输入≥0的整数"); continue
            return v
        except:
            print("请输入有效整数")

def prompt_existing_network_dir():
    while True:
        d = input("请输入现有网络目录（例如 C:/Tengfei/data/data/topic_networks/topic_combined）: ").strip()
        if os.path.isdir(d):
            return d
        print("目录不存在，请重新输入。")

def collect_topic_users(crawler: TagAdderCrawler, topic: str, target: int, threshold: int):
    """从 s.weibo.com 按tag收集用户ID，应用粉丝阈值（0不设）"""
    topic_encoded = requests.utils.quote(f"#{topic}#")
    page = 1
    seen = set()
    picked = []
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
            cl = json.load(f)
        for c in cl:
            cookies[c['name']] = c['value']
    except:
        pass

    consecutive_empty = 0
    max_consecutive_empty = 3

    while len(picked) < target and not should_exit:
        page_url = f"https://s.weibo.com/weibo/{topic_encoded}" if page == 1 else f"https://s.weibo.com/weibo/{topic_encoded}&page={page}"
        print(f"  - 请求第{page}页: {page_url}")
        try:
            time.sleep(random.uniform(1.0, 1.5))
            resp = requests.get(page_url, headers=headers, cookies=cookies, timeout=15)
            if resp.status_code != 200:
                print(f"    状态码 {resp.status_code}，重试下一页")
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    break
                page += 1
                continue

            uids = crawler.extract_users_from_search(resp.text, seen)
            if not uids:
                consecutive_empty += 1
                print(f"    空页（连续{consecutive_empty}）")
                if consecutive_empty >= max_consecutive_empty:
                    break
                page += 1
                continue
            consecutive_empty = 0

            for uid in uids:
                if uid in seen:
                    continue
                seen.add(uid)
                # 阈值过滤（0表示不设）
                fans = crawler.check_user_fans_count(uid) if threshold > 0 else 0
                if threshold > 0 and fans > threshold:
                    print(f"    跳过用户 {uid}（粉丝 {fans} > 阈值 {threshold}）")
                    continue
                picked.append(uid)
                print(f"    [+] 收录用户 {uid}（粉丝 {fans if threshold>0 else 'N/A'}） | {len(picked)}/{target}")
                time.sleep(random.uniform(0.3, 0.6))
                if len(picked) >= target or should_exit:
                    break

            page += 1
            if page > 200:
                print("    达到最大页数200，停止翻页")
                break
        except Exception as e:
            print(f"    请求异常: {e}")
            break

    return picked

def load_existing_network(crawler: TagAdderCrawler, network_dir: str):
    crawler.network_dir = network_dir
    edges_path = os.path.join(network_dir, 'edges.csv')
    users_path = os.path.join(network_dir, 'users.csv')
    popularity_path = os.path.join(network_dir, 'popularity.csv')

    if not os.path.exists(edges_path):
        raise FileNotFoundError(f"未找到edges.csv: {edges_path}")

    edges_df = pd.read_csv(edges_path)
    crawler.edges_data = []
    crawler.edges_set = set()
    for _, row in edges_df.iterrows():
        s = str(row['source']); t = str(row['target'])
        crawler.edges_data.append((s, t))
        crawler.edges_set.add((s, t))

    existing_nodes = set(edges_df['source'].astype(str)).union(set(edges_df['target'].astype(str)))
    if os.path.exists(users_path):
        users_df = pd.read_csv(users_path)
        users_df['user_id'] = users_df['user_id'].astype(str)
        crawler.users_df = users_df
        existing_nodes |= set(users_df['user_id'].astype(str))
    crawler.existing_nodes = existing_nodes

    crawler.popularity_map = {}
    if os.path.exists(popularity_path):
        pop_df = pd.read_csv(popularity_path)
        if 'user_id' in pop_df.columns and 'avg_popularity' in pop_df.columns:
            for _, r in pop_df.iterrows():
                crawler.popularity_map[str(r['user_id'])] = float(r['avg_popularity'])

    print(f"📥 已加载现有网络: 节点≈{len(existing_nodes)}，边 {len(crawler.edges_data)}")

def save_new_tag_users_map(mapping: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, 'new_tag_users.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"📝 已保存新标签用户映射: {out_path}")

def write_back_network(crawler: TagAdderCrawler):
    """将追加后的边与人气写回现有网络目录"""
    edges_df = pd.DataFrame(crawler.edges_data, columns=['source', 'target'])
    edges_path = os.path.join(crawler.network_dir, 'edges.csv')
    edges_df.to_csv(edges_path, index=False, encoding='utf-8-sig')

    # popularity.csv 追加/覆盖（以map为准）
    pop_items = sorted(crawler.popularity_map.items(), key=lambda x: x[0])
    pop_df = pd.DataFrame(pop_items, columns=['user_id', 'avg_popularity'])
    pop_path = os.path.join(crawler.network_dir, 'popularity.csv')
    pop_df.to_csv(pop_path, index=False, encoding='utf-8-sig')

    print(f"💾 已写回 edges.csv 与 popularity.csv 到: {crawler.network_dir}")
    print(f"   边总数: {len(crawler.edges_data)}，影响力用户数: {len(crawler.popularity_map)}")

def integrate_new_users_to_network(crawler: TagAdderCrawler, new_user_ids: list):
    """为新人在现网中补充“粉丝边：被关注者→粉丝（博主→粉丝）”，并按需写入影响力"""
    added_nodes = 0
    edges_added = 0
    processed = 0

    for uid in new_user_ids:
        if should_exit:
            break
        processed += 1
        print(f"\n[新人 {processed}/{len(new_user_ids)}] 处理 {uid} ...")

        # 抓取粉丝/关注集合（weibo.cn）
        fans_ids = crawler.crawl_user_fans_ids_cn(uid)          # 粉丝集合：现网粉丝 u 命中 => (uid, u)
        follow_ids = crawler.crawl_user_following_ids_cn(uid)   # 关注集合：新人关注现网 v => (v, uid)

        # 与现网节点求交
        fans_in_existing = fans_ids & crawler.existing_nodes
        follow_in_existing = follow_ids & crawler.existing_nodes

        # 粉丝边方向：被关注者→粉丝（博主→粉丝）
        new_edges = 0

        # 新人关注了现网中的人 v：v（被关注者/博主）→ uid（粉丝）
        for v in follow_in_existing:
            e = (str(v), str(uid))
            if e not in crawler.edges_set:
                crawler.edges_data.append(e)
                crawler.edges_set.add(e)
                new_edges += 1

        # 现网用户 u 是新人的粉丝：uid（被关注者/博主）→ u（粉丝）
        for u in fans_in_existing:
            e = (str(uid), str(u))
            if e not in crawler.edges_set:
                crawler.edges_data.append(e)
                crawler.edges_set.add(e)
                new_edges += 1

        if new_edges > 0:
            # 仅当有连边时，记录其近十条平均转赞评
            if str(uid) not in crawler.popularity_map:
                popularity = crawler.calculate_user_popularity(uid)
                crawler.popularity_map[str(uid)] = float(popularity)
            edges_added += new_edges
            added_nodes += 1
            print(f"  -> 新增边 {new_edges} 条（关注命中 {len(follow_in_existing)}，粉丝命中 {len(fans_in_existing)}）")
        else:
            print("  -> 未命中任何现网连接，跳过加入网络（不写入影响力）")

        # 轻量节流
        time.sleep(random.uniform(0.5, 1.0))

    print(f"\n✅ 新人整合完成：加入网络的新人 {added_nodes} 人，新增边 {edges_added} 条")

def main():
    signal.signal(signal.SIGINT, signal_handler)

    print("fetch4_adder：按tag批量找人并扩充现有网络（粉丝边：被关注者→粉丝，博主→粉丝）")
    crawler = TagAdderCrawler()
    if not crawler.setup_drivers():
        return
    if not crawler.load_cookies_cn():
        crawler.cleanup()
        return

    try:
        # 1) 关键词与人数
        items = prompt_keywords_targets(max_items=20)
        if not items:
            print("未输入关键词，程序结束。")
            return

        # 2) 粉丝阈值
        threshold = prompt_threshold()
        print(f"粉丝阈值: {threshold}（0表示不设）")

        # 3) 逐关键词收集符合阈值的tag用户
        topic_users_map = {}
        for topic, target in items:
            if should_exit:
                break
            print(f"\n=== 关键词 #{topic}# | 目标人数: {target} ===")
            users = collect_topic_users(crawler, topic, target, threshold)
            topic_users_map[topic] = users
            print(f"关键词 #{topic}# 收集完成：{len(users)}/{target}")

        # 4) 输出一个映射表（单文件）
        adder_dir = os.path.join(BASE_OUTPUT_DIR, 'topic_adder')
        save_new_tag_users_map(topic_users_map, adder_dir)

        if should_exit:
            print("⚠️ 用户中断，已保存标签用户映射，未进行网络整合。")
            return

        # 5) 载入现有网络，然后整合新人（weibo.cn 粉丝/关注，两向命中则加边）
        network_dir = prompt_existing_network_dir()
        load_existing_network(crawler, network_dir)

        # 将所有关键词得到的新人合并去重
        all_new_users = set()
        for arr in topic_users_map.values():
            all_new_users.update([str(u) for u in arr])

        # 整合新人并写回网络
        integrate_new_users_to_network(crawler, list(all_new_users))
        write_back_network(crawler)

        print("\n🎉 全部完成。")
    except KeyboardInterrupt:
        print("\n⚠️ 中断，已尽力保存当前状态。")
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    main()