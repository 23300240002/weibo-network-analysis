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

# 🎯 配置：匹配fetch3.py的设置
TARGET_NETWORK_PATH = 'C:/Tengfei/data/data/domain_network3/user_3855570307'
COOKIE_PATH = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'

# 爬取参数（与fetch3.py保持一致）
MAX_PAGES_LIMIT = 20
CONSECUTIVE_EMPTY_THRESHOLD = 2
SLEEP_MIN = 0.4
SLEEP_MAX = 0.8
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.5

# 进度保存参数
SAVE_INTERVAL = 20  # 每处理20个用户保存一次进度

class WeiboMissedUsersFinder:
    def __init__(self, cookie_path=COOKIE_PATH):
        self.driver = None
        self.cookie_path = cookie_path
        
    def setup_driver(self):
        """设置Chrome浏览器（与fetch3.py相同）"""
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
            
            # 设置超时时间
            self.driver.set_page_load_timeout(8)
            self.driver.implicitly_wait(2)
            
            print("✅ 浏览器设置成功")
            return True
        except Exception as e:
            print(f"❌ 浏览器设置失败: {e}")
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
            print("✅ Cookie加载成功")
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
    
    def crawl_user_fans(self, user_id):
        """爬取用户的粉丝列表（与fetch3.py相同的方法）"""
        print(f"  🔍 重新爬取用户 {user_id} 的粉丝...")
        
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
            
            # 受微博限制，最多只能爬20页
            for page in range(1, MAX_PAGES_LIMIT + 1):
                if page > 1:
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/fans?page={page}'
                        self.driver.get(next_page_url)
                        time.sleep(random.uniform(0.5, 1.0))
                    except Exception as e:
                        break
                
                # 查找粉丝链接
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
                
                # 🔥 新增：每隔10页增加额外等待，防止反爬
                if page % 10 == 0 and page > 0:
                    extra_wait = random.uniform(1.0, 2.0)
                    print(f"    已爬取 {page} 页，额外等待 {extra_wait:.1f} 秒防反爬...")
                    time.sleep(extra_wait)
            
            actual_fans_count = len(fans_data)
            print(f"  ✅ 用户 {user_id} 重新爬取到 {actual_fans_count} 个粉丝")
            
            return fans_data
            
        except Exception as e:
            print(f"  ❌ 爬取用户 {user_id} 粉丝时出错: {e}")
            return []
    
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()

def load_network_data(network_path):
    """🔥 修复版：只加载edges数据，从edges中获取真实网络用户"""
    edges_file = os.path.join(network_path, 'edges.csv')
    
    if not os.path.exists(edges_file):
        print(f"❌ 未找到edges文件: {edges_file}")
        return None
    
    print(f"📁 加载网络数据:")
    print(f"  - 边数据: {edges_file}")
    
    edges_df = pd.read_csv(edges_file)
    
    # 转换为字符串类型确保一致性
    edges_df['source'] = edges_df['source'].astype(str)
    edges_df['target'] = edges_df['target'].astype(str)
    
    print(f"  📊 边数据: {len(edges_df)} 条边")
    
    return edges_df

def get_network_users_from_edges(edges_df):
    """🔥 修复版：从edges表中获取真实网络用户（A、B、C类用户）"""
    # 🎯 关键修复：真实网络用户 = edges中所有出现过的用户ID
    source_users = set(edges_df['source'].unique())
    target_users = set(edges_df['target'].unique())
    network_users = source_users.union(target_users)
    
    print(f"🔍 真实网络用户统计（仅基于edges）:")
    print(f"  📊 source用户数: {len(source_users)}")
    print(f"  📊 target用户数: {len(target_users)}")
    print(f"  📊 网络总用户数: {len(network_users)} （这才是真实的A、B、C类用户）")
    
    return network_users

def find_zero_outdegree_users_in_network(edges_df, network_users):
    """🔥 修复版：只在真实网络用户中找出度为0的用户"""
    # 获取所有在edges中作为source出现的用户（即爬过粉丝的用户）
    users_with_crawled_fans = set(edges_df['source'].unique())
    
    # 🎯 关键修复：只在网络用户中查找出度为0的用户
    zero_outdegree_users = network_users - users_with_crawled_fans
    
    print(f"\n🔍 出度分析结果（仅针对真实网络用户）:")
    print(f"  📊 网络总用户数: {len(network_users)}")
    print(f"  📊 在edges中作为source的用户数: {len(users_with_crawled_fans)}")
    print(f"  📊 在edges中从未作为source的用户数: {len(zero_outdegree_users)}")
    print(f"  📊 无出边用户比例: {len(zero_outdegree_users)/len(network_users)*100:.1f}%")
    print(f"  ✅ 这些用户可能因中断、反爬、故障或确实无粉丝而缺少出边")
    
    return list(zero_outdegree_users)

def save_progress(processed_users, new_edges, network_path):
    """保存进度到临时文件"""
    progress_file = os.path.join(network_path, 'refind_progress.json')
    
    progress_data = {
        'processed_users': list(processed_users),
        'new_edges': new_edges,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_processed': len(processed_users),
        'total_new_edges': len(new_edges)
    }
    
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 进度已保存: {len(processed_users)} 个用户完成, {len(new_edges)} 条新边")

def load_progress(network_path):
    """加载进度"""
    progress_file = os.path.join(network_path, 'refind_progress.json')
    
    if not os.path.exists(progress_file):
        return set(), []
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        
        processed_users = set(progress_data.get('processed_users', []))
        new_edges = progress_data.get('new_edges', [])
        timestamp = progress_data.get('timestamp', '未知')
        
        print(f"📁 加载进度文件: {progress_file}")
        print(f"  📊 已处理用户: {len(processed_users)} 个")
        print(f"  📊 已发现新边: {len(new_edges)} 条")
        print(f"  📊 保存时间: {timestamp}")
        
        return processed_users, new_edges
        
    except Exception as e:
        print(f"❌ 加载进度文件失败: {e}")
        return set(), []

def save_final_results(original_edges_df, new_edges, network_path):
    """保存最终结果"""
    if not new_edges:
        print("✅ 没有发现新边，无需更新edges.csv")
        return
    
    # 创建新边的DataFrame
    new_edges_df = pd.DataFrame(new_edges, columns=['source', 'target'])
    
    # 合并原有边和新边
    updated_edges_df = pd.concat([original_edges_df, new_edges_df], ignore_index=True)
    
    # 去重（防止重复边）
    before_dedup = len(updated_edges_df)
    updated_edges_df = updated_edges_df.drop_duplicates()
    after_dedup = len(updated_edges_df)
    
    if before_dedup > after_dedup:
        print(f"⚠️ 去重: {before_dedup} → {after_dedup} (-{before_dedup-after_dedup}条重复边)")
    
    # 保存备份
    backup_dir = os.path.join(network_path, 'backup')
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir, f'edges_backup_{timestamp}.csv')
    original_edges_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
    print(f"✅ 原始edges已备份: {backup_file}")
    
    # 保存更新后的edges
    edges_file = os.path.join(network_path, 'edges.csv')
    updated_edges_df.to_csv(edges_file, index=False, encoding='utf-8-sig')
    print(f"✅ 更新后的edges已保存: {edges_file}")
    print(f"  📊 原始边数: {len(original_edges_df)}")
    print(f"  📊 新增边数: {len(new_edges)}")
    print(f"  📊 最终边数: {len(updated_edges_df)}")
    
    # 保存新边详情
    new_edges_file = os.path.join(network_path, f'new_edges_found_{timestamp}.csv')
    new_edges_df.to_csv(new_edges_file, index=False, encoding='utf-8-sig')
    print(f"✅ 新发现边的详情: {new_edges_file}")
    
    # 删除进度文件
    progress_file = os.path.join(network_path, 'refind_progress.json')
    if os.path.exists(progress_file):
        os.remove(progress_file)
        print(f"✅ 进度文件已清理")

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"遗漏用户查找开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("微博粉丝网络遗漏用户查找器 v2.0 (修复版)")
    print(f"🎯 目标网络: {TARGET_NETWORK_PATH}")
    print(f"🔍 查找对象: 真实网络中出度为0的用户（仅限edges中的A、B、C类用户）")
    print(f"🚀 处理方法: 重新爬取这些用户的粉丝列表，补充遗漏的边")
    print(f"🛡️ 边过滤: 只添加指向网络中已有用户的边，严格保持网络边界")
    print(f"🔄 断点续传: 支持中断后继续")
    print(f"💾 数据安全: 自动备份原始edges.csv")
    print(f"🔥 修复内容: 只分析edges网络中的用户，不包含users.csv中的冗余D类用户")
    print("=" * 80)
    
    # 检查目标网络路径
    if not os.path.exists(TARGET_NETWORK_PATH):
        print(f"❌ 目标网络路径不存在: {TARGET_NETWORK_PATH}")
        print(f"❌ 请确认fetch3.py已经开始并生成了基础数据文件")
        return False
    
    # 🔥 修复：只加载edges数据
    print(f"\n🔍 第一步：加载网络数据")
    edges_df = load_network_data(TARGET_NETWORK_PATH)
    
    if edges_df is None:
        print(f"❌ 无法加载网络数据")
        return False
    
    # 🔥 修复：从edges中获取真实网络用户
    print(f"\n🔍 第二步：识别真实网络用户")
    network_users = get_network_users_from_edges(edges_df)
    
    # 🔥 修复：只在真实网络用户中找出度为0的用户
    print(f"\n🔍 第三步：分析真实网络中的出度为0用户")
    zero_outdegree_users = find_zero_outdegree_users_in_network(edges_df, network_users)
    
    if not zero_outdegree_users:
        print(f"✅ 真实网络中所有用户都已爬过粉丝，无需查找遗漏用户")
        return True
    
    # 加载进度
    print(f"\n🔄 第四步：检查进度状态")
    processed_users, new_edges = load_progress(TARGET_NETWORK_PATH)
    
    # 计算需要处理的用户
    users_to_process = set(zero_outdegree_users) - processed_users
    
    print(f"\n📋 第五步：计算处理计划")
    print(f"  📊 真实网络用户总数: {len(network_users)}")
    print(f"  📊 出度为0的用户总数: {len(zero_outdegree_users)}")
    print(f"  📊 已处理用户数: {len(processed_users)}")
    print(f"  📊 待处理用户数: {len(users_to_process)}")
    print(f"  📊 已发现新边数: {len(new_edges)}")
    print(f"  🛡️ 网络边界保护: 只有指向网络中 {len(network_users)} 个真实用户的边才会被添加")
    
    if len(users_to_process) == 0:
        print(f"✅ 真实网络中所有出度为0的用户已处理完成！")
        # 保存最终结果
        save_final_results(edges_df, new_edges, TARGET_NETWORK_PATH)
        return True
    
    # 确认是否继续
    print(f"\n⚠️ 预计需要重新爬取 {len(users_to_process)} 个用户的粉丝")
    print(f"⚠️ 按平均每用户5秒计算，大约需要 {len(users_to_process) * 5 / 60:.1f} 分钟")
    print(f"✅ 相比之前的错误计算（37万用户），现在只需处理 {len(users_to_process)} 个真实网络用户")
    
    confirm = input("是否继续？(y/n): ").strip().lower()
    if confirm != 'y':
        print("用户取消操作")
        return False
    
    # 初始化爬虫
    print(f"\n🚀 第六步：开始重新爬取")
    finder = WeiboMissedUsersFinder()
    
    if not finder.setup_driver():
        return False
    
    if not finder.load_cookies():
        print("请先确保cookie文件有效")
        finder.cleanup()
        return False
    
    if not finder.test_login_status():
        print("登录状态检查失败，请检查cookie")
        finder.cleanup()
        return False
    
    try:
        processed_count = len(processed_users)
        total_users = len(zero_outdegree_users)
        users_to_process_list = list(users_to_process)
        
        print(f"开始处理剩余的 {len(users_to_process_list)} 个用户...")
        
        batch_start_time = datetime.now()
        consecutive_errors = 0
        new_edges_in_batch = []
        filtered_edges_count = 0  # 统计被过滤的边数
        
        for i, user_id in enumerate(users_to_process_list):
            processed_count += 1
            completion = processed_count / total_users * 100
            
            print(f"\n处理用户 {user_id} [{i+1}/{len(users_to_process_list)}] (总进度: {completion:.1f}%):")
            
            try:
                # 重新爬取用户的粉丝列表
                fans_data = finder.crawl_user_fans(user_id)
                processed_users.add(user_id)
                
                if fans_data:
                    # 🛡️ 边过滤逻辑：只添加指向真实网络用户的边
                    user_new_edges = []
                    user_filtered_edges = 0
                    
                    for fan in fans_data:
                        fan_id = str(fan.get('id'))
                        if fan_id and fan_id != user_id:
                            new_edge = (user_id, fan_id)  # 用户→粉丝（与fetch3.py保持一致）
                            
                            # 🛡️ 关键：检查粉丝是否属于真实网络中的用户
                            if fan_id not in network_users:
                                user_filtered_edges += 1
                                filtered_edges_count += 1
                                continue  # 跳过：粉丝不在真实网络中，可能是D类或更远的用户
                            
                            # 检查是否已存在（在原始边或新边中）
                            edge_exists = False
                            
                            # 检查原始边
                            if not edges_df[(edges_df['source'] == user_id) & (edges_df['target'] == fan_id)].empty:
                                edge_exists = True
                            
                            # 检查新边
                            if not edge_exists and new_edge in new_edges:
                                edge_exists = True
                            
                            if not edge_exists:
                                new_edges.append(new_edge)
                                new_edges_in_batch.append(new_edge)
                                user_new_edges.append(new_edge)
                    
                    print(f"  ✅ 用户 {user_id}: 发现 {len(fans_data)} 个粉丝, 新增 {len(user_new_edges)} 条边")
                    if user_filtered_edges > 0:
                        print(f"    🛡️ 已过滤 {user_filtered_edges} 条指向网络外用户的边")
                    
                    if len(user_new_edges) > 0:
                        print(f"    🔥 发现遗漏！用户 {user_id} 实际有 {len(fans_data)} 个粉丝")
                else:
                    print(f"  ✅ 用户 {user_id}: 确实没有粉丝")
                
                # 重置错误计数
                consecutive_errors = 0
                
                # 每处理SAVE_INTERVAL个用户保存一次进度
                if (i + 1) % SAVE_INTERVAL == 0:
                    save_progress(processed_users, new_edges, TARGET_NETWORK_PATH)
                    
                    # 计算速度统计
                    batch_duration = datetime.now() - batch_start_time
                    avg_time_per_user = batch_duration.total_seconds() / SAVE_INTERVAL
                    remaining_users = len(users_to_process_list) - (i + 1)
                    estimated_remaining_time = remaining_users * avg_time_per_user / 60
                    
                    print(f"  📊 批次完成: 平均每用户 {avg_time_per_user:.1f} 秒")
                    print(f"  📊 本批次发现新边: {len(new_edges_in_batch)} 条")
                    print(f"  📊 本批次过滤边数: {filtered_edges_count} 条")
                    print(f"  📊 预计剩余时间: {estimated_remaining_time:.1f} 分钟")
                    
                    # 重置批次计时
                    batch_start_time = datetime.now()
                    new_edges_in_batch.clear()
                    filtered_edges_count = 0
                
                # 随机等待
                wait_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"  ❌ 用户 {user_id} 处理失败: {e}")
                consecutive_errors += 1
                
                # 如果连续错误过多，增加等待时间
                if consecutive_errors >= 3:
                    print(f"  ⚠️ 连续 {consecutive_errors} 个错误，增加等待时间...")
                    time.sleep(random.uniform(5.0, 10.0))
                
                # 仍然标记为已处理（避免重复尝试）
                processed_users.add(user_id)
                
                continue
            
            # 批次间等待
            if i < len(users_to_process_list) - 1:
                batch_wait = random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
                time.sleep(batch_wait)
        
        # 保存最后的进度
        save_progress(processed_users, new_edges, TARGET_NETWORK_PATH)
        
        # 保存最终结果
        save_final_results(edges_df, new_edges, TARGET_NETWORK_PATH)
        
        print(f"\n" + "="*80)
        print(f"遗漏用户查找完成！")
        print(f"="*80)
        print(f"✅ 处理用户数: {len(zero_outdegree_users)}")
        print(f"✅ 发现新边数: {len(new_edges)}")
        print(f"🛡️ 总过滤边数: {filtered_edges_count}")
        
        if len(new_edges) > 0:
            print(f"🎉 发现 {len(new_edges)} 条遗漏的边！")
            print(f"📊 这些边已补充到edges.csv中")
            print(f"💡 说明：之前的爬取过程中确实存在遗漏")
        else:
            print(f"✅ 没有发现遗漏的边")
            print(f"💡 说明：出度为0的用户确实没有粉丝")
        
        if filtered_edges_count > 0:
            print(f"\n🛡️ 边过滤保护统计:")
            print(f"  - 总共过滤了 {filtered_edges_count} 条指向网络外用户的边")
            print(f"  - 这些边指向的是D类或更远距离的用户，正确被过滤")
            print(f"  - 网络边界得到严格保持，符合fetch3.py的设计原则")
        
        # 统计被恢复的用户
        recovered_users = set()
        for source, target in new_edges:
            recovered_users.add(source)
        
        if recovered_users:
            print(f"\n📊 被恢复的用户统计:")
            print(f"  - 总计 {len(recovered_users)} 个用户被恢复了粉丝")
            print(f"  - 这些用户之前被错误地标记为无粉丝")
            
            # 显示几个示例
            sample_users = list(recovered_users)[:5]
            for user_id in sample_users:
                user_new_edges = [(s, t) for s, t in new_edges if s == user_id]
                print(f"    用户 {user_id}: 恢复了 {len(user_new_edges)} 个粉丝")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 用户中断程序")
        # 保存当前进度
        save_progress(processed_users, new_edges, TARGET_NETWORK_PATH)
        print(f"✅ 当前进度已保存，可稍后继续")
        
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        # 保存当前进度
        save_progress(processed_users, new_edges, TARGET_NETWORK_PATH)
        
    finally:
        finder.cleanup()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n总耗时: {duration}")

if __name__ == "__main__":
    main()