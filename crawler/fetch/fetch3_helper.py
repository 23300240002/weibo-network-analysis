import os
import json
import time
import random
import pandas as pd
import requests
from datetime import datetime
import re

# 🎯 配置：与fetch3.py保持一致的参数
TARGET_NETWORK_PATH = 'C:/Tengfei/data/data/domain_network3/user_3855570307'
COOKIE_PATH = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'

# 速度参数（与fetch3.py保持一致）
SLEEP_MIN = 0.4
SLEEP_MAX = 0.6
BATCH_INTERVAL_MIN = 0.5
BATCH_INTERVAL_MAX = 1.0

# 进度保存参数
SAVE_INTERVAL = 20  # 每处理20个用户保存一次进度
MAX_RETRIES = 3     # 最大重试次数

class WeiboTotalPopularityHelper:
    def __init__(self, cookie_path=COOKIE_PATH):
        self.cookie_path = cookie_path
        self.headers = None
        
    def load_cookies_and_setup_headers(self):
        """加载cookies并设置请求头"""
        if not os.path.exists(self.cookie_path):
            print(f"❌ 未找到cookie文件: {self.cookie_path}")
            return False
        
        try:
            with open(self.cookie_path, 'r', encoding='utf-8') as f:
                cookies_list = json.load(f)
            
            # 将cookies列表转换为字符串
            cookie_str = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies_list])
            
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Cookie': cookie_str,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Referer': 'https://weibo.com',
            }
            
            print("✅ Cookie和请求头设置成功")
            return True
            
        except Exception as e:
            print(f"❌ Cookie加载失败: {e}")
            return False
    
    def test_login_status(self):
        """测试登录状态"""
        test_url = 'https://weibo.com/ajax/statuses/mymblog?uid=1234567890&page=1'
        
        try:
            response = requests.get(test_url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'ok' in data and data['ok'] == 1:
                    print("✅ 登录状态正常")
                    return True
                else:
                    print("⚠️ 登录可能有问题，但继续尝试")
                    return True
            else:
                print(f"❌ 登录状态检查失败，状态码: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ 登录状态检查异常: {e}")
            return False
    
    def get_user_profile_info(self, user_id, max_retries=MAX_RETRIES):
        """📊 获取用户资料信息，包括总转赞评数和发帖数（参考fetch_following.py）"""
        url = f'https://weibo.com/ajax/profile/info?uid={user_id}'
        
        for retry in range(max_retries):
            try:
                # 计算退避时间
                wait_time = (2 ** retry) * 1 if retry > 0 else 0
                if retry > 0:
                    print(f"    第{retry+1}次重试，等待{wait_time}秒...")
                    time.sleep(wait_time)
                
                response = requests.get(url, headers=self.headers, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'data' in data and 'user' in data['data']:
                        user_info = data['data']['user']
                        
                        # 获取发帖总数
                        statuses_count = user_info.get('statuses_count', 0)
                        
                        # 获取总转赞评数据
                        status_counter = user_info.get('status_total_counter', {})
                        
                        if status_counter:
                            # 🔥 参考fetch_following.py的处理方法
                            reposts_count = int(str(status_counter.get('repost_cnt', '0')).replace(',', ''))
                            attitudes_count = int(str(status_counter.get('like_cnt', '0')).replace(',', ''))
                            comments_count = int(str(status_counter.get('comment_cnt', '0')).replace(',', ''))
                        else:
                            reposts_count = 0
                            attitudes_count = 0
                            comments_count = 0
                        
                        # 计算总互动数
                        total_interactions = reposts_count + attitudes_count + comments_count
                        
                        # 计算总体平均流行度
                        if statuses_count > 0:
                            avg_popularity_of_all = total_interactions / statuses_count
                        else:
                            avg_popularity_of_all = 0.0
                        
                        result = {
                            'statuses_count': statuses_count,
                            'reposts_count': reposts_count,
                            'attitudes_count': attitudes_count,
                            'comments_count': comments_count,
                            'total_interactions': total_interactions,
                            'avg_popularity_of_all': avg_popularity_of_all
                        }
                        
                        return result
                    else:
                        print(f"    ⚠️ 用户 {user_id} 数据格式异常")
                        return None
                else:
                    print(f"    ⚠️ 用户 {user_id} 请求失败，状态码: {response.status_code}")
                    
            except Exception as e:
                print(f"    ⚠️ 用户 {user_id} 请求异常: {e}")
        
        # 所有重试都失败
        print(f"    ❌ 用户 {user_id} 获取失败，已重试{max_retries}次")
        return None

def load_existing_popularity_data(network_path):
    """加载现有的popularity.csv文件"""
    popularity_file = os.path.join(network_path, 'popularity.csv')
    
    if not os.path.exists(popularity_file):
        print(f"❌ 未找到popularity.csv文件: {popularity_file}")
        return None
    
    try:
        popularity_df = pd.read_csv(popularity_file)
        print(f"✅ 成功加载popularity.csv，包含 {len(popularity_df)} 个用户")
        
        # 检查现有列
        print(f"📋 现有列: {list(popularity_df.columns)}")
        
        return popularity_df
        
    except Exception as e:
        print(f"❌ 加载popularity.csv失败: {e}")
        return None

def save_progress(processed_data, network_path):
    """保存进度到临时文件"""
    progress_file = os.path.join(network_path, 'helper_progress.json')
    
    progress_data = {
        'processed_data': processed_data,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_processed': len(processed_data)
    }
    
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 进度已保存: {len(processed_data)} 个用户完成")

def load_progress(network_path):
    """加载进度"""
    progress_file = os.path.join(network_path, 'helper_progress.json')
    
    if not os.path.exists(progress_file):
        return {}
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        
        processed_data = progress_data.get('processed_data', {})
        timestamp = progress_data.get('timestamp', '未知')
        
        print(f"📁 加载进度文件: {progress_file}")
        print(f"  📊 已处理用户: {len(processed_data)} 个")
        print(f"  📊 保存时间: {timestamp}")
        
        return processed_data
        
    except Exception as e:
        print(f"❌ 加载进度文件失败: {e}")
        return {}

def update_popularity_csv(original_df, processed_data, network_path):
    """更新popularity.csv文件，添加avg_popularity_of_all列"""
    # 创建备份
    backup_dir = os.path.join(network_path, 'backup')
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(backup_dir, f'popularity_backup_{timestamp}.csv')
    original_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
    print(f"✅ 原始popularity.csv已备份: {backup_file}")
    
    # 复制原始数据
    updated_df = original_df.copy()
    
    # 🔥 关键修复：统一数据类型为字符串，解决类型匹配问题
    updated_df['user_id'] = updated_df['user_id'].astype(str)
    
    # 添加新列
    updated_df['avg_popularity_of_all'] = 0.0
    
    # 🔥 修复：更新数据时确保类型匹配并添加调试信息
    successful_updates = 0
    failed_updates = 0
    total_updates = len(processed_data)
    
    print(f"\n🔧 开始数据更新，总计 {total_updates} 个用户...")
    
    for user_id, data in processed_data.items():
        # 🔥 关键：确保user_id统一为字符串格式
        user_id_str = str(user_id).strip()
        
        # 🔥 新增：处理浮点数格式（如"3855570307.0" -> "3855570307"）
        if '.' in user_id_str:
            try:
                user_id_str = str(int(float(user_id_str)))
            except:
                pass
        
        mask = updated_df['user_id'] == user_id_str
        
        if mask.any():
            updated_df.loc[mask, 'avg_popularity_of_all'] = data['avg_popularity_of_all']
            successful_updates += 1
        else:
            failed_updates += 1
            if failed_updates <= 5:  # 只显示前5个失败的例子
                print(f"⚠️ 警告：用户 {user_id_str} 在DataFrame中未找到")
                # 🔥 调试信息：显示DataFrame中的实际格式
                sample_ids = updated_df['user_id'].head(3).tolist()
                print(f"   DataFrame样本ID: {sample_ids}")
                print(f"   查找的ID: '{user_id_str}' (类型: {type(user_id_str)})")
    
    print(f"\n🔧 数据更新结果:")
    print(f"  ✅ 成功更新: {successful_updates}/{total_updates}")
    print(f"  ❌ 更新失败: {failed_updates}/{total_updates}")
    print(f"  📊 成功率: {successful_updates/total_updates*100:.1f}%")
    
    # 保存更新后的文件
    popularity_file = os.path.join(network_path, 'popularity.csv')
    updated_df.to_csv(popularity_file, index=False, encoding='utf-8-sig')
    print(f"✅ 更新后的popularity.csv已保存: {popularity_file}")
    
    # 🔥 新增：强制验证写入结果
    print(f"\n🔍 验证写入结果...")
    verification_df = pd.read_csv(popularity_file)
    non_zero_count = (verification_df['avg_popularity_of_all'] > 0).sum()
    total_count = len(verification_df)
    
    print(f"🔍 验证结果:")
    print(f"  📊 总用户数: {total_count}")
    print(f"  📊 avg_popularity_of_all > 0的用户数: {non_zero_count}")
    print(f"  📊 非零比例: {non_zero_count/total_count*100:.1f}%")
    
    if non_zero_count == 0:
        print(f"❌ 严重问题：所有用户的avg_popularity_of_all都是0！")
        print(f"   这说明数据更新完全失败，需要检查类型匹配问题")
        
        # 🔥 额外调试：检查原始数据类型
        print(f"\n🔧 调试信息:")
        print(f"   原始DataFrame user_id类型: {original_df['user_id'].dtype}")
        print(f"   更新后DataFrame user_id类型: {updated_df['user_id'].dtype}")
        print(f"   processed_data样本keys: {list(processed_data.keys())[:3]}")
    else:
        print(f"✅ 数据更新成功！")
    
    # 显示统计信息
    if non_zero_count > 0:
        print(f"\n📊 影响力对比（基于成功更新的数据）:")
        if 'avg_popularity' in updated_df.columns:
            mask_valid = updated_df['avg_popularity_of_all'] > 0
            valid_data = updated_df[mask_valid]
            
            if len(valid_data) > 0:
                print(f"  📊 最新10条平均影响力: {valid_data['avg_popularity'].mean():.2f}")
                print(f"  📊 总体平均影响力: {valid_data['avg_popularity_of_all'].mean():.2f}")
                
                # 统计为0的情况
                zero_recent = (updated_df['avg_popularity'] == 0).sum()
                zero_total = (updated_df['avg_popularity_of_all'] == 0).sum()
                print(f"  📊 最新10条影响力为0的用户: {zero_recent} 个 ({zero_recent/len(updated_df)*100:.1f}%)")
                print(f"  📊 总体影响力为0的用户: {zero_total} 个 ({zero_total/len(updated_df)*100:.1f}%)")
    
    # 清理进度文件
    progress_file = os.path.join(network_path, 'helper_progress.json')
    if os.path.exists(progress_file):
        os.remove(progress_file)
        print(f"✅ 进度文件已清理")

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"总体影响力补充开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("微博总体影响力补充工具 v1.0")
    print(f"🎯 目标网络: {TARGET_NETWORK_PATH}")
    print(f"🔍 任务: 为所有用户补充avg_popularity_of_all（总转赞评/总发帖数）")
    print(f"📊 数据源: weibo.com用户资料页")
    print(f"🔄 特性: 断点续传、自动备份、进度保存")
    print(f"⚡ 速度参数: 与fetch3.py保持一致")
    print("=" * 80)
    
    # 检查目标网络路径
    if not os.path.exists(TARGET_NETWORK_PATH):
        print(f"❌ 目标网络路径不存在: {TARGET_NETWORK_PATH}")
        return False
    
    # 加载现有popularity.csv
    print(f"\n🔍 第一步：加载现有数据")
    popularity_df = load_existing_popularity_data(TARGET_NETWORK_PATH)
    
    if popularity_df is None:
        return False
    
    # 检查是否已经有avg_popularity_of_all列
    if 'avg_popularity_of_all' in popularity_df.columns:
        print(f"⚠️ 发现已存在avg_popularity_of_all列")
        overwrite = input("是否覆盖现有数据？(y/n): ").strip().lower()
        if overwrite != 'y':
            print("❌ 用户取消操作")
            return False
    
    # 获取用户列表
    user_list = popularity_df['user_id'].astype(str).tolist()
    print(f"📋 需要处理的用户总数: {len(user_list)}")
    
    # 加载进度
    print(f"\n🔄 第二步：检查进度状态")
    processed_data = load_progress(TARGET_NETWORK_PATH)
    
    # 计算需要处理的用户
    users_to_process = [user_id for user_id in user_list if user_id not in processed_data]
    
    print(f"\n📋 处理计划:")
    print(f"  📊 总用户数: {len(user_list)}")
    print(f"  📊 已处理用户数: {len(processed_data)}")
    print(f"  📊 待处理用户数: {len(users_to_process)}")
    
    if len(users_to_process) == 0:
        print(f"✅ 所有用户已处理完成！")
        # 直接更新CSV文件
        update_popularity_csv(popularity_df, processed_data, TARGET_NETWORK_PATH)
        return True
    
    # 确认是否继续
    print(f"\n⚠️ 预计需要爬取 {len(users_to_process)} 个用户的资料信息")
    print(f"⚠️ 按平均每用户3秒计算，大约需要 {len(users_to_process) * 3 / 60:.1f} 分钟")
    
    confirm = input("是否继续？(y/n): ").strip().lower()
    if confirm != 'y':
        print("用户取消操作")
        return False
    
    # 初始化爬虫
    print(f"\n🚀 第三步：开始爬取总体影响力")
    helper = WeiboTotalPopularityHelper()
    
    if not helper.load_cookies_and_setup_headers():
        print("请先确保cookie文件有效")
        return False
    
    if not helper.test_login_status():
        print("登录状态检查失败，但继续尝试")
    
    try:
        processed_count = len(processed_data)
        total_users = len(user_list)
        
        print(f"开始处理剩余的 {len(users_to_process)} 个用户...")
        
        batch_start_time = datetime.now()
        consecutive_errors = 0
        success_count = 0
        
        for i, user_id in enumerate(users_to_process):
            processed_count += 1
            completion = processed_count / total_users * 100
            
            print(f"\n处理用户 {user_id} [{i+1}/{len(users_to_process)}] (总进度: {completion:.1f}%):")
            
            try:
                # 🔥 获取总体影响力数据
                profile_data = helper.get_user_profile_info(user_id)
                
                if profile_data:
                    processed_data[user_id] = profile_data
                    success_count += 1
                    
                    print(f"  ✅ 用户 {user_id}: 发帖{profile_data['statuses_count']}条, "
                          f"总互动{profile_data['total_interactions']}, "
                          f"总体影响力{profile_data['avg_popularity_of_all']:.2f}")
                else:
                    print(f"  ❌ 用户 {user_id}: 获取失败")
                    # 即使失败也要记录，避免重复尝试
                    processed_data[user_id] = {
                        'statuses_count': 0,
                        'reposts_count': 0,
                        'attitudes_count': 0,
                        'comments_count': 0,
                        'total_interactions': 0,
                        'avg_popularity_of_all': 0.0
                    }
                
                # 重置错误计数
                consecutive_errors = 0
                
                # 每处理SAVE_INTERVAL个用户保存一次进度
                if (i + 1) % SAVE_INTERVAL == 0:
                    save_progress(processed_data, TARGET_NETWORK_PATH)
                    
                    # 计算速度统计
                    batch_duration = datetime.now() - batch_start_time
                    avg_time_per_user = batch_duration.total_seconds() / SAVE_INTERVAL
                    remaining_users = len(users_to_process) - (i + 1)
                    estimated_remaining_time = remaining_users * avg_time_per_user / 60
                    
                    print(f"  📊 批次完成: 平均每用户 {avg_time_per_user:.1f} 秒")
                    print(f"  📊 本批次成功率: {success_count/SAVE_INTERVAL*100:.1f}%")
                    print(f"  📊 预计剩余时间: {estimated_remaining_time:.1f} 分钟")
                    
                    # 重置批次计时和计数
                    batch_start_time = datetime.now()
                    success_count = 0
                
                # 随机等待（与fetch3.py保持一致）
                wait_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"  ❌ 用户 {user_id} 处理失败: {e}")
                consecutive_errors += 1
                
                # 如果连续错误过多，增加等待时间
                if consecutive_errors >= 3:
                    print(f"  ⚠️ 连续 {consecutive_errors} 个错误，增加等待时间...")
                    time.sleep(random.uniform(5.0, 10.0))
                
                # 记录失败的用户
                processed_data[user_id] = {
                    'statuses_count': 0,
                    'reposts_count': 0,
                    'attitudes_count': 0,
                    'comments_count': 0,
                    'total_interactions': 0,
                    'avg_popularity_of_all': 0.0
                }
                
                continue
            
            # 批次间等待
            if i < len(users_to_process) - 1:
                batch_wait = random.uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX)
                time.sleep(batch_wait)
        
        # 保存最后的进度
        save_progress(processed_data, TARGET_NETWORK_PATH)
        
        # 更新popularity.csv文件
        update_popularity_csv(popularity_df, processed_data, TARGET_NETWORK_PATH)
        
        print(f"\n" + "="*80)
        print(f"总体影响力补充完成！")
        print(f"="*80)
        print(f"✅ 处理用户数: {len(user_list)}")
        print(f"✅ 成功用户数: {len([d for d in processed_data.values() if d['avg_popularity_of_all'] > 0])}")
        print(f"✅ 数据已更新到popularity.csv")
        
        print(f"\n🎉 现在popularity.csv包含两种影响力衡量方法：")
        print(f"  - avg_popularity: 最新10条微博的转赞评平均值")
        print(f"  - avg_popularity_of_all: 总转赞评数/总发帖数")
        print(f"  - 可以在相关性分析中对比这两种方法的效果")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 用户中断程序")
        # 保存当前进度
        save_progress(processed_data, TARGET_NETWORK_PATH)
        print(f"✅ 当前进度已保存，可稍后继续")
        
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        # 保存当前进度
        save_progress(processed_data, TARGET_NETWORK_PATH)
        
    finally:
        pass
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n总耗时: {duration}")

if __name__ == "__main__":
    main()