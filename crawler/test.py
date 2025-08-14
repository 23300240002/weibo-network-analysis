import os
import sys
import time
import json
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime

class WeiboFansTest:
    def __init__(self):
        self.driver = None
        self.cookie_path = 'crawler/crawler_for_weibo_fans-master/cookie.json'
        
    def setup_driver(self):
        """设置Chrome浏览器"""
        print("正在设置浏览器...")
        
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 可选：无头模式（后台运行）
        # chrome_options.add_argument('--headless')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("浏览器设置成功")
            return True
        except Exception as e:
            print(f"浏览器设置失败: {e}")
            return False
    
    def load_cookies(self):
        """加载cookie"""
        if not os.path.exists(self.cookie_path):
            print(f"未找到cookie文件: {self.cookie_path}")
            print("需要先运行get_cookie.py获取cookie")
            return False
        
        try:
            with open(self.cookie_path, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            
            # 先访问微博页面
            self.driver.get('https://weibo.cn')
            time.sleep(2)
            
            # 添加cookie
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"添加cookie失败: {e}")
            
            print("Cookie加载成功")
            return True
        except Exception as e:
            print(f"Cookie加载失败: {e}")
            return False
    
    def test_login_status(self):
        """测试登录状态"""
        try:
            self.driver.get('https://weibo.cn')
            time.sleep(3)
            
            # 检查是否需要登录
            page_source = self.driver.page_source
            if '登录' in page_source and '密码' in page_source:
                print("❌ 登录状态检查失败，需要重新登录")
                return False
            else:
                print("✅ 登录状态正常")
                return True
        except Exception as e:
            print(f"登录状态检查异常: {e}")
            return False
    
    def get_user_info(self, user_id):
        """获取用户基本信息，包括粉丝数和关注数"""
        try:
            info_url = f'https://weibo.cn/{user_id}/info'
            self.driver.get(info_url)
            time.sleep(2)
            
            page_source = self.driver.page_source
            
            # 从页面源代码中提取粉丝数和关注数
            fans_count = 0
            following_count = 0
            
            # 查找粉丝数信息
            if '粉丝[' in page_source:
                import re
                fans_match = re.search(r'粉丝\[(\d+)\]', page_source)
                if fans_match:
                    fans_count = int(fans_match.group(1))
            
            # 查找关注数信息  
            if '关注[' in page_source:
                following_match = re.search(r'关注\[(\d+)\]', page_source)
                if following_match:
                    following_count = int(following_match.group(1))
            
            return fans_count, following_count
            
        except Exception as e:
            print(f"获取用户信息失败: {e}")
            return 0, 0
    
    def test_get_user_fans_complete(self, user_id, max_pages=None):
        """完整爬取用户粉丝列表"""
        print(f"\n开始完整爬取用户 {user_id} 的粉丝列表...")
        
        # 首先获取用户的预期粉丝数
        expected_fans, expected_following = self.get_user_info(user_id)
        print(f"用户 {user_id} 预期粉丝数: {expected_fans}, 预期关注数: {expected_following}")
        
        # 如果没有指定最大页数，根据预期粉丝数计算
        if max_pages is None:
            # 假设每页大约10-20个粉丝，增加一些余量
            estimated_pages = max(20, (expected_fans // 10) + 5)
            max_pages = min(estimated_pages, 200)  # 设置上限避免无限爬取
            print(f"自动计算最大页数: {max_pages}")
        
        try:
            # 构建用户粉丝页面URL
            fans_url = f'https://weibo.cn/{user_id}/fans'
            print(f"访问粉丝页面: {fans_url}")
            
            self.driver.get(fans_url)
            time.sleep(3)
            
            # 检查页面是否正确加载
            page_source = self.driver.page_source
            
            if '用户不存在' in page_source:
                print(f"❌ 用户 {user_id} 不存在")
                return []
            
            if '登录' in page_source:
                print("❌ 需要登录，cookie可能已失效")
                return []
            
            fans_data = []
            consecutive_empty_pages = 0
            
            for page in range(1, max_pages + 1):
                if page > 1:
                    # 翻页
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/fans?page={page}'
                        self.driver.get(next_page_url)
                        time.sleep(random.uniform(2, 4))  # 增加随机等待时间
                    except Exception as e:
                        print(f"翻页失败: {e}")
                        break
                
                print(f"  正在爬取第 {page} 页...")
                
                # 查找粉丝信息
                try:
                    # 尝试多种选择器来定位粉丝链接
                    selectors = [
                        "//table//a[contains(@href, '/u/')]",
                        "//a[contains(@href, '/u/')]",
                        "//table//a[contains(@href, 'weibo.cn/u/')]",
                        "//div//a[contains(@href, '/u/')]"
                    ]
                    
                    fan_elements = []
                    for selector in selectors:
                        fan_elements = self.driver.find_elements(By.XPATH, selector)
                        if fan_elements:
                            break
                    
                    page_fans = []
                    processed_ids = set()  # 避免重复
                    
                    for element in fan_elements:
                        try:
                            fan_href = element.get_attribute('href')
                            fan_name = element.text.strip()
                            
                            if fan_href and '/u/' in fan_href:
                                # 提取用户ID
                                if 'weibo.cn/u/' in fan_href:
                                    fan_id = fan_href.split('/u/')[-1].split('?')[0].split('/')[0]
                                else:
                                    fan_id = fan_href.split('/u/')[-1].split('?')[0].split('/')[0]
                                
                                # 验证ID是否为数字且未重复处理
                                if fan_id.isdigit() and fan_id not in processed_ids and fan_name:
                                    page_fans.append({
                                        'fan_id': fan_id,
                                        'fan_name': fan_name,
                                        'page': page,
                                        'href': fan_href
                                    })
                                    processed_ids.add(fan_id)
                        except Exception as e:
                            continue
                    
                    print(f"    第 {page} 页找到 {len(page_fans)} 个粉丝")
                    
                    # 如果这一页没有粉丝
                    if len(page_fans) == 0:
                        consecutive_empty_pages += 1
                        print(f"    第 {page} 页无粉丝数据 (连续空页面: {consecutive_empty_pages})")
                        
                        # 如果连续3页都没有数据，可能已经到底了
                        if consecutive_empty_pages >= 3:
                            print(f"    连续 {consecutive_empty_pages} 页无数据，停止爬取")
                            break
                    else:
                        consecutive_empty_pages = 0  # 重置连续空页面计数
                        fans_data.extend(page_fans)
                    
                    # 检查是否已经获取到足够的粉丝
                    if expected_fans > 0 and len(fans_data) >= expected_fans:
                        print(f"    已获取 {len(fans_data)} 个粉丝，达到预期数量 {expected_fans}")
                        break
                        
                except Exception as e:
                    print(f"    第 {page} 页爬取失败: {e}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 5:
                        break
                
                # 随机等待，避免被反爬
                wait_time = random.uniform(1.5, 3.5)
                time.sleep(wait_time)
                
                # 每10页增加额外等待
                if page % 10 == 0:
                    extra_wait = random.uniform(5, 10)
                    print(f"    已爬取 {page} 页，额外等待 {extra_wait:.1f} 秒...")
                    time.sleep(extra_wait)
            
            print(f"✅ 粉丝列表爬取完成，共获取 {len(fans_data)} 个粉丝")
            print(f"   预期粉丝数: {expected_fans}, 实际获取: {len(fans_data)}")
            print(f"   覆盖率: {(len(fans_data)/max(1, expected_fans)*100):.1f}%")
            
            return fans_data
            
        except Exception as e:
            print(f"❌ 获取粉丝列表失败: {e}")
            return []
    
    def test_get_user_following_complete(self, user_id, max_pages=None):
        """完整爬取用户关注列表"""
        print(f"\n开始完整爬取用户 {user_id} 的关注列表...")
        
        # 获取预期关注数
        expected_fans, expected_following = self.get_user_info(user_id)
        print(f"用户 {user_id} 预期关注数: {expected_following}")
        
        # 如果没有指定最大页数，根据预期关注数计算
        if max_pages is None:
            estimated_pages = max(20, (expected_following // 10) + 5)
            max_pages = min(estimated_pages, 200)
            print(f"自动计算最大页数: {max_pages}")
        
        try:
            # 构建用户关注页面URL
            following_url = f'https://weibo.cn/{user_id}/follow'
            print(f"访问关注页面: {following_url}")
            
            self.driver.get(following_url)
            time.sleep(3)
            
            following_data = []
            consecutive_empty_pages = 0
            
            for page in range(1, max_pages + 1):
                if page > 1:
                    try:
                        next_page_url = f'https://weibo.cn/{user_id}/follow?page={page}'
                        self.driver.get(next_page_url)
                        time.sleep(random.uniform(2, 4))
                    except Exception as e:
                        print(f"翻页失败: {e}")
                        break
                
                print(f"  正在爬取第 {page} 页...")
                
                try:
                    # 多种选择器
                    selectors = [
                        "//table//a[contains(@href, '/u/')]",
                        "//a[contains(@href, '/u/')]",
                        "//table//a[contains(@href, 'weibo.cn/u/')]",
                        "//div//a[contains(@href, '/u/')]"
                    ]
                    
                    following_elements = []
                    for selector in selectors:
                        following_elements = self.driver.find_elements(By.XPATH, selector)
                        if following_elements:
                            break
                    
                    page_following = []
                    processed_ids = set()
                    
                    for element in following_elements:
                        try:
                            following_href = element.get_attribute('href')
                            following_name = element.text.strip()
                            
                            if following_href and '/u/' in following_href:
                                if 'weibo.cn/u/' in following_href:
                                    following_id = following_href.split('/u/')[-1].split('?')[0].split('/')[0]
                                else:
                                    following_id = following_href.split('/u/')[-1].split('?')[0].split('/')[0]
                                
                                if following_id.isdigit() and following_id not in processed_ids and following_name:
                                    page_following.append({
                                        'following_id': following_id,
                                        'following_name': following_name,
                                        'page': page,
                                        'href': following_href
                                    })
                                    processed_ids.add(following_id)
                        except Exception as e:
                            continue
                    
                    print(f"    第 {page} 页找到 {len(page_following)} 个关注")
                    
                    if len(page_following) == 0:
                        consecutive_empty_pages += 1
                        print(f"    第 {page} 页无关注数据 (连续空页面: {consecutive_empty_pages})")
                        if consecutive_empty_pages >= 3:
                            print(f"    连续 {consecutive_empty_pages} 页无数据，停止爬取")
                            break
                    else:
                        consecutive_empty_pages = 0
                        following_data.extend(page_following)
                    
                    # 检查是否已经获取到足够的关注
                    if expected_following > 0 and len(following_data) >= expected_following:
                        print(f"    已获取 {len(following_data)} 个关注，达到预期数量 {expected_following}")
                        break
                        
                except Exception as e:
                    print(f"    第 {page} 页爬取失败: {e}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 5:
                        break
                
                time.sleep(random.uniform(1.5, 3.5))
                
                if page % 10 == 0:
                    extra_wait = random.uniform(5, 10)
                    print(f"    已爬取 {page} 页，额外等待 {extra_wait:.1f} 秒...")
                    time.sleep(extra_wait)
            
            print(f"✅ 关注列表爬取完成，共获取 {len(following_data)} 个关注")
            print(f"   预期关注数: {expected_following}, 实际获取: {len(following_data)}")
            print(f"   覆盖率: {(len(following_data)/max(1, expected_following)*100):.1f}%")
            
            return following_data
            
        except Exception as e:
            print(f"❌ 获取关注列表失败: {e}")
            return []
    
    def run_comprehensive_test(self):
        """运行完整爬取测试"""
        print("="*80)
        print("微博粉丝/关注列表完整爬取测试")
        print("="*80)
        
        # 1. 设置浏览器
        if not self.setup_driver():
            return False
        
        # 2. 加载cookie
        if not self.load_cookies():
            print("\n⚠️  Cookie加载失败，请先运行以下步骤：")
            print("1. cd crawler/crawler_for_weibo_fans-master")
            print("2. python get_cookie.py")
            print("3. 在浏览器中登录微博")
            print("4. 再次运行本测试")
            self.cleanup()
            return False
        
        # 3. 测试登录状态
        if not self.test_login_status():
            self.cleanup()
            return False
        
        # 4. 测试用户列表
        test_users = ['5864292176', '6046192156']
        
        all_results = {}
        
        for user_id in test_users:
            print(f"\n{'='*60}")
            print(f"测试用户: {user_id}")
            print(f"{'='*60}")
            
            # 完整爬取粉丝列表
            fans_data = self.test_get_user_fans_complete(user_id)
            
            # 完整爬取关注列表  
            following_data = self.test_get_user_following_complete(user_id)
            
            all_results[user_id] = {
                'fans': fans_data,
                'following': following_data
            }
            
            print(f"\n用户 {user_id} 完整爬取结果:")
            print(f"  粉丝数: {len(fans_data)}")
            print(f"  关注数: {len(following_data)}")
            
            # 显示样例
            if len(fans_data) > 0:
                print(f"  粉丝样例: {fans_data[:3]}")
            if len(following_data) > 0:
                print(f"  关注样例: {following_data[:3]}")
        
        # 5. 保存测试结果
        self.save_test_results(all_results)
        
        # 6. 清理
        self.cleanup()
        
        # 7. 生成总结
        self.generate_summary(all_results)
        
        return True
    
    def save_test_results(self, results):
        """保存测试结果"""
        output_dir = 'crawler/weibo_fans_complete_test_results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 保存详细结果
        with open(f'{output_dir}/complete_test_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 保存CSV格式
        for user_id, data in results.items():
            if data['fans']:
                fans_df = pd.DataFrame(data['fans'])
                fans_df.to_csv(f'{output_dir}/user_{user_id}_complete_fans.csv', index=False)
            
            if data['following']:
                following_df = pd.DataFrame(data['following'])
                following_df.to_csv(f'{output_dir}/user_{user_id}_complete_following.csv', index=False)
        
        print(f"\n完整测试结果已保存到: {output_dir}")
    
    def generate_summary(self, results):
        """生成测试总结"""
        print(f"\n{'='*80}")
        print("完整爬取测试总结")
        print(f"{'='*80}")
        
        total_fans = 0
        total_following = 0
        success_users = 0
        
        for user_id, data in results.items():
            fans_count = len(data['fans'])
            following_count = len(data['following'])
            
            total_fans += fans_count
            total_following += following_count
            
            if fans_count > 0 or following_count > 0:
                success_users += 1
            
            print(f"用户 {user_id}: 粉丝 {fans_count} 个, 关注 {following_count} 个")
        
        print(f"\n总结:")
        print(f"  成功测试用户数: {success_users}/{len(results)}")
        print(f"  总粉丝数: {total_fans}")
        print(f"  总关注数: {total_following}")
        
        if total_fans > 0:
            print(f"\n✅ 完整粉丝列表爬取测试成功！")
            print(f"   该方法可以用于完整爬取微博用户的粉丝列表")
        else:
            print(f"\n❌ 粉丝列表爬取测试失败")
    
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()
            print("浏览器已关闭")

def main():
    """主函数"""
    print("开始微博粉丝/关注列表完整爬取测试...")
    
    # 检查依赖
    try:
        import selenium
        print(f"✅ Selenium版本: {selenium.__version__}")
    except ImportError:
        print("❌ 请先安装selenium: pip install selenium")
        return
    
    # 检查Chrome驱动
    try:
        from selenium import webdriver
        driver = webdriver.Chrome()
        driver.quit()
        print("✅ Chrome驱动正常")
    except Exception as e:
        print(f"❌ Chrome驱动问题: {e}")
        print("请确保已安装Chrome和ChromeDriver")
        return
    
    # 检查项目文件
    project_path = 'crawler/crawler_for_weibo_fans-master'
    if not os.path.exists(project_path):
        print(f"❌ 未找到项目文件夹: {project_path}")
        print("请确保已下载并解压项目到正确位置")
        return
    
    # 运行测试
    tester = WeiboFansTest()
    tester.run_comprehensive_test()

if __name__ == "__main__":
    main()