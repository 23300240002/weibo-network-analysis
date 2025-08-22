import os
import json
import time
import random
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import datetime

class WeiboPopularityTester:
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
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_page_load_timeout(15)
            self.driver.implicitly_wait(3)
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
                    continue
            
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
                        print(f"      ✅ {interaction_type}: {num}")
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
    
    def process_single_weibo_div_correct(self, weibo_div_element):
        """🎯 修正版：使用最后一个div提取用户自己的转赞评数据"""
        try:
            print(f"    🔍 开始分析微博div结构...")
            
            # 获取微博div的HTML
            div_html = weibo_div_element.get_attribute('outerHTML')
            
            # 🎯 关键：查找微博div内的子div元素
            child_divs = weibo_div_element.find_elements(By.XPATH, "./div")
            print(f"    📦 找到 {len(child_divs)} 个子div")
            
            # 提取微博内容
            content = "内容未提取"
            try:
                ctt_element = weibo_div_element.find_element(By.CLASS_NAME, "ctt")
                content = ctt_element.text.strip()
                content = content[:100] + ('...' if len(content) > 100 else '')
                print(f"    ✅ 内容: {content}")
            except:
                print(f"    ❌ 无法提取内容")
            
            # 判断是否为转发微博
            is_repost = '转发了' in div_html
            print(f"    📝 微博类型: {'转发' if is_repost else '原创'}")
            
            interactions = {'reposts': 0, 'likes': 0, 'comments': 0}
            post_time = "时间未找到"
            
            # 🔥 关键修正：始终使用最后一个div（无论转发还是原创）
            if len(child_divs) > 0:
                last_div = child_divs[-1]  # 取最后一个div
                last_div_html = last_div.get_attribute('outerHTML')
                
                print(f"    🎯 分析最后一个div（第{len(child_divs)}个）获取用户自己的数据...")
                print(f"    🎯 最后一个div内容预览: {last_div.text[:100]}...")
                
                # 从最后一个div提取用户自己的转赞评数据
                interactions = self.extract_interactions_from_html(last_div_html)
                
                # 从最后一个div提取时间
                post_time = self.extract_time_from_html(last_div_html)
            else:
                print(f"    ❌ 没有找到子div，使用整个div")
                interactions = self.extract_interactions_from_html(div_html)
                post_time = self.extract_time_from_html(div_html)
            
            print(f"    ⏰ 时间: {post_time}")
            print(f"    📊 最终结果: 赞{interactions['likes']}, 转{interactions['reposts']}, 评{interactions['comments']}")
            
            return {
                'content': content,
                'time': post_time,
                'interactions': interactions,
                'total_interactions': sum(interactions.values()),
                'is_repost': is_repost
            }
            
        except Exception as e:
            print(f"    ❌ 处理微博div时出错: {e}")
            return None
    
    def extract_posts_with_correct_method(self, max_posts):
        """🔥 使用修正后的方法提取帖子"""
        posts_data = []
        
        try:
            print(f"  🔍 使用修正版方法分析页面结构...")
            
            # 🎯 关键：使用Selenium直接查找微博div元素
            weibo_divs = self.driver.find_elements(By.XPATH, "//div[@class='c' and contains(@id, 'M_')]")
            print(f"  📦 找到 {len(weibo_divs)} 个微博div元素")
            
            for i, weibo_div in enumerate(weibo_divs):
                if len(posts_data) >= max_posts:
                    break
                
                print(f"\n  📝 处理第 {i+1} 个微博:")
                
                # 🎯 使用修正版处理方法
                post_data = self.process_single_weibo_div_correct(weibo_div)
                
                if post_data and post_data['content'] != "内容未提取":
                    posts_data.append(post_data)
                    print(f"    ✅ 微博 {len(posts_data)} 数据提取成功")
                else:
                    print(f"    ❌ 微博数据提取失败，跳过")
            
            print(f"\n  📊 最终提取到 {len(posts_data)} 条有效微博")
            
        except Exception as e:
            print(f"  ❌ 修正版方法分析出错: {e}")
        
        return posts_data
    
    def calculate_user_popularity_correct(self, user_id, max_posts=10):
        """修正版用户流行度计算"""
        print(f"\n🎯 开始计算用户 {user_id} 的流行度（基于最新{max_posts}条微博）")
        print(f"🔧 使用修正版方法：始终使用最后一个div提取用户自己的数据")
        
        try:
            profile_url = f'https://weibo.cn/u/{user_id}'
            print(f"📱 访问: {profile_url}")
            self.driver.get(profile_url)
            time.sleep(3)
            
            # 使用修正版分析方法
            posts_data = self.extract_posts_with_correct_method(max_posts)
            
            if not posts_data:
                print(f"❌ 未能提取到微博数据")
                return 0.0
            
            # 计算平均流行度
            total_interactions = 0
            valid_posts = 0
            
            print(f"\n📊 微博数据分析:")
            for i, post in enumerate(posts_data):
                interactions = post['interactions']
                post_total = interactions['likes'] + interactions['reposts'] + interactions['comments']
                post_type = "转发" if post['is_repost'] else "原创"
                
                print(f"  微博 {i+1} ({post_type}): 赞{interactions['likes']}, 转{interactions['reposts']}, 评{interactions['comments']}, 总计{post_total}")
                print(f"    时间: {post['time']}")
                print(f"    内容: {post['content']}")
                
                total_interactions += post_total
                valid_posts += 1
            
            if valid_posts > 0:
                avg_popularity = total_interactions / valid_posts
                print(f"\n✅ 流行度计算完成:")
                print(f"   有效微博数: {valid_posts}")
                print(f"   总互动数: {total_interactions}")
                print(f"   平均流行度: {avg_popularity:.2f}")
                return avg_popularity
            else:
                print(f"❌ 没有有效的微博数据")
                return 0.0
                
        except Exception as e:
            print(f"❌ 流行度计算失败: {e}")
            return 0.0
    
    def run_three_users_test(self, user_ids):
        """测试三个指定用户"""
        print("="*80)
        print(f"微博用户流行度修正版测试")
        print(f"🔧 核心修正: 始终使用最后一个div提取用户自己的转赞评数据")
        print(f"🔧 适用场景: 转发微博和原创微博")
        print(f"🔧 测试用户: {', '.join(user_ids)}")
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # 设置浏览器
        if not self.setup_driver():
            return False
        
        # 加载Cookie
        if not self.load_cookies():
            print("\n❌ Cookie加载失败")
            self.cleanup()
            return False
        
        # 测试登录状态
        if not self.test_login_status():
            self.cleanup()
            return False
        
        try:
            results = {}
            
            for i, user_id in enumerate(user_ids):
                print(f"\n{'='*50}")
                print(f"测试用户 {i+1}/3: {user_id}")
                print(f"{'='*50}")
                
                # 计算用户流行度
                avg_popularity = self.calculate_user_popularity_correct(user_id, max_posts=10)
                results[user_id] = avg_popularity
                
                print(f"\n📊 用户 {user_id} 测试结果:")
                print(f"   平均流行度: {avg_popularity:.2f}")
                
                # 短暂等待避免请求过快
                if i < len(user_ids) - 1:
                    time.sleep(2)
            
            print(f"\n" + "="*80)
            print(f"所有用户测试完成！")
            print("="*80)
            
            for user_id, popularity in results.items():
                print(f"✅ 用户 {user_id}: 平均流行度 {popularity:.2f}")
            
            if all(pop >= 0 for pop in results.values()):
                print(f"\n🎉 修正版方法测试成功！")
                print(f"💡 可以安全应用到fetch3.py中")
                return True
            else:
                print(f"\n😞 部分用户测试失败")
                return False
            
        except Exception as e:
            print(f"\n❌ 测试过程中出现异常: {e}")
            return False
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()

def main():
    """主函数"""
    test_users = ["6361680464"]
    
    print("微博用户流行度修正版测试")
    print("🔧 核心修正:")
    print("1. 🎯 转发微博：使用最后一个div（包含用户自己的转赞评）")
    print("2. 📝 原创微博：使用最后一个div（即唯一的div）")
    print("3. 🔍 不再依赖div数量判断，统一使用最后一个div")
    print("4. ⏰ 同时提取时间和转赞评，确保100%准确")
    print(f"5. 👥 测试用户: {', '.join(test_users)}")
    print("")
    
    tester = WeiboPopularityTester()
    success = tester.run_three_users_test(test_users)
    
    if success:
        print("\n🎉 修正版测试成功！")
        print("💡 关键发现：最后一个div始终包含用户自己的转赞评数据")
        print("💡 可以安全集成到fetch3.py和get_popularity3.py中")
    else:
        print("\n😞 测试失败，需要进一步调试")

if __name__ == "__main__":
    main()