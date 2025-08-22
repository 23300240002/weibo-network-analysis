import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_weibo_cookies():
    """获取微博CN的Cookie"""
    print("正在启动浏览器获取Cookie...")
    
    # 设置Chrome选项
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # 访问微博CN
        print("正在访问 https://weibo.cn ...")
        driver.get('https://weibo.cn')
        
        print("\n" + "="*60)
        print("请在打开的浏览器中手动登录微博账号")
        print("登录完成后，按回车键继续...")
        print("="*60)
        
        # 等待用户登录
        input()
        
        # 获取Cookie
        cookies = driver.get_cookies()
        
        # 保存Cookie到文件
        cookie_file = 'C:/Tengfei/data/crawler/crawler_for_weibo_fans-master/cookie.json'
        with open(cookie_file, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Cookie已保存到: {cookie_file}")
        print(f"✅ 共获取到 {len(cookies)} 个Cookie项")
        
        # 显示关键Cookie
        key_cookies = ['SUB', 'SUBP', 'SSOLoginState', '_T_WM', 'ALF', 'SCF']
        print("\n关键Cookie:")
        for cookie in cookies:
            if cookie['name'] in key_cookies:
                print(f"  {cookie['name']}: {cookie['value'][:20]}...")
        
        driver.quit()
        return True
        
    except Exception as e:
        print(f"❌ 获取Cookie失败: {e}")
        return False

if __name__ == "__main__":
    print("微博Cookie获取工具")
    print("注意：请确保Chrome浏览器已安装")
    
    success = get_weibo_cookies()
    
    if success:
        print("\n✅ Cookie获取成功！")
        print("现在可以运行 fetch3.py 进行爬取了")
    else:
        print("\n❌ Cookie获取失败，请检查网络连接和浏览器设置")