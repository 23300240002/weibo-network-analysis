# 导入需要的数据包
from selenium import webdriver
import time
import json
import os

# 首先创建函数(*^_^*)
def get_cookies():
    # 确保保存目录存在
    save_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    # 保存cookies的文件 - 修正文件名
    cookie_file = os.path.join(save_dir, 'cookie.json')  # 修正：文件名改为 cookie.json
    
    print(f"Cookie将保存到: {cookie_file}")

    # 打开需要获取cookies的网站
    wb = webdriver.Chrome()
    wb.implicitly_wait(3)
    wb.get('https://weibo.cn')

    # 网站打开后，在时间内手动执行登录操作
    print("请在120秒内完成登录...")
    time.sleep(120)

    # 登录成功后，获取cookies并保存为json格式
    cookies = wb.get_cookies()
    print("获取到的cookies:")
    print(cookies)
    print(f"Cookie类型: {type(cookies)}")
    
    # 修正：使用正确的文件保存方式
    try:
        with open(cookie_file, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, indent=2, ensure_ascii=False)
        print(f"✅ Cookie已成功保存到: {cookie_file}")
        
        # 验证文件是否存在
        if os.path.exists(cookie_file):
            print(f"✅ 文件确认存在: {cookie_file}")
            # 显示文件大小
            file_size = os.path.getsize(cookie_file)
            print(f"✅ 文件大小: {file_size} 字节")
        else:
            print(f"❌ 文件保存失败: {cookie_file}")
            
    except Exception as e:
        print(f"❌ 保存cookie时出错: {e}")

    # 关闭浏览器
    wb.quit()  # 修正：使用 quit() 而不是 close()
    print("浏览器已关闭")

# 执行代码获得cookie
if __name__ == "__main__":
    print("开始获取微博cookie...")
    get_cookies()
    print("cookie获取完成!")