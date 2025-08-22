import os
import pandas as pd
from datetime import datetime
import shutil

class NetworkDataCleaner:
    def __init__(self, network_path):
        self.network_path = network_path
        self.edges_file = os.path.join(network_path, 'edges.csv')
        self.users_file = os.path.join(network_path, 'users.csv')
        self.popularity_file = os.path.join(network_path, 'popularity.csv')
        
    def backup_original_files(self):
        """备份原始文件"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = os.path.join(self.network_path, f'backup_before_clean_{timestamp}')
        
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        files_to_backup = []
        
        if os.path.exists(self.edges_file):
            shutil.copy2(self.edges_file, os.path.join(backup_dir, 'edges.csv'))
            files_to_backup.append('edges.csv')
        
        if os.path.exists(self.users_file):
            shutil.copy2(self.users_file, os.path.join(backup_dir, 'users.csv'))
            files_to_backup.append('users.csv')
        
        if os.path.exists(self.popularity_file):
            shutil.copy2(self.popularity_file, os.path.join(backup_dir, 'popularity.csv'))
            files_to_backup.append('popularity.csv')
        
        print(f"✅ 原始文件已备份到: {backup_dir}")
        print(f"   备份文件: {', '.join(files_to_backup)}")
        
        return backup_dir
    
    def analyze_user_impact(self, user_id_to_remove):
        """分析要删除的用户对网络的影响"""
        print(f"\n🔍 分析用户 {user_id_to_remove} 对网络的影响...")
        
        # 分析edges
        if os.path.exists(self.edges_file):
            edges_df = pd.read_csv(self.edges_file)
            edges_df['source'] = edges_df['source'].astype(str)
            edges_df['target'] = edges_df['target'].astype(str)
            
            # 统计相关边
            as_source = edges_df[edges_df['source'] == user_id_to_remove]
            as_target = edges_df[edges_df['target'] == user_id_to_remove]
            total_related_edges = len(as_source) + len(as_target)
            
            print(f"📊 边数据分析:")
            print(f"   总边数: {len(edges_df):,}")
            print(f"   作为source的边数: {len(as_source):,} (该用户关注了多少人)")
            print(f"   作为target的边数: {len(as_target):,} (多少人关注了该用户)")
            print(f"   相关边总数: {total_related_edges:,}")
            print(f"   将删除的边占比: {total_related_edges/len(edges_df)*100:.2f}%")
            
            # 分析连接的用户
            connected_users = set()
            connected_users.update(as_source['target'].tolist())
            connected_users.update(as_target['source'].tolist())
            print(f"   直接连接的用户数: {len(connected_users):,}")
        
        # 分析users
        if os.path.exists(self.users_file):
            users_df = pd.read_csv(self.users_file)
            users_df['user_id'] = users_df['user_id'].astype(str)
            
            user_exists = user_id_to_remove in users_df['user_id'].values
            print(f"📊 用户数据分析:")
            print(f"   总用户数: {len(users_df):,}")
            print(f"   目标用户存在: {'是' if user_exists else '否'}")
        
        # 分析popularity
        if os.path.exists(self.popularity_file):
            popularity_df = pd.read_csv(self.popularity_file)
            popularity_df['user_id'] = popularity_df['user_id'].astype(str)
            
            user_exists = user_id_to_remove in popularity_df['user_id'].values
            print(f"📊 流行度数据分析:")
            print(f"   总用户数: {len(popularity_df):,}")
            print(f"   目标用户存在: {'是' if user_exists else '否'}")
    
    def clean_edges_data(self, user_id_to_remove):
        """清洗edges数据"""
        if not os.path.exists(self.edges_file):
            print(f"❌ edges.csv文件不存在: {self.edges_file}")
            return False
        
        print(f"\n🧹 清洗edges数据...")
        
        # 读取原始数据
        edges_df = pd.read_csv(self.edges_file)
        original_count = len(edges_df)
        
        # 转换为字符串类型确保一致性
        edges_df['source'] = edges_df['source'].astype(str)
        edges_df['target'] = edges_df['target'].astype(str)
        
        # 删除包含目标用户的所有边
        cleaned_edges_df = edges_df[
            (edges_df['source'] != user_id_to_remove) & 
            (edges_df['target'] != user_id_to_remove)
        ]
        
        removed_count = original_count - len(cleaned_edges_df)
        
        # 保存清洗后的数据
        new_edges_file = os.path.join(self.network_path, 'new_edges.csv')
        cleaned_edges_df.to_csv(new_edges_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ edges数据清洗完成:")
        print(f"   原始边数: {original_count:,}")
        print(f"   删除边数: {removed_count:,}")
        print(f"   剩余边数: {len(cleaned_edges_df):,}")
        print(f"   删除比例: {removed_count/original_count*100:.2f}%")
        print(f"   新文件: {new_edges_file}")
        
        return True
    
    def clean_users_data(self, user_id_to_remove):
        """清洗users数据"""
        if not os.path.exists(self.users_file):
            print(f"⚠️ users.csv文件不存在: {self.users_file}")
            return True
        
        print(f"\n🧹 清洗users数据...")
        
        # 读取原始数据
        users_df = pd.read_csv(self.users_file)
        original_count = len(users_df)
        
        # 转换为字符串类型确保一致性
        users_df['user_id'] = users_df['user_id'].astype(str)
        
        # 删除目标用户
        cleaned_users_df = users_df[users_df['user_id'] != user_id_to_remove]
        
        removed_count = original_count - len(cleaned_users_df)
        
        # 保存清洗后的数据
        new_users_file = os.path.join(self.network_path, 'new_users.csv')
        cleaned_users_df.to_csv(new_users_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ users数据清洗完成:")
        print(f"   原始用户数: {original_count:,}")
        print(f"   删除用户数: {removed_count}")
        print(f"   剩余用户数: {len(cleaned_users_df):,}")
        print(f"   新文件: {new_users_file}")
        
        return True
    
    def clean_popularity_data(self, user_id_to_remove):
        """清洗popularity数据"""
        if not os.path.exists(self.popularity_file):
            print(f"⚠️ popularity.csv文件不存在: {self.popularity_file}")
            return True
        
        print(f"\n🧹 清洗popularity数据...")
        
        # 读取原始数据
        popularity_df = pd.read_csv(self.popularity_file)
        original_count = len(popularity_df)
        
        # 转换为字符串类型确保一致性
        popularity_df['user_id'] = popularity_df['user_id'].astype(str)
        
        # 删除目标用户
        cleaned_popularity_df = popularity_df[popularity_df['user_id'] != user_id_to_remove]
        
        removed_count = original_count - len(cleaned_popularity_df)
        
        # 保存清洗后的数据
        new_popularity_file = os.path.join(self.network_path, 'new_popularity.csv')
        cleaned_popularity_df.to_csv(new_popularity_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ popularity数据清洗完成:")
        print(f"   原始用户数: {original_count:,}")
        print(f"   删除用户数: {removed_count}")
        print(f"   剩余用户数: {len(cleaned_popularity_df):,}")
        print(f"   新文件: {new_popularity_file}")
        
        return True
    
    def generate_cleaning_report(self, user_id_to_remove, backup_dir):
        """生成清洗报告"""
        report_file = os.path.join(self.network_path, f'cleaning_report_{user_id_to_remove}.txt')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"网络数据清洗报告\n")
            f.write(f"{'='*50}\n")
            f.write(f"清洗时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"目标网络: {self.network_path}\n")
            f.write(f"清洗用户ID: {user_id_to_remove}\n")
            f.write(f"备份目录: {backup_dir}\n\n")
            
            f.write(f"清洗原因:\n")
            f.write(f"该用户是垃圾节点，与大量用户相连，导致二跳邻居网络异常庞大，\n")
            f.write(f"严重影响网络指标计算效率。删除后可显著提升计算性能。\n\n")
            
            f.write(f"生成文件:\n")
            f.write(f"- new_edges.csv: 清洗后的边数据\n")
            f.write(f"- new_users.csv: 清洗后的用户数据\n")
            f.write(f"- new_popularity.csv: 清洗后的流行度数据\n\n")
            
            f.write(f"使用建议:\n")
            f.write(f"1. 检查清洗效果是否符合预期\n")
            f.write(f"2. 如果满意，可将new_*.csv文件重命名替换原文件\n")
            f.write(f"3. 重新运行create3.py计算网络指标\n")
            f.write(f"4. 如有问题，可从备份目录恢复原始数据\n")
        
        print(f"📋 清洗报告已生成: {report_file}")
    
    def clean_network(self, user_id_to_remove):
        """执行完整的网络清洗流程"""
        print(f"🚀 开始清洗网络数据...")
        print(f"📁 目标网络: {self.network_path}")
        print(f"🗑️ 要删除的用户: {user_id_to_remove}")
        
        # 分析影响
        self.analyze_user_impact(user_id_to_remove)
        
        # 确认操作
        print(f"\n⚠️ 即将删除用户 {user_id_to_remove} 的所有相关数据")
        confirm = input("确认继续？(y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ 用户取消操作")
            return False
        
        # 备份原始文件
        backup_dir = self.backup_original_files()
        
        # 执行清洗
        success = True
        success &= self.clean_edges_data(user_id_to_remove)
        success &= self.clean_users_data(user_id_to_remove)
        success &= self.clean_popularity_data(user_id_to_remove)
        
        if success:
            # 生成报告
            self.generate_cleaning_report(user_id_to_remove, backup_dir)
            
            print(f"\n🎉 网络数据清洗完成！")
            print(f"📁 清洗后的文件:")
            print(f"   - new_edges.csv")
            print(f"   - new_users.csv") 
            print(f"   - new_popularity.csv")
            print(f"📁 原始文件备份在: {backup_dir}")
            print(f"\n💡 使用建议:")
            print(f"   1. 检查清洗效果是否符合预期")
            print(f"   2. 如果满意，用new_*.csv替换原始文件")
            print(f"   3. 重新运行create3.py计算网络指标")
            print(f"   4. 如有问题，从备份目录恢复原始数据")
            
            return True
        else:
            print(f"❌ 清洗过程中出现错误")
            return False

def get_user_input():
    """获取用户输入"""
    print("="*60)
    print("网络数据清洗工具")
    print("="*60)
    print("功能: 删除指定用户的所有相关数据（边、用户信息、流行度）")
    print("用途: 清理垃圾节点，如微博'新手指南'等影响网络质量的用户")
    print("="*60)
    
    # 获取网络路径
    default_path = 'C:/Tengfei/data/data/domain_network3/user_3855570307'
    network_path = input(f"请输入网络数据路径 (默认: {default_path}): ").strip()
    if not network_path:
        network_path = default_path
    
    # 检查路径是否存在
    if not os.path.exists(network_path):
        print(f"❌ 路径不存在: {network_path}")
        return None, None
    
    # 获取要删除的用户ID
    print(f"\n常见垃圾用户ID:")
    print(f"  - 2671109275 (微博新手指南)")
    print(f"  - 可以输入其他发现的垃圾用户ID")
    
    user_id = input(f"\n请输入要删除的用户ID: ").strip()
    if not user_id:
        print(f"❌ 用户ID不能为空")
        return None, None
    
    return network_path, user_id

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"网络数据清洗开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 获取用户输入
    network_path, user_id_to_remove = get_user_input()
    if not network_path or not user_id_to_remove:
        return
    
    # 执行清洗
    cleaner = NetworkDataCleaner(network_path)
    success = cleaner.clean_network(user_id_to_remove)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    if success:
        print(f"\n✅ 清洗完成! 耗时: {duration}")
    else:
        print(f"\n❌ 清洗失败! 耗时: {duration}")

if __name__ == "__main__":
    main()