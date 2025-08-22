import os
import pandas as pd
import numpy as np
from datetime import datetime

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        id_str = str(id_value).strip()
        if id_str == '-2147483648':
            return id_str
        return str(int(float(id_str)))
    except:
        return str(id_value).strip()

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

class AdvancedAnomalyDetector:
    """高级异常用户检测器"""
    
    def __init__(self):
        self.merged_df = None
        self.edges_df = None
        self.popularity_map = {}
        self.user_neighbors = {}  # 存储每个用户的邻居
        
    def load_data(self):
        """加载数据"""
        print("正在加载数据...")
        
        # 🔥 修改：使用新的数据路径，兼容create3.py的输出
        merged_data_path = 'C:/Tengfei/data/results/user_3855570307_metrics/merged_metrics_popularity.csv'
        if not os.path.exists(merged_data_path):
            print(f"错误: 未找到文件 {merged_data_path}")
            return False
        
        self.merged_df = pd.read_csv(merged_data_path)
        self.merged_df['user_id'] = self.merged_df['user_id'].apply(normalize_id)
        
        # 🔥 修改：使用新的边数据路径
        edges_path = 'C:/Tengfei/data/data/domain_network3/user_3855570307/edges.csv'
        if not os.path.exists(edges_path):
            print(f"错误: 未找到文件 {edges_path}")
            return False
            
        self.edges_df = pd.read_csv(edges_path)
        self.edges_df['source'] = self.edges_df['source'].apply(normalize_id)
        self.edges_df['target'] = self.edges_df['target'].apply(normalize_id)
        
        # 创建流行度映射
        self.popularity_map = dict(zip(self.merged_df['user_id'], self.merged_df['avg_popularity']))
        
        # 预处理邻居关系（用于方案三）
        print("正在预处理邻居关系...")
        self.user_neighbors = {}
        for _, row in self.edges_df.iterrows():
            source = row['source']
            target = row['target']
            if source not in self.user_neighbors:
                self.user_neighbors[source] = set()
            self.user_neighbors[source].add(target)
        
        print(f"数据加载完成: {len(self.merged_df)} 个可分析用户")
        
        # 🔥 新增：检查明星用户标识列是否存在
        if 'is_celebrity' in self.merged_df.columns:
            celebrity_count = self.merged_df['is_celebrity'].sum()
            print(f"✅ 检测到明星用户标识列，共有 {celebrity_count} 个明星用户")
        else:
            print(f"⚠️ 未检测到明星用户标识列，方法4将无法使用")
        
        return True
    
    def method1_influence_edge_ratio(self, exclude_pct):
        """方法1: 影响力/连边数比值异常检测"""
        print(f"\n=== 方法1: 影响力/连边数比值检测 (排除前{exclude_pct}%) ===")
        
        # 计算每个用户的连边数
        user_out_edges = self.edges_df['source'].value_counts().to_dict()
        user_in_edges = self.edges_df['target'].value_counts().to_dict()
        
        result_data = []
        for _, row in self.merged_df.iterrows():
            user_id = row['user_id']
            avg_popularity = row['avg_popularity']
            
            out_count = user_out_edges.get(user_id, 0)
            in_count = user_in_edges.get(user_id, 0)
            total_edges = out_count + in_count
            
            influence_edge_ratio = avg_popularity / (total_edges + 1e-10)
            
            result_data.append({
                'user_id': user_id,
                'avg_popularity': avg_popularity,
                'edge_count': total_edges,
                'influence_edge_ratio': influence_edge_ratio
            })
        
        result_df = pd.DataFrame(result_data)
        result_df = result_df.sort_values('influence_edge_ratio', ascending=False)
        
        if exclude_pct == 0:
            abnormal_users = set()
        else:
            n_to_exclude = int(np.ceil(len(result_df) * exclude_pct / 100))
            abnormal_users = set(result_df.head(n_to_exclude)['user_id'])
        
        print(f"检测到 {len(abnormal_users)} 个比值异常用户")
        return abnormal_users
    
    def method2_structural_hole_anomaly(self, exclude_pct):
        """方法2: 结构洞异常检测 - 直接使用CSV中的数据"""
        print(f"\n=== 方法2: 结构洞异常检测 (排除前{exclude_pct}%) ===")
        
        if exclude_pct == 0:
            print("原始网络，无需检测异常用户")
            return set()
        
        # 🔥 修改：检查介数中心性列是否存在
        if 'betweenness_centrality' not in self.merged_df.columns:
            print("❌ 数据中缺少betweenness_centrality列，方法2无法使用")
            return set()
        
        print("正在计算结构洞异常分数...")
        
        # 直接从CSV中获取数据，无需重新计算
        anomaly_scores = []
        max_popularity = self.merged_df['avg_popularity'].max()
        
        for _, row in self.merged_df.iterrows():
            user_id = row['user_id']
            popularity = row['avg_popularity']
            betweenness = row['betweenness_centrality']
            
            # 计算异常分数：高影响力但低介数中心性
            if popularity > 0 and betweenness >= 0:
                popularity_norm = popularity / max_popularity
                anomaly_score = popularity_norm / (betweenness + 1e-6)
            else:
                anomaly_score = 0
            
            anomaly_scores.append({
                'user_id': user_id,
                'popularity': popularity,
                'betweenness': betweenness,
                'anomaly_score': anomaly_score
            })
        
        anomaly_df = pd.DataFrame(anomaly_scores)
        anomaly_df = anomaly_df.sort_values('anomaly_score', ascending=False)
        
        n_to_exclude = int(np.ceil(len(anomaly_df) * exclude_pct / 100))
        abnormal_users = set(anomaly_df.head(n_to_exclude)['user_id'])
        
        print(f"检测到 {len(abnormal_users)} 个结构洞异常用户")
        
        # 显示前5个异常用户示例
        if len(abnormal_users) > 0:
            print("前5个结构洞异常用户示例:")
            top_5 = anomaly_df.head(5)
            for idx, (_, row) in enumerate(top_5.iterrows()):
                print(f"  {idx+1}. 用户ID: {row['user_id']}, 异常分数: {row['anomaly_score']:.2f}, "
                      f"影响力: {row['popularity']:.2f}, 介数中心性: {row['betweenness']:.6f}")
        
        return abnormal_users
    
    def method3_neighbor_quality_anomaly(self, exclude_pct):
        """方法3: 邻居质量异常检测 - 修正版，确保精确排除比例"""
        print(f"\n=== 方法3: 邻居质量异常检测 (排除前{exclude_pct}%) ===")
        
        if exclude_pct == 0:
            print("原始网络，无需检测异常用户")
            return set()
        
        print("正在计算邻居质量异常分数...")
        anomaly_scores = []
        processed_count = 0
        
        for _, row in self.merged_df.iterrows():
            user_id = row['user_id']
            popularity = row['avg_popularity']
            
            processed_count += 1
            if processed_count % 1000 == 0:
                print(f"已处理 {processed_count}/{len(self.merged_df)} 个用户...")
            
            # 获取用户的出邻居（关注的人）
            neighbors = self.user_neighbors.get(user_id, set())
            
            # 计算邻居的影响力
            if len(neighbors) > 0:
                neighbor_popularities = []
                for neighbor in neighbors:
                    neighbor_pop = self.popularity_map.get(neighbor, 0)
                    neighbor_popularities.append(neighbor_pop)
                
                avg_neighbor_popularity = np.mean(neighbor_popularities)
                
                # 异常分数：自身影响力/邻居平均影响力
                # 比值越大，说明自己影响力高但邻居影响力低，越异常
                if avg_neighbor_popularity > 0:
                    anomaly_score = popularity / avg_neighbor_popularity
                else:
                    anomaly_score = popularity  # 如果邻居影响力都是0，则异常分数就是自身影响力
            else:
                # 没有邻居的用户，异常分数设为自身影响力
                anomaly_score = popularity
            
            anomaly_scores.append({
                'user_id': user_id,
                'popularity': popularity,
                'neighbor_count': len(neighbors),
                'avg_neighbor_popularity': np.mean([self.popularity_map.get(n, 0) for n in neighbors]) if neighbors else 0,
                'anomaly_score': anomaly_score
            })
        
        # 转换为DataFrame并排序
        anomaly_df = pd.DataFrame(anomaly_scores)
        anomaly_df = anomaly_df.sort_values('anomaly_score', ascending=False)
        
        # 按照指定比例排除用户 - 基于总用户数计算
        n_to_exclude = int(np.ceil(len(self.merged_df) * exclude_pct / 100))
        abnormal_users = set(anomaly_df.head(n_to_exclude)['user_id'])
        
        actual_exclude_pct = len(abnormal_users) / len(self.merged_df) * 100
        
        print(f"检测到 {len(abnormal_users)} 个邻居质量异常用户")
        print(f"实际排除比例: {actual_exclude_pct:.2f}%")
        
        # 显示前5个异常用户示例
        if len(abnormal_users) > 0:
            print("前5个邻居质量异常用户示例:")
            top_5 = anomaly_df.head(5)
            for idx, (_, row) in enumerate(top_5.iterrows()):
                print(f"  {idx+1}. 用户ID: {row['user_id']}, 异常分数: {row['anomaly_score']:.2f}, "
                    f"影响力: {row['popularity']:.2f}, 邻居数: {row['neighbor_count']}, "
                    f"邻居平均影响力: {row['avg_neighbor_popularity']:.2f}")
        
        return abnormal_users
    
    def method4_celebrity_removal(self):
        """🔥 新增方法4: 明星用户移除检测 - 直接移除所有明星用户"""
        print(f"\n=== 方法4: 明星用户移除检测 ===")
        
        # 检查是否有明星用户标识列
        if 'is_celebrity' not in self.merged_df.columns:
            print("❌ 数据中缺少is_celebrity列，方法4无法使用")
            return set()
        
        print("正在识别明星用户...")
        
        # 找出所有明星用户
        celebrity_users = set(self.merged_df[self.merged_df['is_celebrity'] == True]['user_id'])
        
        actual_exclude_pct = len(celebrity_users) / len(self.merged_df) * 100
        
        print(f"检测到 {len(celebrity_users)} 个明星用户")
        print(f"实际排除比例: {actual_exclude_pct:.2f}%")
        
        # 显示前5个明星用户示例
        if len(celebrity_users) > 0:
            celebrity_df = self.merged_df[self.merged_df['is_celebrity'] == True].sort_values('avg_popularity', ascending=False)
            print("前5个明星用户示例（按影响力排序）:")
            top_5 = celebrity_df.head(5)
            for idx, (_, row) in enumerate(top_5.iterrows()):
                # 获取用户的度数信息
                out_degree = row.get('global_out_degree', 0)
                in_degree = row.get('global_in_degree', 0)
                print(f"  {idx+1}. 用户ID: {row['user_id']}, 影响力: {row['avg_popularity']:.2f}, "
                      f"出度: {out_degree}, 入度: {in_degree}")
        
        return celebrity_users
    
    def detect_anomalies_batch(self, methods, exclude_percentages):
        """批量检测多个比例下的异常用户"""
        all_results = {}
        
        for exclude_pct in exclude_percentages:
            print(f"\n{'='*80}")
            print(f"开始处理排除比例: {exclude_pct}%")
            print(f"{'='*80}")
            
            all_abnormal_users = set()
            method_results = {}
            
            if 1 in methods:
                method1_users = self.method1_influence_edge_ratio(exclude_pct)
                all_abnormal_users.update(method1_users)
                method_results['method1'] = method1_users
            
            if 2 in methods:
                method2_users = self.method2_structural_hole_anomaly(exclude_pct)
                all_abnormal_users.update(method2_users)
                method_results['method2'] = method2_users
            
            if 3 in methods:
                method3_users = self.method3_neighbor_quality_anomaly(exclude_pct)
                all_abnormal_users.update(method3_users)
                method_results['method3'] = method3_users
            
            # 🔥 新增：方法4的处理
            if 4 in methods:
                method4_users = self.method4_celebrity_removal()
                all_abnormal_users.update(method4_users)
                method_results['method4'] = method4_users
            
            all_results[exclude_pct] = {
                'all_abnormal_users': all_abnormal_users,
                'method_results': method_results
            }
            
            print(f"\n排除比例 {exclude_pct}% 处理完成，共检测到 {len(all_abnormal_users)} 个异常用户")
        
        return all_results

def interactive_detection():
    """交互式异常检测"""
    print("=== 高级异常用户检测系统（批量模式）===")
    print("\n可用的检测方法：")
    print("1. 影响力/连边数比值异常检测")
    print("2. 结构洞异常检测（高影响力但低介数中心性）")
    print("3. 邻居质量异常检测（高影响力但邻居质量低）")
    print("4. 明星用户移除检测（直接移除所有明星用户）🔥新增")
    
    # 选择方法
    while True:
        try:
            method_input = input("\n请选择要使用的方法（用逗号分隔，如1,2,3,4）: ").strip()
            methods = [int(x.strip()) for x in method_input.split(',')]
            if all(m in [1, 2, 3, 4] for m in methods):
                break
            else:
                print("请输入有效的方法编号（1-4）")
        except ValueError:
            print("请输入有效的数字")
    
    # 🔥 新增：如果选择了方法4，提醒用户其特殊性
    if 4 in methods:
        print(f"\n⚠️ 注意：方法4（明星用户移除）与其他方法不同：")
        print(f"   - 不依赖排除比例，直接移除所有明星用户")
        print(f"   - 将在每个排除比例下都执行相同的明星用户移除")
        confirm = input("确认要包含方法4吗？(y/n): ").strip().lower()
        if confirm != 'y':
            methods.remove(4)
            print(f"✅ 已移除方法4，当前选择：{methods}")
    
    # 选择排除比例
    while True:
        try:
            percentages_input = input("请输入要测试的排除百分比（用逗号分隔，如0,1,3,5,10，其中0表示原始网络）: ").strip()
            exclude_percentages = [float(x.strip()) for x in percentages_input.split(',')]
            if all(0 <= p <= 50 for p in exclude_percentages):
                # 去重并排序
                exclude_percentages = sorted(list(set(exclude_percentages)))
                break
            else:
                print("请输入0-50之间的百分比")
        except ValueError:
            print("请输入有效的数字")
    
    return methods, exclude_percentages

def save_batch_results(detector, all_results, methods, output_base_dir):
    """保存批量检测结果"""
    method_names = '_'.join([f"method{m}" for m in methods])
    
    # 为每个排除比例创建文件夹并保存结果
    for exclude_pct, results in all_results.items():
        if exclude_pct == 0:
            # 原始网络特殊处理
            output_dir = f'{output_base_dir}/original_network_0pct'
        else:
            output_dir = f'{output_base_dir}/advanced_{method_names}_{exclude_pct}pct'
        
        ensure_dir(output_dir)
        
        all_abnormal_users = results['all_abnormal_users']
        method_results = results['method_results']
        
        # 保存简单的异常用户列表
        if exclude_pct == 0:
            # 原始网络：创建空的DataFrame
            abnormal_df = pd.DataFrame(columns=['user_id', 'detection_method', 'exclude_percentage'])
        else:
            abnormal_df = pd.DataFrame({
                'user_id': list(all_abnormal_users),
                'detection_method': f"methods_{method_names}",
                'exclude_percentage': exclude_pct
            })
        
        # 🔥 修改：添加详细信息，包括明星用户标识
        detailed_info = []
        if exclude_pct == 0:
            # 原始网络：空详细信息
            detailed_df = pd.DataFrame(columns=['user_id', 'avg_popularity', 'edge_count', 
                                               'detected_by_method1', 'detected_by_method2', 
                                               'detected_by_method3', 'detected_by_method4', 'is_celebrity'])
        else:
            # 计算edge_count用于详细信息
            user_out_edges = detector.edges_df['source'].value_counts().to_dict()
            user_in_edges = detector.edges_df['target'].value_counts().to_dict()
            
            for user_id in all_abnormal_users:
                user_info = detector.merged_df[detector.merged_df['user_id'] == user_id].iloc[0]
                out_count = user_out_edges.get(user_id, 0)
                in_count = user_in_edges.get(user_id, 0)
                total_edges = out_count + in_count
                
                detailed_info.append({
                    'user_id': user_id,
                    'avg_popularity': user_info['avg_popularity'],
                    'edge_count': total_edges,
                    'detected_by_method1': user_id in method_results.get('method1', set()),
                    'detected_by_method2': user_id in method_results.get('method2', set()),
                    'detected_by_method3': user_id in method_results.get('method3', set()),
                    'detected_by_method4': user_id in method_results.get('method4', set()),  # 🔥新增
                    'is_celebrity': user_info.get('is_celebrity', False)  # 🔥新增
                })
            detailed_df = pd.DataFrame(detailed_info)
        
        # 保存文件
        abnormal_df.to_csv(f'{output_dir}/abnormal_users.csv', index=False)
        detailed_df.to_csv(f'{output_dir}/abnormal_users_detailed.csv', index=False)
        
        # 🔥 修改：生成报告，包含方法4信息
        with open(f'{output_dir}/detection_report.txt', 'w', encoding='utf-8') as f:
            if exclude_pct == 0:
                f.write("=== 原始网络分析报告 ===\n\n")
                f.write(f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"网络类型: 原始网络（未排除任何用户）\n")
                f.write(f"排除比例: {exclude_pct}%\n\n")
            else:
                f.write("=== 高级异常用户检测报告 ===\n\n")
                f.write(f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"使用方法: {methods}\n")
                f.write(f"排除比例: {exclude_pct}%\n\n")
            
            f.write(f"总用户数: {len(detector.merged_df)}\n")
            f.write(f"检测到的异常用户总数: {len(all_abnormal_users)}\n")
            f.write(f"实际排除比例: {len(all_abnormal_users)/len(detector.merged_df)*100:.2f}%\n\n")
            
            if exclude_pct > 0:
                for method_name, users in method_results.items():
                    method_descriptions = {
                        'method1': '影响力/连边数比值异常',
                        'method2': '结构洞异常（高影响力低介数中心性）',
                        'method3': '邻居质量异常（高影响力低邻居质量）',
                        'method4': '明星用户移除'  # 🔥新增
                    }
                    desc = method_descriptions.get(method_name, method_name)
                    f.write(f"{method_name} ({desc}) 检测到: {len(users)} 个用户\n")
                
                # 方法重叠分析
                if len(method_results) > 1:
                    f.write(f"\n=== 方法重叠分析 ===\n")
                    method_sets = list(method_results.values())
                    if len(method_sets) >= 2:
                        intersection = set.intersection(*method_sets)
                        f.write(f"所有方法共同检测到: {len(intersection)} 个用户\n")
                        
                        # 🔥 新增：特别分析方法4的重叠情况
                        if 'method4' in method_results:
                            method4_users = method_results['method4']
                            other_methods = {k: v for k, v in method_results.items() if k != 'method4'}
                            if other_methods:
                                other_union = set.union(*other_methods.values()) if other_methods else set()
                                overlap_with_others = method4_users.intersection(other_union)
                                f.write(f"明星用户与其他方法重叠: {len(overlap_with_others)} 个用户\n")
                                f.write(f"仅被方法4检测到的明星用户: {len(method4_users - other_union)} 个用户\n")
        
        print(f"  - 排除比例 {exclude_pct}% 结果已保存到: {output_dir}")

def main():
    """主函数"""
    # 交互式选择
    methods, exclude_percentages = interactive_detection()
    
    print(f"\n将要执行的配置:")
    print(f"检测方法: {methods}")
    print(f"排除比例: {exclude_percentages}")
    print(f"总共需要处理 {len(exclude_percentages)} 种情况")
    
    # 🔥 新增：特别提醒方法4的特殊性
    if 4 in methods:
        print(f"\n⚠️ 特别提醒：方法4（明星用户移除）将在每个比例下执行相同操作")
    
    confirm = input("\n确认开始批量检测？(y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消检测")
        return
    
    # 初始化检测器
    detector = AdvancedAnomalyDetector()
    if not detector.load_data():
        return
    
    # 执行批量检测
    print(f"\n开始执行批量异常检测...")
    start_time = datetime.now()
    
    all_results = detector.detect_anomalies_batch(methods, exclude_percentages)
    
    # 创建输出目录
    output_base_dir = 'results/pick_out_abnormal_users'
    ensure_dir(output_base_dir)
    
    # 保存所有结果
    print(f"\n开始保存批量检测结果...")
    save_batch_results(detector, all_results, methods, output_base_dir)
    
    # 🔥 修改：生成批量汇总报告，包含方法4信息
    summary_path = f'{output_base_dir}/batch_detection_summary.txt'
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("====== 批量异常用户检测汇总报告 ======\n")
        f.write(f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"使用方法: {methods}\n")
        f.write(f"测试比例: {exclude_percentages}\n\n")
        
        # 🔥 新增：方法说明
        f.write("=== 检测方法说明 ===\n")
        method_descriptions = {
            1: "影响力/连边数比值异常检测",
            2: "结构洞异常检测（高影响力但低介数中心性）",
            3: "邻居质量异常检测（高影响力但邻居质量低）",
            4: "明星用户移除检测（直接移除所有明星用户）"
        }
        for method_num in methods:
            f.write(f"方法{method_num}: {method_descriptions[method_num]}\n")
        f.write("\n")
        
        f.write("=== 各比例检测结果汇总 ===\n")
        f.write(f"{'排除比例':<10} {'异常用户数':<12} {'实际排除比例':<15} {'状态'}\n")
        f.write("-" * 50 + "\n")
        
        for exclude_pct in exclude_percentages:
            results = all_results[exclude_pct]
            abnormal_count = len(results['all_abnormal_users'])
            actual_pct = abnormal_count / len(detector.merged_df) * 100
            status = "原始网络" if exclude_pct == 0 else "已处理"
            
            f.write(f"{exclude_pct}%{'':<7} {abnormal_count:<12} {actual_pct:<15.2f}% {status}\n")
        
        f.write(f"\n=== 处理统计 ===\n")
        f.write(f"总用户数: {len(detector.merged_df)}\n")
        f.write(f"处理的比例数: {len(exclude_percentages)}\n")
        f.write(f"使用的检测方法数: {len(methods)}\n")
        
        # 🔥 新增：明星用户统计
        if 4 in methods and 'is_celebrity' in detector.merged_df.columns:
            celebrity_count = detector.merged_df['is_celebrity'].sum()
            f.write(f"明星用户总数: {celebrity_count}\n")
            f.write(f"明星用户比例: {celebrity_count/len(detector.merged_df)*100:.2f}%\n")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*80}")
    print(f"批量检测完成！")
    print(f"总耗时: {duration}")
    print(f"处理了 {len(exclude_percentages)} 种排除比例: {exclude_percentages}")
    print(f"结果保存在: {output_base_dir}")
    print(f"批量汇总报告: {summary_path}")
    
    # 打印简要结果
    print(f"\n=== 批量检测结果预览 ===")
    for exclude_pct in exclude_percentages:
        results = all_results[exclude_pct]
        abnormal_count = len(results['all_abnormal_users'])
        actual_pct = abnormal_count / len(detector.merged_df) * 100
        status = "（原始网络）" if exclude_pct == 0 else ""
        print(f"{exclude_pct}% 排除: {abnormal_count} 个异常用户 ({actual_pct:.2f}%) {status}")

if __name__ == "__main__":
    main()