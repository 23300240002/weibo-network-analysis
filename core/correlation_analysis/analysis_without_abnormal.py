import os
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import re

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def normalize_id(id_value):
    """规范化用户ID，确保格式一致"""
    try:
        id_str = str(id_value).strip()
        if id_str == '-2147483648':
            return id_str
        return str(int(float(id_str)))
    except:
        return str(id_value).strip()

def detect_abnormal_user_folders():
    """自动检测所有异常用户文件夹"""
    base_dir = 'results/pick_out_abnormal_users'
    
    if not os.path.exists(base_dir):
        print(f"错误: 异常用户目录不存在 {base_dir}")
        return []
    
    folders = []
    for item in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, item)
        if os.path.isdir(folder_path):
            csv_file = os.path.join(folder_path, 'abnormal_users.csv')
            if os.path.exists(csv_file):
                folders.append(item)
    
    # 按文件夹名称排序，确保原始网络排在前面
    folders.sort(key=lambda x: (0 if 'original' in x.lower() else 1, x))
    
    print(f"检测到 {len(folders)} 个异常用户文件夹:")
    for folder in folders:
        print(f"  - {folder}")
    
    return folders

def parse_folder_info(folder_name):
    """解析文件夹名称，提取方法和排除比例信息"""
    if 'original_network' in folder_name.lower():
        return {
            'exclude_pct': 0.0,
            'methods': ['original'],
            'description': '原始网络',
            'short_name': 'Original',
            'folder_name': folder_name
        }
    
    # 解析advanced_method1_2_3_10.0pct格式
    match = re.search(r'advanced_(.+?)_(\d+(?:\.\d+)?)pct', folder_name)
    if match:
        methods_part = match.group(1)
        exclude_pct = float(match.group(2))
        
        # 提取方法编号
        methods = re.findall(r'method(\d+)', methods_part)
        method_names = [f"method{m}" for m in methods]
        
        # 创建简短清晰的名称
        short_name = f"排除{exclude_pct}%"
        
        return {
            'exclude_pct': exclude_pct,
            'methods': method_names,
            'description': f'排除前{exclude_pct}%异常用户（方法: {", ".join(method_names)}）',
            'short_name': short_name,
            'folder_name': folder_name
        }
    
    # 如果解析失败，返回默认值
    return {
        'exclude_pct': -1,
        'methods': ['unknown'],
        'description': f'未知配置（{folder_name}）',
        'short_name': folder_name[:10],
        'folder_name': folder_name
    }

def load_abnormal_users_from_folder(folder_name):
    """从指定文件夹加载异常用户列表"""
    abnormal_file = f'results/pick_out_abnormal_users/{folder_name}/abnormal_users.csv'
    
    if not os.path.exists(abnormal_file):
        print(f"警告: 未找到文件 {abnormal_file}")
        return set()
    
    try:
        abnormal_df = pd.read_csv(abnormal_file)
        if len(abnormal_df) == 0:
            print(f"  - 加载了 0 个异常用户（{folder_name}）")
            return set()
        
        abnormal_users = set(abnormal_df['user_id'].apply(normalize_id))
        print(f"  - 加载了 {len(abnormal_users)} 个异常用户（{folder_name}）")
        return abnormal_users
    except Exception as e:
        print(f"加载异常用户文件出错 {abnormal_file}: {e}")
        return set()

def detect_network_features(merged_df):
    """🔥 新增：自动检测数据中的网络特征，排除非分析字段"""
    # 需要排除的字段
    excluded_columns = {
        'user_id',           # 用户ID
        'center_node',       # 中心节点（与user_id重复）
        'avg_popularity',    # 影响力（因变量）
        'is_celebrity'       # 明星用户标识（非网络指标）
    }
    
    # 🔥 自动检测所有可分析的网络特征
    network_features = []
    for col in merged_df.columns:
        if col not in excluded_columns:
            # 检查是否为数值型
            if pd.api.types.is_numeric_dtype(merged_df[col]):
                network_features.append(col)
    
    # 按照重要性排序（优先显示传统的6大网络指标）
    priority_order = [
        'density', 'clustering_coefficient', 'average_nearest_neighbor_degree',
        'betweenness_centrality', 'spectral_radius', 'modularity',
        'global_out_degree', 'global_in_degree', 'global_total_degree',
        'node_count', 'edge_count'
    ]
    
    # 重新排序：优先级特征在前，其他特征在后
    ordered_features = []
    for feature in priority_order:
        if feature in network_features:
            ordered_features.append(feature)
            
    # 添加其他未在优先级列表中的特征
    for feature in network_features:
        if feature not in ordered_features:
            ordered_features.append(feature)
    
    print(f"\n🔍 自动检测到 {len(ordered_features)} 个可分析的网络特征:")
    
    # 🔥 新增：按类别显示特征
    traditional_features = [f for f in ordered_features if f in priority_order[:6]]
    degree_features = [f for f in ordered_features if f in priority_order[6:9]]
    network_size_features = [f for f in ordered_features if f in priority_order[9:11]]
    other_features = [f for f in ordered_features if f not in priority_order]
    
    if traditional_features:
        print(f"  📊 传统网络指标 ({len(traditional_features)}个): {', '.join(traditional_features)}")
    if degree_features:
        print(f"  🔗 度数指标 ({len(degree_features)}个): {', '.join(degree_features)}")
    if network_size_features:
        print(f"  📏 网络规模指标 ({len(network_size_features)}个): {', '.join(network_size_features)}")
    if other_features:
        print(f"  ➕ 其他指标 ({len(other_features)}个): {', '.join(other_features)}")
    
    return ordered_features

def calculate_correlations_without_abnormal(merged_df, abnormal_users, folder_info):
    """🔥 修改版：计算排除异常用户后的相关性，自动检测所有网络特征"""
    merged_df['user_id'] = merged_df['user_id'].apply(normalize_id)
    filtered_df = merged_df[~merged_df['user_id'].isin(abnormal_users)].copy()
    
    print(f"  - 原始用户数: {len(merged_df)}")
    print(f"  - 排除异常用户数: {len(abnormal_users)}")
    print(f"  - 剩余正常用户数: {len(filtered_df)}")
    
    if len(filtered_df) < 10:
        print(f"  - 警告: 剩余用户数过少 ({len(filtered_df)})，可能影响相关性分析的可靠性")
    
    # 🔥 关键修改：自动检测网络特征
    network_features = detect_network_features(filtered_df)
    
    if not network_features:
        print(f"  - 错误: 未检测到任何可分析的网络特征")
        return {}, len(merged_df), len(abnormal_users), len(filtered_df)
    
    # 计算相关性
    correlations = {}
    
    for feature in network_features:
        if feature not in filtered_df.columns:
            print(f"  - 警告: 特征 {feature} 不在数据中")
            correlations[feature] = {
                'spearman_corr': np.nan,
                'spearman_p': np.nan,
                'kendall_corr': np.nan,
                'kendall_p': np.nan
            }
            continue
        
        try:
            # 检查是否有有效数据
            valid_mask = (~pd.isna(filtered_df[feature])) & (~pd.isna(filtered_df['avg_popularity']))
            valid_feature = filtered_df.loc[valid_mask, feature]
            valid_popularity = filtered_df.loc[valid_mask, 'avg_popularity']
            
            if len(valid_feature) < 3:
                print(f"  - 警告: 特征 {feature} 有效数据点过少 ({len(valid_feature)})")
                correlations[feature] = {
                    'spearman_corr': np.nan,
                    'spearman_p': np.nan,
                    'kendall_corr': np.nan,
                    'kendall_p': np.nan
                }
                continue
            
            # 计算Spearman相关系数
            spearman_corr, spearman_p = stats.spearmanr(valid_feature, valid_popularity)
            
            # 计算Kendall相关系数
            kendall_corr, kendall_p = stats.kendalltau(valid_feature, valid_popularity)
            
            correlations[feature] = {
                'spearman_corr': spearman_corr,
                'spearman_p': spearman_p,
                'kendall_corr': kendall_corr,
                'kendall_p': kendall_p
            }
            
            print(f"  - {feature}: Spearman={spearman_corr:.4f}(p={spearman_p:.4f}), Kendall={kendall_corr:.4f}(p={kendall_p:.4f})")
            
        except Exception as e:
            print(f"  - 计算 {feature} 相关性时出错: {e}")
            correlations[feature] = {
                'spearman_corr': np.nan,
                'spearman_p': np.nan,
                'kendall_corr': np.nan,
                'kendall_p': np.nan
            }
    
    return correlations, len(merged_df), len(abnormal_users), len(filtered_df)

def save_results(correlations, original_count, excluded_count, remaining_count, 
                folder_info, output_dir):
    """🔥 修改版：保存分析结果，支持动态特征数量"""
    result_dir = os.path.join(output_dir, folder_info['folder_name'])
    ensure_dir(result_dir)
    
    # 保存详细的相关性结果到CSV
    results_data = []
    for feature, corr_data in correlations.items():
        results_data.append({
            'feature': feature,
            'spearman_correlation': corr_data['spearman_corr'],
            'spearman_p_value': corr_data['spearman_p'],
            'kendall_correlation': corr_data['kendall_corr'],
            'kendall_p_value': corr_data['kendall_p']
        })
    
    results_df = pd.DataFrame(results_data)
    csv_path = os.path.join(result_dir, 'correlation_results.csv')
    results_df.to_csv(csv_path, index=False)
    
    # 保存汇总报告到TXT
    txt_path = os.path.join(result_dir, 'analysis_summary.txt')
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(f"====== {folder_info['description']} 相关性分析结果 ======\n")
        f.write(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"检测方法: {', '.join(folder_info['methods'])}\n")
        f.write(f"排除比例: {folder_info['exclude_pct']}%\n\n")
        
        f.write(f"=== 数据统计 ===\n")
        f.write(f"原始用户总数: {original_count}\n")
        f.write(f"排除异常用户数: {excluded_count}\n")
        f.write(f"剩余正常用户数: {remaining_count}\n")
        f.write(f"排除比例: {excluded_count/original_count*100:.2f}%\n")
        f.write(f"保留比例: {remaining_count/original_count*100:.2f}%\n\n")
        
        # 🔥 修改：动态标题，支持任意数量的特征
        f.write(f"=== 网络特征与影响力相关性分析 (共{len(correlations)}个特征) ===\n")
        f.write(f"{'特征名称':<35} {'Spearman相关系数':<18} {'Spearman P值':<15} {'Kendall相关系数':<17} {'Kendall P值':<15} {'显著性'}\n")
        f.write("-" * 120 + "\n")
        
        for feature, corr_data in correlations.items():
            spearman_sig = "显著" if not pd.isna(corr_data['spearman_p']) and corr_data['spearman_p'] < 0.05 else "不显著"
            kendall_sig = "显著" if not pd.isna(corr_data['kendall_p']) and corr_data['kendall_p'] < 0.05 else "不显著"
            
            # 综合显著性判断
            overall_sig = "显著" if (spearman_sig == "显著" or kendall_sig == "显著") else "不显著"
            
            f.write(f"{feature:<35} "
                   f"{corr_data['spearman_corr']:<18.4f} "
                   f"{corr_data['spearman_p']:<15.4f} "
                   f"{corr_data['kendall_corr']:<17.4f} "
                   f"{corr_data['kendall_p']:<15.4f} "
                   f"{overall_sig}\n")
        
        f.write(f"\n=== 分析说明 ===\n")
        f.write(f"1. 使用检测方法: {', '.join(folder_info['methods'])}\n")
        f.write(f"2. 排除比例: {folder_info['exclude_pct']}%\n")
        f.write(f"3. 自动检测到 {len(correlations)} 个网络特征进行分析\n")
        f.write(f"4. Spearman相关系数衡量单调关系，Kendall相关系数衡量序列一致性\n")
        f.write(f"5. P值<0.05认为相关性显著\n")
        f.write(f"6. 相关系数绝对值越大，表示相关性越强\n")
        f.write(f"7. 已自动排除非分析字段: user_id, center_node, avg_popularity, is_celebrity\n")
    
    print(f"  - 结果已保存到: {result_dir}")
    return csv_path, txt_path

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"开始自适应异常用户相关性分析...")
    print(f"分析时间: {start_time}")
    
    # 🔥 修改：使用新的数据路径
    merged_data_path = 'C:/Tengfei/data/results/user_3855570307_metrics/merged_metrics_popularity.csv'
    
    if not os.path.exists(merged_data_path):
        print(f"错误: 未找到合并数据文件 {merged_data_path}")
        print("请先运行 create3.py 生成 merged_metrics_popularity.csv")
        return
    
    print(f"正在加载合并数据: {merged_data_path}")
    try:
        merged_df = pd.read_csv(merged_data_path)
        print(f"成功加载数据，包含 {len(merged_df)} 个用户")
    except Exception as e:
        print(f"加载合并数据出错: {e}")
        return
    
    # 🔥 修改：检查必要的列（移除硬编码的网络特征检查）
    required_columns = ['user_id', 'avg_popularity']
    
    missing_columns = [col for col in required_columns if col not in merged_df.columns]
    if missing_columns:
        print(f"错误: 合并数据缺少必要的列: {missing_columns}")
        return
    
    print(f"✅ 数据格式验证通过")
    print(f"📊 数据包含列: {list(merged_df.columns)}")
    
    # 自动检测异常用户文件夹
    print(f"\n{'='*60}")
    print(f"自动检测异常用户文件夹...")
    abnormal_folders = detect_abnormal_user_folders()
    
    if not abnormal_folders:
        print("错误: 未找到任何异常用户文件夹")
        print("请先运行 pick_out_abnormal_users.py 生成异常用户数据")
        return
    
    # 创建输出目录
    output_dir = 'results/correlation_result'
    ensure_dir(output_dir)
    
    print(f"\n{'='*60}")
    print(f"开始分析 {len(abnormal_folders)} 种配置下的相关性...")
    
    all_results = {}
    
    # 分析每个配置
    for folder_name in abnormal_folders:
        print(f"\n{'='*60}")
        
        # 解析文件夹信息
        folder_info = parse_folder_info(folder_name)
        print(f"分析配置: {folder_info['description']}")
        print(f"文件夹: {folder_name}")
        print(f"{'='*60}")
        
        # 加载异常用户列表
        abnormal_users = load_abnormal_users_from_folder(folder_name)
        
        # 计算相关性
        print(f"  - 开始计算相关性...")
        correlations, original_count, excluded_count, remaining_count = calculate_correlations_without_abnormal(
            merged_df, abnormal_users, folder_info)
        
        # 保存结果
        csv_path, txt_path = save_results(correlations, original_count, excluded_count, 
                                        remaining_count, folder_info, output_dir)
        
        # 存储结果用于后续比较
        all_results[folder_name] = {
            'folder_info': folder_info,
            'correlations': correlations,
            'original_count': original_count,
            'excluded_count': excluded_count,
            'remaining_count': remaining_count
        }
    
    # 🔥 修改：生成动态对比汇总报告
    print(f"\n{'='*60}")
    print(f"生成对比汇总报告...")
    
    summary_path = os.path.join(output_dir, 'comprehensive_comparison_summary.txt')
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("====== 异常用户筛选相关性分析综合对比汇总 ======\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 配置概览
        f.write("=== 分析配置概览 ===\n")
        for i, folder_name in enumerate(abnormal_folders, 1):
            folder_info = all_results[folder_name]['folder_info']
            f.write(f"{i}. {folder_info['short_name']}: {folder_info['description']}\n")
        f.write("\n")
        
        # 数据量对比
        f.write("=== 数据量对比 ===\n")
        f.write(f"{'配置':<15} {'原始用户数':<12} {'排除用户数':<12} {'剩余用户数':<12} {'排除比例':<10} {'保留比例'}\n")
        f.write("-" * 75 + "\n")
        for folder_name in abnormal_folders:
            result = all_results[folder_name]
            folder_info = result['folder_info']
            exclude_pct = result['excluded_count'] / result['original_count'] * 100
            remain_pct = result['remaining_count'] / result['original_count'] * 100
            
            f.write(f"{folder_info['short_name']:<15} {result['original_count']:<12} {result['excluded_count']:<12} "
                   f"{result['remaining_count']:<12} {exclude_pct:<10.2f}% {remain_pct:.2f}%\n")
        
        # 🔥 修改：动态获取所有特征进行对比
        all_features = set()
        for folder_name in abnormal_folders:
            all_features.update(all_results[folder_name]['correlations'].keys())
        
        features = sorted(list(all_features))  # 排序保证一致性
        
        f.write(f"\n=== Spearman相关系数对比 (共{len(features)}个特征) ===\n")
        # 构建列标题
        header = f"{'特征':<35} "
        for folder_name in abnormal_folders:
            folder_info = all_results[folder_name]['folder_info']
            header += f"{folder_info['short_name']:<15} "
        f.write(header + "\n")
        f.write("-" * (35 + 15 * len(abnormal_folders)) + "\n")
        
        for feature in features:
            line = f"{feature:<35} "
            for folder_name in abnormal_folders:
                if feature in all_results[folder_name]['correlations']:
                    corr = all_results[folder_name]['correlations'][feature]['spearman_corr']
                    if pd.isna(corr):
                        line += f"{'N/A':<15}"
                    else:
                        line += f"{corr:<15.4f}"
                else:
                    line += f"{'N/A':<15}"
            f.write(line + "\n")
        
        f.write(f"\n=== Kendall相关系数对比 (共{len(features)}个特征) ===\n")
        # 构建列标题
        header = f"{'特征':<35} "
        for folder_name in abnormal_folders:
            folder_info = all_results[folder_name]['folder_info']
            header += f"{folder_info['short_name']:<15} "
        f.write(header + "\n")
        f.write("-" * (35 + 15 * len(abnormal_folders)) + "\n")
        
        for feature in features:
            line = f"{feature:<35} "
            for folder_name in abnormal_folders:
                if feature in all_results[folder_name]['correlations']:
                    corr = all_results[folder_name]['correlations'][feature]['kendall_corr']
                    if pd.isna(corr):
                        line += f"{'N/A':<15}"
                    else:
                        line += f"{corr:<15.4f}"
                else:
                    line += f"{'N/A':<15}"
            f.write(line + "\n")
        
        f.write(f"\n=== 分析说明 ===\n")
        f.write(f"1. 自动检测并分析了 {len(features)} 个网络特征\n")
        f.write(f"2. 已排除非分析字段: user_id, center_node, avg_popularity, is_celebrity\n")
        f.write(f"3. Original: 原始网络，未排除任何用户\n")
        for folder_name in abnormal_folders:
            folder_info = all_results[folder_name]['folder_info']
            if folder_info['exclude_pct'] > 0:
                f.write(f"4. {folder_info['short_name']}: 排除前{folder_info['exclude_pct']}%异常用户\n")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*60}")
    print(f"分析完成！")
    print(f"总耗时: {duration}")
    print(f"处理了 {len(abnormal_folders)} 种配置")
    print(f"结果保存在: {output_dir}")
    print(f"综合对比汇总报告: {summary_path}")
    
    # 打印简要结果
    print(f"\n=== 分析结果预览 ===")
    for folder_name in abnormal_folders:
        result = all_results[folder_name]
        folder_info = result['folder_info']
        print(f"{folder_info['short_name']}: 排除{result['excluded_count']}用户, 剩余{result['remaining_count']}用户")

if __name__ == "__main__":
    main()