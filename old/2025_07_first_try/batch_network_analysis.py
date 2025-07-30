"""
批量网络分析脚本 - 评估data/all目录下所有CSV文件的网络质量
"""

import os
import time
import csv
from datetime import datetime
import networkx as nx
import pandas as pd
from network_analyzer import (
    build_social_network, 
    analyze_network, 
    get_largest_component, 
    plot_degree_distribution,
    print_metrics,
    evaluate_network
)

def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def analyze_csv_network(csv_path, output_dir, max_rows=None):
    """分析单个CSV文件中的网络并输出结果"""
    # 获取CSV文件名（不含路径和扩展名）
    csv_filename = os.path.basename(csv_path).split('.')[0]
    
    print(f"\n{'='*50}")
    print(f"开始分析网络: {csv_filename}")
    print(f"{'='*50}")
    
    # 创建输出目录
    file_output_dir = os.path.join(output_dir, csv_filename)
    ensure_dir(file_output_dir)
    
    try:
        # 构建网络
        start_time = time.time()
        G = build_social_network(csv_path, max_rows=max_rows)
        build_time = time.time() - start_time
        
        # 检查网络是否为空
        if len(G.nodes) == 0:
            print(f"警告: {csv_filename} 生成的网络为空！")
            with open(os.path.join(output_dir, "analysis_summary.txt"), 'a', encoding='utf-8') as f:
                f.write(f"{csv_filename}: 网络为空\n")
            return
        
        # 分析整个网络
        full_metrics = analyze_network(G)
        
        # 获取并分析最大连通分量
        largest_cc = get_largest_component(G)
        largest_cc_metrics = analyze_network(largest_cc)
        
        # 生成度分布图
        plot_path = os.path.join(file_output_dir, "degree_distribution.png")
        plot_degree_distribution(G, f"Degree Distribution - {csv_filename}", plot_path)
        
        # 生成最大连通分量度分布图
        largest_cc_plot_path = os.path.join(file_output_dir, "largest_cc_degree_distribution.png")
        plot_degree_distribution(largest_cc, f"Largest Component - {csv_filename}", largest_cc_plot_path)
        
        # 保存详细结果到文本文件
        result_path = os.path.join(file_output_dir, "network_analysis.txt")
        with open(result_path, 'w', encoding='utf-8') as f:
            f.write(f"网络分析结果: {csv_filename}\n")
            f.write(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"网络构建耗时: {build_time:.2f} 秒\n\n")
            
            f.write("=== 完整网络指标 ===\n")
            for key, value in full_metrics.items():
                if isinstance(value, float):
                    f.write(f"{key}: {value:.6f}\n")
                else:
                    f.write(f"{key}: {value}\n")
            
            f.write("\n=== 最大连通分量指标 ===\n")
            for key, value in largest_cc_metrics.items():
                if isinstance(value, float):
                    f.write(f"{key}: {value:.6f}\n")
                else:
                    f.write(f"{key}: {value}\n")
            
            # 添加网络质量评估
            f.write("\n=== 网络质量评估 ===\n")
            if full_metrics["网络密度"] < 0.001:
                f.write("✗ 网络极度稀疏（密度<0.001），远低于理想的社交网络密度\n")
            elif full_metrics["网络密度"] < 0.01:
                f.write("△ 网络稀疏（密度<0.01），但在大型社交网络中较为常见\n")
            else:
                f.write("✓ 网络密度良好（密度≥0.01），适合进行网络分析\n")
            
            # 使用抽样聚类系数而非完整聚类系数
            if "抽样聚类系数" in full_metrics and isinstance(full_metrics["抽样聚类系数"], float):
                if full_metrics["抽样聚类系数"] < 0.01:
                    f.write("✗ 聚类系数极低，网络中几乎没有形成三角形结构\n")
                elif full_metrics["抽样聚类系数"] < 0.1:
                    f.write("△ 聚类系数较低，网络中局部聚集现象不明显\n")
                else:
                    f.write("✓ 聚类系数良好，网络中存在局部聚集现象\n")
            
            if full_metrics["孤立节点占比"] > 0.3:
                f.write("✗ 孤立节点占比过高，许多用户没有互动\n")
            elif full_metrics["孤立节点占比"] > 0.1:
                f.write("△ 孤立节点比例中等，部分用户缺乏互动\n")
            else:
                f.write("✓ 孤立节点比例低，用户互动活跃\n")
            
            if full_metrics["最大连通分量占比"] < 0.5:
                f.write("✗ 网络高度分散，最大连通分量占比低于50%\n")
            elif full_metrics["最大连通分量占比"] < 0.8:
                f.write("△ 网络连通性一般，最大连通分量占比在50%-80%之间\n")
            else:
                f.write("✓ 网络连通性良好，最大连通分量占比超过80%\n")
        
        # 计算评分
        score = 0
        if full_metrics["网络密度"] >= 0.01: score += 2
        elif full_metrics["网络密度"] >= 0.001: score += 1
        
        if "抽样聚类系数" in full_metrics and isinstance(full_metrics["抽样聚类系数"], float):
            if full_metrics["抽样聚类系数"] >= 0.1: score += 2
            elif full_metrics["抽样聚类系数"] >= 0.01: score += 1
        
        if full_metrics["孤立节点占比"] <= 0.1: score += 2
        elif full_metrics["孤立节点占比"] <= 0.3: score += 1
        
        if full_metrics["最大连通分量占比"] >= 0.8: score += 2
        elif full_metrics["最大连通分量占比"] >= 0.5: score += 1
        
        # 将核心指标添加到汇总文件
        with open(os.path.join(output_dir, "analysis_summary.txt"), 'a', encoding='utf-8') as f:
            f.write(f"{csv_filename}: 节点数={full_metrics['节点数']}, 边数={full_metrics['边数']}, "
                    f"密度={full_metrics['网络密度']:.6f}, 聚类系数={full_metrics.get('抽样聚类系数', 'N/A') if isinstance(full_metrics.get('抽样聚类系数'), float) else 'N/A'}, "
                    f"最大连通分量占比={full_metrics['最大连通分量占比']:.2%}, 得分={score}\n")
        
        # 保存核心指标到CSV文件，方便后续比较
        summary_csv_path = os.path.join(output_dir, "analysis_summary.csv")
        if not os.path.exists(summary_csv_path):
            with open(summary_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['文件名', '节点数', '边数', '密度', '抽样聚类系数', '最大连通分量占比', '最大连通分量节点数', '孤立节点占比', '得分'])
        
        with open(summary_csv_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                csv_filename, 
                full_metrics['节点数'], 
                full_metrics['边数'], 
                full_metrics['网络密度'], 
                full_metrics.get('抽样聚类系数', 'N/A') if isinstance(full_metrics.get('抽样聚类系数'), float) else 'N/A',
                full_metrics['最大连通分量占比'],
                largest_cc_metrics['节点数'],
                full_metrics['孤立节点占比'],
                score
            ])
        
        print(f"分析完成: {csv_filename}")
        print(f"结果已保存到: {result_path}")
        
    except Exception as e:
        print(f"分析 {csv_filename} 时出错: {e}")
        with open(os.path.join(output_dir, "analysis_errors.txt"), 'a', encoding='utf-8') as f:
            f.write(f"{csv_filename}: {str(e)}\n")

def main():
    # 设置输入和输出目录
    input_dir = "./data/all/"
    output_dir = "./results/network_analysis"
    
    # 确保输出目录存在
    ensure_dir(output_dir)
    
    # 初始化汇总文件
    with open(os.path.join(output_dir, "analysis_summary.txt"), 'w', encoding='utf-8') as f:
        f.write(f"网络分析汇总 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    # 获取输入目录中的所有CSV文件
    csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print(f"警告: 在 {input_dir} 目录中没有找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件需要分析")
    
    # 分析每个CSV文件
    for i, csv_file in enumerate(csv_files):
        csv_path = os.path.join(input_dir, csv_file)
        print(f"\n处理文件 {i+1}/{len(csv_files)}: {csv_file}")
        
        # 可选: 限制处理的行数以加快测试速度
        # max_rows = 10000  # 设置为None处理全部数据
        max_rows = None
        
        analyze_csv_network(csv_path, output_dir, max_rows)
    
    print("\n所有网络分析完成！")
    print(f"汇总结果保存在: {output_dir}/analysis_summary.txt")
    print(f"详细结果保存在各子目录中")

if __name__ == "__main__":
    main()