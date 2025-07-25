import csv
import time
import os
import matplotlib.pyplot as plt
import networkx as nx
import random
from matplotlib.font_manager import FontProperties

def build_social_network(csv_path, encoding='gb18030', max_rows=None):
    """构建社交网络"""
    print(f"开始从 {csv_path} 构建社交网络...")
    start_time = time.time()
    
    # 创建无向图
    G = nx.Graph()
    
    # 遍历CSV，直接使用'MD5-父微博用户ID'字段建立连接
    total_rows = 0
    edges_added = 0
    
    with open(csv_path, 'r', encoding=encoding, errors='ignore') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break
                
            # 获取作者ID和父微博作者ID
            author_id = row.get("MD5-作者ID", "").strip()
            parent_author_id = row.get("MD5-父微博用户ID", "").strip()
            
            # 添加节点
            if author_id:
                G.add_node(author_id)
            if parent_author_id:
                G.add_node(parent_author_id)
            
            # 添加边（排除自环）
            if author_id and parent_author_id and author_id != parent_author_id:
                G.add_edge(author_id, parent_author_id)
                edges_added += 1
            
            total_rows += 1
            
            # 显示进度
            if i % 50000 == 0 and i > 0:
                print(f"  已处理 {i} 行...")
    
    elapsed_time = time.time() - start_time
    print(f"网络构建完成！处理了 {total_rows} 行数据，耗时 {elapsed_time:.2f} 秒")
    
    return G

def analyze_network(G, sample_size=1000):
    """分析网络的基本指标，使用抽样计算聚类系数"""
    print("计算网络基本指标...")
    metrics = {}
    
    # 基本指标
    metrics["节点数"] = len(G.nodes)
    metrics["边数"] = len(G.edges)
    metrics["平均度数"] = sum(dict(G.degree()).values()) / len(G.nodes) if len(G.nodes) > 0 else 0
    metrics["最大度数"] = max(dict(G.degree()).values()) if len(G.nodes) > 0 else 0
    metrics["网络密度"] = nx.density(G)
    
    # 连通性分析
    connected_components = list(nx.connected_components(G))
    metrics["连通分量数"] = len(connected_components)
    
    # 最大连通分量信息
    largest_cc = max(connected_components, key=len)
    metrics["最大连通分量节点数"] = len(largest_cc)
    metrics["最大连通分量占比"] = len(largest_cc) / len(G.nodes) if len(G.nodes) > 0 else 0
    
    # 孤立节点统计
    isolated_nodes = list(nx.isolates(G))
    metrics["孤立节点数"] = len(isolated_nodes)
    metrics["孤立节点占比"] = len(isolated_nodes) / len(G.nodes) if len(G.nodes) > 0 else 0
    
    # 使用抽样计算聚类系数
    if len(G.nodes) > 0:
        try:
            # 如果节点数小于sample_size，则使用所有节点
            if len(G.nodes) <= sample_size:
                sample_nodes = list(G.nodes)
            else:
                # 随机抽样节点，确保抽样节点的度数大于1（至少有2个邻居才能形成三角形）
                nodes_with_degree_gt_1 = [n for n, d in G.degree() if d > 1]
                # 如果有足够的有效节点，从中抽样
                if len(nodes_with_degree_gt_1) > sample_size/2:
                    sample_nodes = random.sample(nodes_with_degree_gt_1, min(sample_size, len(nodes_with_degree_gt_1)))
                # 否则从所有节点中抽样
                else:
                    sample_nodes = random.sample(list(G.nodes), min(sample_size, len(G.nodes)))
            
            print(f"使用 {len(sample_nodes)} 个样本节点计算聚类系数...")
            start_time = time.time()
            
            # 计算抽样节点的聚类系数
            clustering = nx.clustering(G, sample_nodes)
            metrics["抽样聚类系数"] = sum(clustering.values()) / len(clustering) if clustering else 0
            
            elapsed_time = time.time() - start_time
            print(f"聚类系数计算完成，耗时 {elapsed_time:.2f} 秒")
        except Exception as e:
            print(f"计算聚类系数时出错: {e}")
            metrics["抽样聚类系数"] = "计算失败"
    else:
        metrics["抽样聚类系数"] = 0
    
    return metrics

def get_largest_component(G):
    """获取最大连通分量"""
    largest_cc = max(nx.connected_components(G), key=len)
    return G.subgraph(largest_cc).copy()

def plot_degree_distribution(G, title, output_path=None):
    """绘制度分布图"""
    # 计算度分布
    degrees = [d for n, d in G.degree()]
    degree_counts = {}
    for d in degrees:
        degree_counts[d] = degree_counts.get(d, 0) + 1
    
    # 准备绘图数据
    x = list(degree_counts.keys())
    y = list(degree_counts.values())
    
    # 创建图表
    plt.figure(figsize=(10, 6))
    
    # 对数坐标绘制
    plt.loglog(x, y, 'bo', alpha=0.6, markersize=4)
    
    # 设置英文标签避免中文乱码问题
    plt.xlabel('Node Degree (log scale)')
    plt.ylabel('Frequency (log scale)')
    plt.title(title)
    plt.grid(True, alpha=0.3)
    
    # 保存图片
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"度分布图已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()

def print_metrics(metrics, title="网络分析结果"):
    """打印指标结果"""
    print(f"\n========== {title} ==========")
    
    # 基本统计
    print("\n基本统计:")
    print(f"节点数: {metrics['节点数']}")
    print(f"边数: {metrics['边数']}")
    print(f"平均度数: {metrics['平均度数']:.4f}")
    print(f"最大度数: {metrics['最大度数']}")
    print(f"网络密度: {metrics['网络密度']:.6f}")
    
    # 连通性
    print("\n连通性:")
    print(f"连通分量数: {metrics['连通分量数']}")
    print(f"最大连通分量节点数: {metrics['最大连通分量节点数']} ({metrics['最大连通分量占比']:.2%})")
    print(f"孤立节点数: {metrics['孤立节点数']} ({metrics['孤立节点占比']:.2%})")
    
    # 聚类
    print("\n聚类:")
    if "抽样聚类系数" in metrics:
        if isinstance(metrics["抽样聚类系数"], float):
            print(f"抽样聚类系数: {metrics['抽样聚类系数']:.6f}")
        else:
            print(f"抽样聚类系数: {metrics['抽样聚类系数']}")
    
    print("\n==============================")

def evaluate_network(metrics):
    """评估网络质量"""
    print("\n网络质量评估:")
    
    # 基于密度的评估
    if metrics["网络密度"] < 0.001:
        print("✗ 网络极度稀疏（密度<0.001），远低于理想的社交网络密度")
    elif metrics["网络密度"] < 0.01:
        print("△ 网络稀疏（密度<0.01），但在大型社交网络中较为常见")
    else:
        print("✓ 网络密度良好（密度≥0.01），适合进行网络分析")
    
    # 聚类系数评估
    if "抽样聚类系数" in metrics and isinstance(metrics["抽样聚类系数"], float):
        if metrics["抽样聚类系数"] < 0.01:
            print("✗ 聚类系数极低，网络中几乎没有形成三角形结构")
        elif metrics["抽样聚类系数"] < 0.1:
            print("△ 聚类系数较低，网络中局部聚集现象不明显")
        else:
            print("✓ 聚类系数良好，网络中存在局部聚集现象")
    
    # 孤立节点评估
    if metrics["孤立节点占比"] > 0.3:
        print("✗ 孤立节点占比过高，许多用户没有互动")
    elif metrics["孤立节点占比"] > 0.1:
        print("△ 孤立节点比例中等，部分用户缺乏互动")
    else:
        print("✓ 孤立节点比例低，用户互动活跃")
    
    # 连通性评估
    if metrics["最大连通分量占比"] < 0.5:
        print("✗ 网络高度分散，最大连通分量占比低于50%")
    elif metrics["最大连通分量占比"] < 0.8:
        print("△ 网络连通性一般，最大连通分量占比在50%-80%之间")
    else:
        print("✓ 网络连通性良好，最大连通分量占比超过80%")
    
    # 综合评估
    print("\n总体评估:")
    score = 0
    if metrics["网络密度"] >= 0.01: score += 2
    elif metrics["网络密度"] >= 0.001: score += 1
    
    if "抽样聚类系数" in metrics and isinstance(metrics["抽样聚类系数"], float):
        if metrics["抽样聚类系数"] >= 0.1: score += 2
        elif metrics["抽样聚类系数"] >= 0.01: score += 1
    
    if metrics["孤立节点占比"] <= 0.1: score += 2
    elif metrics["孤立节点占比"] <= 0.3: score += 1
    
    if metrics["最大连通分量占比"] >= 0.8: score += 2
    elif metrics["最大连通分量占比"] >= 0.5: score += 1
    
    if score >= 6:
        print("这个网络整体质量良好，适合进行深入的网络分析研究。")
    elif score >= 3:
        print("这个网络质量一般，某些方面存在不足，但仍可用于研究。")
    else:
        print("这个网络质量较差，建议使用其最大连通分量进行分析，或考虑其他数据集。")

def main():
    # 设置CSV文件路径
    csv_file = "./data/3人伪造老干妈印章与腾讯签合同被刑拘.csv"
    
    # 可选参数：限制处理的最大行数（用于测试）
    # max_rows = 10000  # 仅处理前10000行
    max_rows = None  # 处理全部数据
    
    # 确保结果目录存在
    os.makedirs("./results", exist_ok=True)
    
    # 1. 构建社交网络
    G = build_social_network(csv_file, max_rows=max_rows)
    
    # 2. 分析整个网络的指标
    print(f"\n分析整个网络（{len(G.nodes)}个节点，{len(G.edges)}条边）...")
    metrics = analyze_network(G)
    print_metrics(metrics, "整个网络分析结果")
    
    # 3. 绘制整个网络的度分布图
    plot_degree_distribution(G, "Whole Network Degree Distribution", "./results/whole_network_degree_distribution.png")
    
    # 4. 提取并分析最大连通分量
    print("\n提取并分析最大连通分量...")
    largest_cc = get_largest_component(G)
    print(f"最大连通分量包含 {len(largest_cc.nodes)} 个节点和 {len(largest_cc.edges)} 条边")
    
    largest_cc_metrics = analyze_network(largest_cc)
    print_metrics(largest_cc_metrics, "最大连通分量分析结果")
    
    # 5. 绘制最大连通分量的度分布图
    plot_degree_distribution(largest_cc, "Largest Component Degree Distribution", "./results/largest_cc_degree_distribution.png")
    
    # 6. 评估网络质量
    evaluate_network(metrics)
    
    print("\n研究建议:")
    if metrics["网络密度"] < 0.001:
        print("1. 建议使用最大连通分量进行后续分析，排除孤立的小组件")
    print("2. 在分析聚类特性时，应考虑网络的稀疏性和低聚类系数")
    print("3. 关注节点度分布和中心性指标，这些可能比社区结构更有意义")

if __name__ == "__main__":
    main()