import easygraph
import easygraph.classes as eg
import easygraph.functions as eg_f
import networkx as nx
import numpy as np
from scipy import linalg
import pandas as pd
import matplotlib.pyplot as plt

def calculate_spectral_radius(G):
    """计算图的谱半径（最大特征值的绝对值）"""
    try:
        # 使用顶层easygraph模块的to_numpy_array
        adj_matrix = easygraph.to_numpy_array(G)
        eigenvalues = linalg.eigvals(adj_matrix)
        return float(np.max(np.abs(eigenvalues)))
    except Exception as e:
        print(f"计算谱半径出错: {e}")
        return 0.0

def calculate_modularity(G):
    """计算图的模块度"""
    try:
        communities = eg_f.louvain_communities(G)
        return eg_f.modularity(G, communities)
    except Exception as e:
        print(f"计算模块度出错: {e}")
        return 0.0

def calculate_average_neighbor_degree(G, node):
    """计算节点的邻居平均度数"""
    neighbors = list(G.neighbors(node))
    if not neighbors:
        return 0.0
    
    # 直接计算邻居的度数（通过计算邻居的邻居数量）
    neighbor_degrees = []
    for neighbor in neighbors:
        # 直接计算每个邻居有多少个连接节点
        deg_val = len(list(G.neighbors(neighbor)))
        neighbor_degrees.append(deg_val)
    
    return sum(neighbor_degrees) / len(neighbor_degrees)

def create_ego_network(G, node):
    """创建二跳邻居网络"""
    try:
        # 明确指定radius=2获取二跳邻居网络
        return eg_f.ego_graph(G, node, radius=2)
    except Exception as e:
        print(f"创建邻居网络出错: {e}")
        return None

def calculate_network_metrics(ego_graph, center_node):
    """计算网络的六个指标"""
    metrics = {}
    
    # 1. 密度
    try:
        metrics['density'] = eg.density(ego_graph)
    except Exception as e:
        print(f"计算密度出错: {e}")
        metrics['density'] = 0.0
        
    # 2. 聚类系数
    try:
        metrics['clustering_coefficient'] = eg_f.clustering(ego_graph, center_node)
    except Exception as e:
        print(f"计算聚类系数出错: {e}")
        metrics['clustering_coefficient'] = 0.0
    
    # 3. 邻居平均度
    try:
        metrics['average_nearest_neighbor_degree'] = calculate_average_neighbor_degree(ego_graph, center_node)
    except Exception as e:
        print(f"计算邻居平均度出错: {e}")
        metrics['average_nearest_neighbor_degree'] = 0.0
    
    # 4. 局部介数中心性
    try:
        bc = eg_f.betweenness_centrality(ego_graph)
        # 此处EasyGraph的返回值为列表
        node_list = list(ego_graph.nodes)
        # 找到中心节点在列表中的索引
        center_index = node_list.index(center_node)
        # 使用索引获取中心节点的介数中心性
        metrics['ego_betweenness'] = bc[center_index]
    except Exception as e:
        print(f"计算介数中心性出错: {e}")
        metrics['ego_betweenness'] = 0.0
    
    # 5. 谱半径
    try:
        metrics['spectral_radius'] = calculate_spectral_radius(ego_graph)
    except Exception as e:
        print(f"计算谱半径出错: {e}")
        metrics['spectral_radius'] = 0.0
    
    # 6. 模块度
    try:
        metrics['modularity'] = calculate_modularity(ego_graph)
    except Exception as e:
        print(f"计算模块度出错: {e}")
        metrics['modularity'] = 0.0
    
    return metrics

def print_separator():
    """打印分隔线"""
    print("\n" + "="*60 + "\n")

def test_network_metrics():
    """测试网络指标计算"""
    print("创建测试网络...")
    
    # 创建一个测试网络，具有明确的结构
    # A是中心节点，B、C是一跳邻居，D、E、F是二跳邻居，G是三跳邻居，H是孤立节点
    G = eg.Graph()
    
    # 构建星形+环形的混合结构
    G.add_edge('A', 'B')  # 中心与一跳
    G.add_edge('A', 'C')  # 中心与一跳
    G.add_edge('B', 'D')  # 一跳与二跳
    G.add_edge('B', 'E')  # 一跳与二跳
    G.add_edge('C', 'F')  # 一跳与二跳
    G.add_edge('D', 'E')  # 二跳间的边，形成三角形
    G.add_edge('E', 'F')  # 二跳间的边，形成环
    G.add_edge('E', 'G')  # 三跳邻居G，混淆项，确认网络爬取无误
    G.add_edge('F', 'G')  # 三跳邻居G，混淆项，确认网络爬取无误
    G.add_node('H')  # 孤立节点，混淆项，确认网络爬取无误
    
    print(f"原始网络：{len(G.nodes)} 个节点，{len(G.edges)} 条边")
    print(f"节点：{list(G.nodes)}")
    print(f"边：{[(u, v) for u, v, _ in G.edges]}")
    
    # 网络可视化
    try:
        plt.figure(figsize=(10, 8)) # 设置图形大小
        pos = {'A': (0, 0), 'B': (-1, 1), 'C': (1, 1), 
               'D': (-2, 2), 'E': (0, 2), 'F': (2, 2),
                'G': (0, 3), 'H': (-2, 3)}
        nx.draw(nx.Graph([(u, v) for u, v, _ in G.edges]), 
                pos=pos, with_labels=True, node_size=500, font_size=15)
        plt.title("测试网络结构")
        plt.savefig("test_network.png")
        print("网络结构图已保存至 test_network.png")
    except Exception as e:
        print(f"绘制网络图出错: {e}")
    
    # 选择中心节点
    center_node = 'A'
    
    print_separator()
    print("1. 测试二跳邻居网络提取...")
    ego_net = create_ego_network(G, center_node)
    
    if ego_net:
        print(f"二跳邻居网络：{len(ego_net.nodes)} 个节点，{len(ego_net.edges)} 条边")
        print(f"节点：{list(ego_net.nodes)}")
        print(f"边：{[(u, v) for u, v, _ in ego_net.edges]}")
        
        # 验证二跳邻居网络是否完整
        expected_nodes = {'A', 'B', 'C', 'D', 'E', 'F'}
        expected_edges = {('A', 'B'), ('A', 'C'), ('B', 'D'), ('B', 'E'), 
                         ('C', 'F'), ('D', 'E'), ('E', 'F')}
        
        actual_nodes = set(ego_net.nodes)
        
        # 将边转换为无序对以进行比较（忽略属性字典）
        actual_edges = set()
        for u, v, _ in ego_net.edges:
            # 使用frozenset确保无序比较
            actual_edges.add(frozenset([u, v]))
        
        expected_edges = set(frozenset(e) for e in expected_edges)
        
        print("\n二跳邻居网络验证结果：")
        print(f"节点验证：{'✓ 正确' if actual_nodes == expected_nodes else '✗ 错误'}")
        print(f"边验证：{'✓ 正确' if actual_edges == expected_edges else '✗ 错误'}")
        
        if actual_nodes != expected_nodes:
            print(f"  缺失节点: {expected_nodes - actual_nodes}")
            print(f"  多余节点: {actual_nodes - expected_nodes}")
        
        if actual_edges != expected_edges:
            missing = set()
            for e in expected_edges:
                if e not in actual_edges:
                    missing.add(tuple(e))
            
            extra = set()
            for e in actual_edges:
                if e not in expected_edges:
                    extra.add(tuple(e))
                    
            print(f"  缺失边: {missing}")
            print(f"  多余边: {extra}")
        
        print_separator()
        print("2. 测试六个网络指标计算...")
        metrics = calculate_network_metrics(ego_net, center_node)
        
        # 打印计算结果
        print("\n计算得到的网络指标：")
        for metric, value in sorted(metrics.items()):
            print(f"{metric:30}: {value:.6f}")
        
        # 手动计算一些可以验证的指标
        print_separator()
        print("3. 手动验证部分指标...")
        
        # 密度 = 边数 / (n*(n-1)/2)
        n = len(ego_net.nodes)
        manual_density = len(ego_net.edges) / (n * (n-1) / 2)
        print(f"密度手动计算: {manual_density:.6f}")
        print(f"密度比较: {'✓ 一致' if abs(metrics['density'] - manual_density) < 1e-6 else '✗ 不一致'}")
        
        # A的聚类系数应为0，因为它的邻居B和C之间没有边
        manual_clustering = 0
        print(f"聚类系数手动计算: {manual_clustering:.6f}")
        print(f"聚类系数比较: {'✓ 一致' if abs(metrics['clustering_coefficient'] - manual_clustering) < 1e-6 else '✗ 不一致'}")
        
        # 邻居平均度：A的邻居是B和C，B的度为3，C的度为2，平均为2.5
        manual_annd = (3 + 2) / 2
        print(f"邻居平均度手动计算: {manual_annd:.6f}")
        print(f"邻居平均度比较: {'✓ 一致' if abs(metrics['average_nearest_neighbor_degree'] - manual_annd) < 1e-6 else '✗ 不一致'}")
        
        print_separator()
        print("测试完成！")
    else:
        print("创建二跳邻居网络失败，无法继续测试")

if __name__ == "__main__":
    test_network_metrics()