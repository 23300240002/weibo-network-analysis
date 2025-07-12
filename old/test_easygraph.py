import easygraph.classes as eg
import easygraph.functions as eg_f

def test_betweenness_complex():
    # 构造含5个节点的图
    nodes = ['A', 'B', 'C', 'D', 'E']
    G = eg.Graph()
    G.add_nodes_from(nodes)
    # 添加边：A–B, B–C, C–D, D–E, B–D
    G.add_edge('A', 'B')
    G.add_edge('B', 'C')
    G.add_edge('C', 'D')
    G.add_edge('D', 'E')
    G.add_edge('B', 'D')
    
    # 调用 EasyGraph 内的 betweenness_centrality函数（假设返回归一化后的结果）
    bc = eg_f.betweenness_centrality(G)
    result = dict(zip(list(G.nodes), bc))
    
    print("复杂图中计算得到的介数中心性：")
    for node in sorted(result.keys()):
        print(f"{node}: {result[node]:.6f}")
    
    # 期望归一化介数中心性：
    # A: 0.0, B: 0.5, C: 0.0, D: 0.5, E: 0.0
    expected = {'A': 0.0, 'B': 0.5, 'C': 0.0, 'D': 0.5, 'E': 0.0}
    passed = all(abs(result[node] - expected[node]) < 1e-6 for node in expected)
    if passed:
        print("测试通过：复杂图的介数中心性计算正确！")
    else:
        print("测试失败：复杂图的介数中心性计算结果不正确！")
    
if __name__ == "__main__":
    test_betweenness_complex()