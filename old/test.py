import csv

def build_graph_method1(csv_path):
    """
    方法1：利用CSV中的“MD5-父微博用户ID”构造无向边关系
    如果某行中存在父微博用户ID，则建立边：作者 -- 父微博用户
    """
    graph = {}  # dict: key -> set(邻居)
    with open(csv_path, 'r', encoding='gb18030', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            author = row.get("MD5-作者ID", "").strip()
            parent_user = row.get("MD5-父微博用户ID", "").strip()
            if author and parent_user and author != parent_user:
                graph.setdefault(author, set()).add(parent_user)
                graph.setdefault(parent_user, set()).add(author)  # 添加反向边
    return graph

def build_graph_method2(csv_path):
    """
    方法2：利用“MD5-父微博ID”构造无向边关系
    先建立 tweet_id 到作者 的映射，再对每行存在 MD5-父微博ID 的，
    如果能通过映射获得父微博的作者，则建立边：作者 -- 父微博作者
    """
    tweet_to_author = {}
    with open(csv_path, 'r', encoding='gb18030', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tweet_id = row.get("MD5-mid", "").strip()
            author = row.get("MD5-作者ID", "").strip()
            if tweet_id and author:
                tweet_to_author[tweet_id] = author

    graph = {}
    with open(csv_path, 'r', encoding='gb18030', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            author = row.get("MD5-作者ID", "").strip()
            parent_tweet = row.get("MD5-父微博ID", "").strip()
            if author and parent_tweet:
                parent_author = tweet_to_author.get(parent_tweet, "").strip()
                if parent_author and parent_author != author:
                    graph.setdefault(author, set()).add(parent_author)
                    graph.setdefault(parent_author, set()).add(author)
    return graph

def find_undirected_triangles(graph):
    """
    找出无向图中的所有三角形（三个顶点），每个三角形只计一次。
    算法：对于每个节点 u，遍历 u 邻居中字典序大于 u 的节点 v，
    再遍历 v 邻居中字典序大于 v 的节点 w，
    如果 w 同时也是 u 的邻居，则 (u,v,w) 构成一个三角形。
    返回存有所有三角形的列表，每个元素为 (u, v, w)。
    """
    triangles = []
    for u in graph:
        for v in [x for x in graph[u] if x > u]:
            for w in [x for x in graph[v] if x > v]:
                if w in graph[u]:
                    triangles.append((u, v, w))
    return triangles

def count_edges(graph):
    """
    计算无向图中的总边数（每条边只计一次）。
    """
    total = 0
    for node in graph:
        total += len(graph[node])
    return total // 2  # 每条边在两个节点出现

def main():
    csv_file = "《网络直播营销活动行为规范》7月1日实施.csv"
    
    print("构造方法1（基于父微博用户ID）的无向关系图...")
    graph1 = build_graph_method1(csv_file)
    edges1 = count_edges(graph1)
    triangles_list_1 = find_undirected_triangles(graph1)
    print(f"方法1：图总边数为 {edges1}，检测到 {len(triangles_list_1)} 个三角形。")
    if triangles_list_1:
        print("三角形结构列表（顶点编号）：")
        for tri in triangles_list_1:
            print(tri)
    
    print("\n构造方法2（基于父微博ID对应作者）的无向关系图...")
    graph2 = build_graph_method2(csv_file)
    edges2 = count_edges(graph2)
    triangles_list_2 = find_undirected_triangles(graph2)
    print(f"方法2：图总边数为 {edges2}，检测到 {len(triangles_list_2)} 个三角形。")
    if triangles_list_2:
        print("三角形结构列表（顶点编号）：")
        for tri in triangles_list_2:
            print(tri)
    
    if len(triangles_list_1) != len(triangles_list_2):
        print("\n注意：两种方法得到的三角形个数不一致！")
    else:
        print("\n两种方法得到的三角形个数一致。")

if __name__ == "__main__":
    main()