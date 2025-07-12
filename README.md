# 微博用户邻居网络与其发布内容影响力的关联研究

本项目研究微博用户邻居网络的结构特征与其发布内容流行度之间的关联关系，通过构建用户的社交网络和邻居网络，提取网络特征，并利用因果森林模型分析网络结构对内容流行度的影响机制。

## 项目结构

```
data/
├── core/                   # 核心代码目录
│   ├── network.py          # 社交网络构建与分析基础函数
│   ├── export_to_jsonl2.py # 提取用户网络特征并导出为JSONL
│   ├── calculate_popularity.py # 计算微博流行度指标
│   ├── casual_forest_analysis.py # 因果森林分析模型
│   └── predict_popularity.py # 流行度预测模型
├── data/                   # 原始数据目录
│   ├── 《网络直播营销活动行为规范》7月1日实施.csv # 微博数据集示例
│   └── ...                 # 其他CSV数据集
├── results/                # 处理结果目录
│   ├── result.jsonl        # 包含完整特征的用户网络特征数据
│   ├── result1.jsonl       # 另一个网络特征数据集
│   └── popularity.jsonl    # 微博流行度数据
└── old/                    # 旧版代码和测试文件（仅作参考）
```

## 数据说明

### 原始数据
本项目使用微博公开数据集，存储为CSV格式，每条记录包含：
- 作者ID（MD5加密）
- 父微博ID及其作者ID
- 内容标题、发布地区
- 粉丝数
- 转发/点赞/评论数量
- 原创/转发标记

### 处理后数据
处理后的数据以JSONL格式存储，包含：

1. **用户网络特征数据** (`result.jsonl`):
   ```json
   {
     "user_id": "hash值",
     "personal_info": {
       "region": "地区",
       "fans_count": 粉丝数,
       "total_posts": 发帖总数
     },
     "network_info": {
       "degree": 度,
       "global_betweenness": 全局介数中心性,
       "global_degree_centrality": 全局度中心性
     },
     "ego_network_info": {
       "clustering_coefficient": 聚类系数,
       "density": 密度,
       "average_nearest_neighbor_degree": 邻居平均度,
       "ego_betweenness": 局部介数中心性
     }
   }
   ```

2. **流行度数据** (`popularity.jsonl`):
   ```json
   {
     "user_id": "hash值",
     "is_retweet": true/false,
     "popularity": 流行度值
   }
   ```

## 处理流程

1. **社交网络构建**：基于微博用户间的转发关系，构建无向图
2. **特征提取**：计算每个用户的邻居网络特征与全局网络指标
3. **流行度计算**：聚合每条微博的转发、点赞、评论数作为流行度指标
4. **因果分析**：使用因果森林模型分析网络特征对流行度的因果效应
5. **预测建模**：构建随机森林模型预测内容流行度

## 核心脚本说明

### network.py
提供社交网络构建与分析的基础函数。

**主要功能**：
- `build_social_network_from_csv(csv_path)`: 从CSV文件构建社交网络图
- `calculate_average_neighbor_degree(G)`: 计算图中节点的平均邻居度
- `compute_graph_metrics(G)`: 计算整个图的统计指标（节点数、边数、平均度等）
- `compute_global_centrality(G, node)`: 计算节点的全局介数中心性
- `compute_global_degree_centrality(G, node)`: 计算节点的全局度中心性

**输入**：微博CSV数据文件
**输出**：社交网络图对象(EasyGraph Graph)

### export_to_jsonl2.py
提取用户网络特征并导出为JSONL格式。

**主要功能**：
- 从CSV数据构建社交网络
- 提取每个用户的个人信息
- 构建Ego Network并计算网络指标
- 将结果保存为JSONL格式

**输入**：微博CSV数据文件
**输出**：包含用户网络特征的JSONL文件(`result.jsonl`)

### calculate_popularity.py
计算每条微博的流行度指标。

**主要功能**：
- 从CSV数据中提取每条微博的作者ID、转发状态
- 计算流行度（转发+点赞+评论数量）
- 将结果保存为JSONL格式

**输入**：微博CSV数据文件
**输出**：包含流行度指标的JSONL文件(`popularity.jsonl`)

### casual_forest_analysis.py
使用因果森林模型分析网络特征对流行度的因果效应。

**主要功能**：
- 合并用户网络特征与流行度数据
- 提取解释变量（聚类系数、密度、邻居平均度、局部介数中心性）
- 控制混淆变量（全局度、是否转发、粉丝数、发帖总数）
- 利用CausalForestDML模型估计局部平均处理效应
- 可视化因果效应分布

**输入**：
- 用户网络特征数据(`result.jsonl`)
- 流行度数据(`popularity.jsonl`)

**输出**：
- 平均因果效应值
- 因果效应分布图

**关键解释变量**：
- `clustering_coefficient`: 聚类系数，衡量邻居间连接的紧密程度
- `density`: 网络密度，反映邻居间实际边数与理论最大边数的比例
- `average_nearest_neighbor_degree`: 邻居平均度，表征邻居节点的平均连接数
- `ego_betweenness`: 局部介数中心性，评估用户在邻居网络中的桥梁作用

**混淆变量**：
- `global_degree`: 用户在全局网络中的度
- `is_retweet_num`: 是否为转发内容
- `fans_count`: 粉丝数量
- `total_posts`: 发帖总数

### predict_popularity.py
构建随机森林模型预测内容流行度。

**主要功能**：
- 合并用户网络特征与流行度数据
- 提取预测特征（包括网络指标和用户属性）
- 构建随机森林回归模型
- 评估模型性能（MSE、R²）
- 使用SHAP分析特征重要性

**输入**：
- 用户网络特征数据(`result.jsonl`)
- 流行度数据(`popularity.jsonl`)

**输出**：
- 模型性能指标
- 特征重要性分析
- 预测值与实际值对比图

## 结果分析

本项目通过因果森林分析和预测模型，探索了几个关键发现：

1. **网络特征的因果效应**：
   - 分析了聚类系数、密度、邻居平均度和局部介数中心性对内容流行度的因果影响
   - 通过控制混淆变量，揭示了网络结构特征的独立效应

2. **预测模型性能**：
   - 使用随机森林模型预测内容流行度
   - 评估模型性能并分析特征重要性

3. **特征交互**：
   - 通过SHAP分析揭示了网络特征与用户属性间的交互影响

## 使用示例

### 从CSV数据构建社交网络并提取特征

```python
# 构建社交网络
from core.network import build_social_network_from_csv
social_graph = build_social_network_from_csv("./data/微博数据.csv")

# 提取网络特征并保存
from core.export_to_jsonl2 import export_users_to_jsonl
export_users_to_jsonl("./data/微博数据.csv", "./results/result.jsonl")

# 计算流行度指标
from core.calculate_popularity import calculate_popularity
calculate_popularity("./data/微博数据.csv", "./results/popularity.jsonl")
```

### 进行因果分析

```python
# 运行因果森林分析
from core.casual_forest_analysis import run_causal_forest_analysis
# 脚本会自动加载result.jsonl和popularity.jsonl文件
# 并针对每个解释变量进行因果分析
```

### 构建预测模型

```python
# 运行流行度预测模型
from core.predict_popularity import main
# 脚本会自动加载数据、构建模型并输出分析结果
```

## 注意事项

1. 使用前请确保安装以下依赖库：
   - pandas
   - numpy
   - matplotlib
   - econml
   - sklearn
   - shap
   - easygraph

2. 数据文件较大时，网络构建和特征提取可能需要较长时间，请耐心等待

3. 因果森林分析的结果解释需结合理论背景和实际场景