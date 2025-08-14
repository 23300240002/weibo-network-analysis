# 微博用户邻居网络与其发布内容影响力的关联研究

本项目旨在系统研究微博用户邻居网络结构特征与其发布内容流行度（影响力）之间的关系。通过自动化爬取微博用户的**粉丝列表**（following network），构建二跳邻居网络（Ego Network），高效提取网络结构指标，并结合用户内容的流行度，探索网络结构对用户影响力的作用机制。

---

## 项目结构

```
data/
├── core/                               # 核心分析与指标计算代码
│   ├── create_ego_network/             # 二跳邻居网络构建与指标提取
│   │   ├── create3.py                  # 第三版（最新）的二跳网络构建与五大指标提取（修复EasyGraph入边bug，支持断点续传）
│   │   ├── analysis_with_networkx.py   # 尝试用networkx修复二跳网络入边问题（未完全解决）
│   │   └── new_analysis.py             # 早期EasyGraph分析主脚本（有入边bug）
│   ├── correlation_analysis/           # 异常用户检测与相关性分析
│   │   ├── pick_out_abnormal_users.py  # 三种异常用户检测方法，批量筛选异常用户
│   │   └── analysis_without_abnormal.py# 排除异常用户后的相关性分析与对比
│   └── network_analysis/               # 网络整体结构分析
│       └── process_following_network.py# 输出节点数、度分布、密度等结构性报告
├── crawler/                            # 网络数据采集与处理
│   ├── weiboSpider/                    # 配置文件（如config.json，填写cookie和目标用户ID）
│   └── fetch/                          # 粉丝网络爬取与合并
│       ├── fetch2.py                   # 采用selenium爬取粉丝列表，构建二跳粉丝网络（最新推荐）
│       ├── fetch_following.py          # 早期关注网络爬虫（已不推荐）
│       ├── merge_networks.py           # 合并多个用户的网络数据为大网络
│       └── refind_missed_users.py      # 补全网络中“没有出边”的用户，提升网络完整性
├── old/                                # 历史代码归档（已弃用/仅供参考）
│   ├── before_2025_07/                 # 2025年7月前的探索代码
│   ├── 2025_07_first_try/              # 2025年7月的旧结果检查代码
│   └── test/                           # 各类测试脚本
├── data/                               # 原始微博CSV数据
│   └── ...                             # 微博原始数据集
├── results/                            # 处理结果目录
│   ├── merged_network_result3/         # 最新大网络及其指标分析结果
│   ├── pick_out_abnormal_users/        # 各种异常用户筛选结果
│   ├── correlation_result/             # 排除异常用户后的相关性分析结果
│   └── ...                             # 其他分析结果
```

---

## 各目录与主要脚本功能说明

### core

#### create_ego_network/
- **create3.py**  
  最新的二跳邻居网络构建与五大指标提取脚本。修复了EasyGraph原生ego_graph无法正确纳入粉丝入边的bug，采用自定义BFS实现真正的“二跳粉丝网络”，并支持断点续传。默认计算五大指标（密度、聚类系数、邻居平均度、谱半径、模块度），如需可扩展为六大指标。
- **analysis_with_networkx.py**  
  尝试用networkx实现二跳网络构建，解决EasyGraph入边bug，但实际效果有限，未完全解决。
- **new_analysis.py**  
  早期主分析脚本，采用EasyGraph直接构建二跳网络，但存在只考虑出边（关注）而忽略入边（粉丝）的bug，导致二跳网络不完整。

#### correlation_analysis/
- **pick_out_abnormal_users.py**  
  提供三种异常用户检测方法（影响力/连边数比值、结构洞异常、邻居质量异常），支持批量筛选异常用户并输出详细报告。排除异常用户有助于去除网络边缘或失真节点，提升后续相关性分析的准确性。
- **analysis_without_abnormal.py**  
  对不同异常用户排除方案下，自动批量分析网络结构指标与流行度的相关性，输出对比报告。

#### network_analysis/
- **process_following_network.py**  
  对每个用户网络及合并网络整体结构进行分析，包括节点数、边数、度分布、网络密度、聚类系数、没有出边的用户比例等，辅助理解网络质量与可见性问题。

### crawler

#### weiboSpider/
- **config.json**  
  配置文件，填写微博cookie和要爬取的目标用户ID列表。

#### fetch/
- **fetch2.py**  
  推荐使用的主爬虫脚本。采用selenium自动化爬取微博粉丝列表，严格只采集粉丝（入边），构建二跳粉丝网络（A类=种子用户，B类=粉丝，C类=粉丝的粉丝）。支持高效断点续传、边过滤（C类只保留指向ABC类的边）、高粉丝用户上限等功能，确保网络结构与“影响力”定义一致。
- **fetch_following.py**  
  早期关注网络爬虫，采集关注列表（出边），已不推荐使用。
- **merge_networks.py**  
  合并多个用户的网络数据（用户表、边表、流行度表、节点类别）为一个大网络，便于整体分析。
- **refind_missed_users.py**  
  检查并补全网络中“没有出边”的用户（如因隐私或漏爬），提升网络数据完整性。

### old

- **before_2025_07/**、**2025_07_first_try/**、**test/**  
  历史探索代码，已弃用，仅供参考。

### data

- 存放原始微博CSV数据，每条记录包含作者ID、内容、粉丝数、转发/点赞/评论数等。

### results

- **merged_network_result3/**  
  最新大网络及其各类指标分析结果（如network_metrics.jsonl、merged_metrics_popularity.csv等）。
- **pick_out_abnormal_users/**  
  各种异常用户筛选结果（不同方法和比例）。
- **correlation_result/**  
  排除异常用户后的相关性分析结果。
- 其他目录存放各类分析结果和历史结果。

---

## 数据处理与分析流水线

1. **微博粉丝网络采集**  
   - 使用`crawler/fetch/fetch2.py`，自动化爬取目标用户的粉丝列表，递归采集二跳粉丝网络（A类=种子，B类=粉丝，C类=粉丝的粉丝）。  
   - 只采集粉丝（入边），不再采集关注（出边），确保网络结构与“影响力”定义一致。  
   - 支持断点续传、高粉丝用户上限、C类边过滤等，保证网络质量。

2. **网络数据合并**  
   - 使用`crawler/fetch/merge_networks.py`，将多个用户的网络数据合并为一个大网络，生成合并后的用户表、边表、流行度表等。

3. **二跳邻居网络构建与指标提取**  
   - 使用`core/create_ego_network/create3.py`，针对合并网络中每个用户，自动提取其二跳邻居网络（真正的“二跳粉丝网络”），高效计算五/六大结构指标，并与用户内容流行度整合，输出合并数据（如merged_metrics_popularity.csv）。
   - 该脚本修复了EasyGraph原生方法无法纳入粉丝入边的bug，支持断点续传。

4. **异常用户检测与排除（可选）**  
   - 使用`core/correlation_analysis/pick_out_abnormal_users.py`，通过三种方法批量检测并排除异常用户（如网络边缘节点、结构洞异常、邻居质量异常等），避免失真节点影响分析结果。

5. **相关性分析**  
   - 使用`core/correlation_analysis/analysis_without_abnormal.py`，对不同异常用户排除方案下，自动分析网络结构指标与流行度的相关性，输出详细对比报告。

6. **网络整体结构分析（辅助）**  
   - 使用`core/network_analysis/process_following_network.py`，分析网络整体结构特征，辅助理解网络质量与可见性。

---

## 研究变量说明

- **自变量X：二跳邻居网络六大结构指标**  
  主要通过EasyGraph高效计算，具体包括：
  1. **density（密度）**：邻居间实际边数与理论最大边数的比值，反映网络紧密程度。
  2. **clustering_coefficient（聚类系数）**：衡量邻居间互相连接的紧密程度。
  3. **average_nearest_neighbor_degree（邻居平均度）**：邻居节点的平均连接数，反映用户周围的活跃度。
  4. **ego_betweenness（局部介数中心性）**：用户在其邻居网络中的桥梁作用。
  5. **spectral_radius（谱半径）**：邻居网络的最大特征值，反映结构复杂性。
  6. **modularity（模块度）**：社区划分的紧密度，反映网络的社区结构明显性。

  > 说明：如遇介数中心性计算过慢，可暂时只计算前五项，但六大指标均为研究重点。

- **因变量Y：内容流行度（avg_popularity）**  
  以用户内容的转发、点赞、评论总数（转赞评）为基础，计算平均流行度，作为用户影响力的衡量。

---

## 课题意义

本项目通过自动化采集和高效分析，系统考察了微博用户邻居网络结构对其内容影响力的作用机制。采用严格的“粉丝网络”采集与二跳邻居网络构建，结合多维结构指标与流行度，揭示社交网络中结构性因素对信息传播和用户影响力的影响，为相关理论与实证研究提供数据与方法支持。

---

## 注意事项

1. **依赖库**：请确保已安装以下依赖（可通过pip安装）：
   - pandas
   - numpy
   - matplotlib
   - easygraph
   - tqdm
   - requests
   - selenium
2. **数据量较大时**，网络构建和指标计算可能需要较长时间，请耐心等待。
3. **cookie失效时**，需重新获取并填写。
4. 本项目仅用于学术研究，严禁用于任何违反平台规定的用途。