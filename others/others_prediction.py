import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import matplotlib.pyplot as plt
from scipy.stats import kendalltau
import random
import os
from datetime import datetime

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimSun', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.size'] = 10

def seed_everything(seed=42):
    """固定随机种子"""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    print(f"✅ 随机种子已固定为 {seed}")

class BiGRUModel(nn.Module):
    """学长的Bi-GRU模型（适配版）"""
    def __init__(self, input_size, hidden_size=128, hidden_size2=32, out_features=1):
        super(BiGRUModel, self).__init__()
        # 双向GRU层
        self.gru = nn.GRU(input_size, hidden_size, batch_first=True, bidirectional=True)
        self.dropout = nn.Dropout(p=0.5)
        # 由于是双向GRU，隐藏层维度变为 hidden_size * 2
        self.l1 = nn.Linear(hidden_size * 2, hidden_size2)
        self.l2 = nn.Linear(hidden_size2, out_features)

    def forward(self, x):
        x = x.unsqueeze(1)  # 添加序列维度
        # Bi-GRU的输出
        gru_out, _ = self.gru(x)
        gru_out_last_step = gru_out[:, -1, :]
        gru_out_last_step = self.dropout(gru_out_last_step)
        
        # 通过全连接层
        l1_out = self.l1(gru_out_last_step)
        output = torch.relu(self.l2(l1_out))
        return output

class GLSTMModel(nn.Module):
    """学长的GLSTM模型（适配版）"""
    def __init__(self, input_size, hidden_size=128, hidden_size2=32, out_features=1):
        super(GLSTMModel, self).__init__()
        # LSTM层
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.dropout = nn.Dropout(p=0.5)
        # 全连接层
        self.l1 = nn.Linear(hidden_size, hidden_size2)
        self.l2 = nn.Linear(hidden_size2, out_features)

    def forward(self, x):
        x = x.unsqueeze(1)  # 添加序列维度
        # LSTM层的输出
        lstm_out, _ = self.lstm(x)
        # 取LSTM输出的最后一个时间步
        lstm_out_last_step = lstm_out[:, -1, :]
        lstm_out_last_step = self.dropout(lstm_out_last_step)
        
        # 通过全连接层
        l1_out = self.l1(lstm_out_last_step)
        output = torch.relu(self.l2(l1_out))
        return output

class MLPBaseline(nn.Module):
    """简单MLP作为基线"""
    def __init__(self, input_size, hidden_size=64, out_features=1):
        super(MLPBaseline, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden_size, hidden_size//2),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden_size//2, out_features),
            nn.ReLU()
        )

    def forward(self, x):
        return self.model(x)

def load_and_preprocess_data(data_path='C:/Tengfei/data/results/test2.csv'):
    """加载和预处理数据"""
    print(f"📁 加载数据: {data_path}")
    
    try:
        df = pd.read_csv(data_path)
        print(f"✅ 成功加载数据: {len(df)} 个用户")
    except Exception as e:
        print(f"❌ 加载数据失败: {e}")
        return None, None, None, None
    
    # 🎯 选择8个相关性较大的特征（基于之前的分析）
    feature_columns = [
        'density',
        'clustering_coefficient', 
        'average_nearest_neighbor_degree',
        'spectral_radius',
        'modularity',
        'global_in_degree',
        'global_out_degree',
        'node_count'
    ]
    
    target_column = 'avg_popularity_of_all'
    
    # 检查列是否存在
    missing_features = [col for col in feature_columns if col not in df.columns]
    if missing_features:
        print(f"❌ 缺少特征列: {missing_features}")
        print(f"📋 可用列: {list(df.columns)}")
        return None, None, None, None
    
    if target_column not in df.columns:
        print(f"❌ 缺少目标列: {target_column}")
        return None, None, None, None
    
    # 提取特征和目标
    X = df[feature_columns].copy()
    y = df[target_column].copy()
    
    print(f"✅ 特征选择完成:")
    print(f"   📊 特征数量: {len(feature_columns)}")
    print(f"   📊 选择的特征: {feature_columns}")
    
    # 数据质量分析
    print(f"\n📊 数据质量分析:")
    print(f"   总样本数: {len(y)}")
    zero_count = (y == 0).sum()
    print(f"   影响力为0的用户: {zero_count} ({zero_count/len(y)*100:.1f}%)")
    print(f"   有影响力的用户: {len(y)-zero_count} ({(len(y)-zero_count)/len(y)*100:.1f}%)")
    print(f"   影响力统计: 均值={y.mean():.2f}, 最大值={y.max():.2f}, 标准差={y.std():.2f}")
    
    # 处理缺失值
    if X.isnull().any().any():
        print(f"⚠️ 发现缺失值，用均值填充")
        X = X.fillna(X.mean())
    
    return X, y, feature_columns, target_column

def create_data_loaders(X, y, test_size=0.2, val_size=0.2, batch_size=32, use_normalization=True):
    """创建数据加载器"""
    print(f"📊 创建数据加载器...")
    
    # 划分数据集
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_size/(1-test_size), random_state=42
    )
    
    print(f"   训练集: {len(X_train)} 样本")
    print(f"   验证集: {len(X_val)} 样本") 
    print(f"   测试集: {len(X_test)} 样本")
    
    # 特征归一化
    if use_normalization:
        print(f"   🔧 应用特征归一化...")
        feature_scaler = StandardScaler()
        X_train_scaled = feature_scaler.fit_transform(X_train)
        X_val_scaled = feature_scaler.transform(X_val)
        X_test_scaled = feature_scaler.transform(X_test)
    else:
        X_train_scaled = X_train.values
        X_val_scaled = X_val.values
        X_test_scaled = X_test.values
        feature_scaler = None
    
    # 目标变量变换（学长使用log变换）
    print(f"   🔧 应用目标变量log变换...")
    y_train_log = np.log(y_train + 1)
    y_val_log = np.log(y_val + 1)
    y_test_log = np.log(y_test + 1)
    
    # 转换为PyTorch张量
    X_train_tensor = torch.FloatTensor(X_train_scaled)
    X_val_tensor = torch.FloatTensor(X_val_scaled)
    X_test_tensor = torch.FloatTensor(X_test_scaled)
    
    y_train_tensor = torch.FloatTensor(y_train_log.values).reshape(-1, 1)
    y_val_tensor = torch.FloatTensor(y_val_log.values).reshape(-1, 1)
    y_test_tensor = torch.FloatTensor(y_test_log.values).reshape(-1, 1)
    
    # 创建数据加载器
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    val_dataset = TensorDataset(X_val_tensor, y_val_tensor)
    test_dataset = TensorDataset(X_test_tensor, y_test_tensor)
    
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)
    
    return {
        'train_loader': train_loader,
        'val_loader': val_loader, 
        'test_loader': test_loader,
        'feature_scaler': feature_scaler,
        'original_data': {
            'y_train': y_train,
            'y_val': y_val,
            'y_test': y_test
        }
    }

def train_model(model, train_loader, val_loader, num_epochs=200, lr=0.001, device='cpu'):
    """训练模型"""
    print(f"🚀 开始训练模型: {model.__class__.__name__}")
    
    model = model.to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    
    train_losses = []
    val_losses = []
    best_val_loss = float('inf')
    best_model_state = None
    patience = 20
    no_improve_count = 0
    
    for epoch in range(num_epochs):
        # 训练阶段
        model.train()
        train_epoch_loss = 0
        for batch_x, batch_y in train_loader:
            batch_x, batch_y = batch_x.to(device), batch_y.to(device)
            
            optimizer.zero_grad()
            outputs = model(batch_x)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()
            
            train_epoch_loss += loss.item()
        
        avg_train_loss = train_epoch_loss / len(train_loader)
        train_losses.append(avg_train_loss)
        
        # 验证阶段
        model.eval()
        val_epoch_loss = 0
        with torch.no_grad():
            for batch_x, batch_y in val_loader:
                batch_x, batch_y = batch_x.to(device), batch_y.to(device)
                outputs = model(batch_x)
                loss = criterion(outputs, batch_y)
                val_epoch_loss += loss.item()
        
        avg_val_loss = val_epoch_loss / len(val_loader)
        val_losses.append(avg_val_loss)
        
        # 保存最佳模型
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            best_model_state = model.state_dict().copy()
            no_improve_count = 0
        else:
            no_improve_count += 1
        
        if (epoch + 1) % 50 == 0:
            print(f"   Epoch {epoch+1}/{num_epochs} - Train Loss: {avg_train_loss:.6f}, Val Loss: {avg_val_loss:.6f}")
        
        # 早停
        if no_improve_count >= patience:
            print(f"   ⏹ 早停于第 {epoch+1} 轮")
            break
    
    # 加载最佳模型
    model.load_state_dict(best_model_state)
    print(f"✅ 训练完成，最佳验证损失: {best_val_loss:.6f}")
    
    return model, train_losses, val_losses

def evaluate_model(model, data_loaders, device='cpu'):
    """评估模型"""
    model.eval()
    results = {}
    
    with torch.no_grad():
        for split_name, loader in [('train', data_loaders['train_loader']), 
                                  ('val', data_loaders['val_loader']),
                                  ('test', data_loaders['test_loader'])]:
            
            all_preds = []
            all_targets = []
            
            for batch_x, batch_y in loader:
                batch_x, batch_y = batch_x.to(device), batch_y.to(device)
                outputs = model(batch_x)
                
                all_preds.extend(outputs.cpu().numpy())
                all_targets.extend(batch_y.cpu().numpy())
            
            # 转换回原始尺度
            preds_original = np.expm1(np.array(all_preds).flatten())
            targets_original = np.expm1(np.array(all_targets).flatten())
            
            # 确保非负
            preds_original = np.maximum(preds_original, 0)
            
            # 计算指标
            mse = mean_squared_error(targets_original, preds_original)
            r2 = r2_score(targets_original, preds_original)
            mae = mean_absolute_error(targets_original, preds_original)
            
            # Kendall tau
            tau, p_value = kendalltau(targets_original, preds_original)
            
            results[split_name] = {
                'mse': mse,
                'r2': r2, 
                'mae': mae,
                'kendall_tau': tau,
                'kendall_p': p_value,
                'predictions': preds_original,
                'targets': targets_original
            }
    
    return results

def plot_results(results, model_name, output_dir):
    """绘制结果图表"""
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 训练集预测散点图
    train_preds = results['train']['predictions']
    train_targets = results['train']['targets']
    train_r2 = results['train']['r2']
    train_tau = results['train']['kendall_tau']
    
    axes[0, 0].scatter(train_targets, train_preds, alpha=0.6, s=20)
    axes[0, 0].plot([train_targets.min(), train_targets.max()], 
                    [train_targets.min(), train_targets.max()], 'r--', lw=2)
    axes[0, 0].set_xlabel('真实影响力')
    axes[0, 0].set_ylabel('预测影响力')
    axes[0, 0].set_title(f'{model_name} - 训练集\nR² = {train_r2:.4f}, Kendall τ = {train_tau:.4f}')
    axes[0, 0].grid(True, alpha=0.3)
    
    # 测试集预测散点图
    test_preds = results['test']['predictions']
    test_targets = results['test']['targets']
    test_r2 = results['test']['r2']
    test_tau = results['test']['kendall_tau']
    
    axes[0, 1].scatter(test_targets, test_preds, alpha=0.6, s=20, color='orange')
    axes[0, 1].plot([test_targets.min(), test_targets.max()], 
                    [test_targets.min(), test_targets.max()], 'r--', lw=2)
    axes[0, 1].set_xlabel('真实影响力')
    axes[0, 1].set_ylabel('预测影响力')
    axes[0, 1].set_title(f'{model_name} - 测试集\nR² = {test_r2:.4f}, Kendall τ = {test_tau:.4f}')
    axes[0, 1].grid(True, alpha=0.3)
    
    # 误差分布直方图
    test_errors = test_preds - test_targets
    axes[1, 0].hist(test_errors, bins=50, alpha=0.7, color='green')
    axes[1, 0].set_xlabel('预测误差')
    axes[1, 0].set_ylabel('频次')
    axes[1, 0].set_title(f'{model_name} - 测试集误差分布')
    axes[1, 0].axvline(x=0, color='red', linestyle='--', alpha=0.7)
    axes[1, 0].grid(True, alpha=0.3)
    
    # 预测值分布对比
    axes[1, 1].hist(test_targets, bins=30, alpha=0.5, label='真实值', color='blue')
    axes[1, 1].hist(test_preds, bins=30, alpha=0.5, label='预测值', color='red')
    axes[1, 1].set_xlabel('影响力值')
    axes[1, 1].set_ylabel('频次')
    axes[1, 1].set_title(f'{model_name} - 测试集分布对比')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # 保存图表
    plot_path = os.path.join(output_dir, f'{model_name}_results.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"📊 结果图表已保存: {plot_path}")
    plt.close()

def save_detailed_results(all_results, output_dir):
    """保存详细结果到CSV"""
    summary_data = []
    
    for model_name, results in all_results.items():
        for split in ['train', 'test']:
            summary_data.append({
                'model': model_name,
                'split': split,
                'mse': results[split]['mse'],
                'r2': results[split]['r2'],
                'mae': results[split]['mae'],
                'kendall_tau': results[split]['kendall_tau'],
                'kendall_p': results[split]['kendall_p']
            })
    
    summary_df = pd.DataFrame(summary_data)
    summary_path = os.path.join(output_dir, 'model_comparison_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"📋 模型对比结果已保存: {summary_path}")
    
    return summary_df

def main():
    """主函数"""
    print("🔬 学长算法对比测试器")
    print("=" * 60)
    print("📊 目标：对比Bi-GRU、GLSTM和MLP在你的数据上的表现")
    print("🎯 数据：8个网络指标 → 影响力预测")
    print("🔄 处理：特征归一化 + 目标log变换")
    print("=" * 60)
    
    seed_everything(42)
    
    # 设置输出目录
    output_dir = 'C:/Tengfei/data/results/others_comparison'
    os.makedirs(output_dir, exist_ok=True)
    
    # 设备选择
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"🖥️ 使用设备: {device}")
    
    # 加载数据
    X, y, feature_columns, target_column = load_and_preprocess_data()
    if X is None:
        return
    
    # 创建数据加载器
    data_loaders = create_data_loaders(X, y, use_normalization=True)
    
    # 定义模型
    input_size = len(feature_columns)
    models = {
        'Bi-GRU': BiGRUModel(input_size=input_size),
        'GLSTM': GLSTMModel(input_size=input_size), 
        'MLP-Baseline': MLPBaseline(input_size=input_size)
    }
    
    print(f"\n🤖 将训练和测试 {len(models)} 个模型:")
    for name in models.keys():
        print(f"   - {name}")
    
    # 训练和评估所有模型
    all_results = {}
    
    for model_name, model in models.items():
        print(f"\n{'='*20} {model_name} {'='*20}")
        
        # 训练模型
        trained_model, train_losses, val_losses = train_model(
            model, data_loaders['train_loader'], data_loaders['val_loader'], 
            num_epochs=300, lr=0.001, device=device
        )
        
        # 评估模型
        results = evaluate_model(trained_model, data_loaders, device=device)
        all_results[model_name] = results
        
        # 显示结果
        print(f"📊 {model_name} 性能:")
        print(f"   训练集 - R²: {results['train']['r2']:.4f}, MAE: {results['train']['mae']:.2f}, Kendall τ: {results['train']['kendall_tau']:.4f}")
        print(f"   测试集 - R²: {results['test']['r2']:.4f}, MAE: {results['test']['mae']:.2f}, Kendall τ: {results['test']['kendall_tau']:.4f}")
        
        # 过拟合检查
        r2_gap = results['train']['r2'] - results['test']['r2']
        overfitting_level = "严重" if r2_gap > 0.2 else "轻微" if r2_gap > 0.1 else "正常"
        print(f"   过拟合检查: R²差异 = {r2_gap:.4f} ({overfitting_level})")
        
        # 生成可视化
        plot_results(results, model_name, output_dir)
    
    # 保存详细对比结果
    summary_df = save_detailed_results(all_results, output_dir)
    
    # 生成对比报告
    print(f"\n" + "="*60)
    print("🏆 模型性能对比总结")
    print("="*60)
    
    # 按测试集R²排序
    test_r2_ranking = [(name, results['test']['r2']) for name, results in all_results.items()]
    test_r2_ranking.sort(key=lambda x: x[1], reverse=True)
    
    print(f"📊 测试集R²排名:")
    for i, (name, r2) in enumerate(test_r2_ranking, 1):
        tau = all_results[name]['test']['kendall_tau']
        mae = all_results[name]['test']['mae']
        print(f"   {i}. {name}: R² = {r2:.4f}, Kendall τ = {tau:.4f}, MAE = {mae:.2f}")
    
    # 按Kendall tau排序
    kendall_ranking = [(name, results['test']['kendall_tau']) for name, results in all_results.items()]
    kendall_ranking.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\n📊 测试集Kendall τ排名:")
    for i, (name, tau) in enumerate(kendall_ranking, 1):
        r2 = all_results[name]['test']['r2']
        mae = all_results[name]['test']['mae']
        print(f"   {i}. {name}: Kendall τ = {tau:.4f}, R² = {r2:.4f}, MAE = {mae:.2f}")
    
    # 最佳模型分析
    best_r2_model = test_r2_ranking[0][0]
    best_tau_model = kendall_ranking[0][0]
    
    print(f"\n🎯 结论分析:")
    print(f"   📈 R²最佳模型: {best_r2_model}")
    print(f"   📈 Kendall τ最佳模型: {best_tau_model}")
    
    if best_r2_model == best_tau_model:
        print(f"   ✅ {best_r2_model} 在两项指标上都表现最佳！")
    else:
        print(f"   ⚠️ 不同指标显示不同的最佳模型，需要根据具体需求选择")
    
    # 与数据质量的关系分析
    zero_ratio = (y == 0).sum() / len(y) * 100
    print(f"\n💡 数据质量影响分析:")
    print(f"   📊 影响力为0的用户比例: {zero_ratio:.1f}%")
    
    best_model_r2 = all_results[best_r2_model]['test']['r2']
    if best_model_r2 < 0.1:
        print(f"   🚨 所有模型R²都很低，证实了数据质量问题的影响")
        print(f"   💡 建议：考虑从传播能力更强的用户开始爬取数据")
    elif best_model_r2 > 0.3:
        print(f"   ✅ 模型性能相对较好，说明网络指标确实有预测价值")
    else:
        print(f"   ⚠️ 模型性能中等，数据质量仍有改善空间")
    
    print(f"\n📁 所有结果已保存到: {output_dir}")
    print(f"   - 模型对比图表: *_results.png")
    print(f"   - 对比数据表: model_comparison_summary.csv")
    
    print(f"\n🎯 学长算法对比测试完成！")

if __name__ == "__main__":
    main()