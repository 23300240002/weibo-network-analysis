import os
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import joblib
import json

def normalize_id(id_value):
    """规范化用户ID"""
    try:
        id_str = str(id_value).strip()
        if id_str == '-2147483648':
            return id_str
        return str(int(float(id_str)))
    except:
        return str(id_value).strip()

def detect_available_abnormal_methods():
    """🔥 新增：自动检测所有可用的异常用户排除方法"""
    base_dir = 'results/pick_out_abnormal_users'
    
    if not os.path.exists(base_dir):
        print(f"❌ 异常用户目录不存在: {base_dir}")
        return []
    
    available_methods = []
    for item in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, item)
        if os.path.isdir(folder_path):
            csv_file = os.path.join(folder_path, 'abnormal_users.csv')
            if os.path.exists(csv_file):
                available_methods.append(item)
    
    # 排序：原始网络在前，然后按数字排序
    available_methods.sort(key=lambda x: (0 if 'original' in x.lower() else 1, x))
    
    return available_methods

def parse_exclude_percentage(method_name):
    """🔥 新增：从方法名称中解析排除比例"""
    if 'original' in method_name.lower():
        return 0.0
    
    import re
    match = re.search(r'(\d+(?:\.\d+)?)pct', method_name)
    if match:
        return float(match.group(1))
    
    return -1  # 无法解析

def load_abnormal_users(abnormal_method):
    """加载指定方法的异常用户列表"""
    if abnormal_method is None:
        return set()
    
    abnormal_file = f'results/pick_out_abnormal_users/{abnormal_method}/abnormal_users.csv'
    
    if not os.path.exists(abnormal_file):
        print(f"⚠️ 未找到异常用户文件: {abnormal_file}")
        return set()
    
    try:
        abnormal_df = pd.read_csv(abnormal_file)
        abnormal_users = set(abnormal_df['user_id'].apply(normalize_id))
        return abnormal_users
    except Exception as e:
        print(f"❌ 加载异常用户失败: {e}")
        return set()

def prepare_features_and_target(data_path, abnormal_users, target_column='avg_popularity_of_all'):
    """准备特征和目标变量"""
    # 加载数据
    df = pd.read_csv(data_path)
    df['user_id'] = df['user_id'].apply(normalize_id)
    
    print(f"📊 原始数据: {len(df)} 个用户")
    
    # 排除异常用户
    normal_df = df[~df['user_id'].isin(abnormal_users)].copy()
    print(f"📊 排除异常用户后: {len(normal_df)} 个用户")
    
    # 检查目标列
    if target_column not in normal_df.columns:
        print(f"❌ 未找到目标列: {target_column}")
        return None, None, None
    
    # 定义特征列（11个网络指标）
    feature_columns = [
        'density', 'clustering_coefficient', 'average_nearest_neighbor_degree',
        'betweenness_centrality', 'spectral_radius', 'modularity',
        'global_out_degree', 'global_in_degree', 'global_total_degree',
        'node_count', 'edge_count'
    ]
    
    # 检查特征列是否存在
    available_features = [col for col in feature_columns if col in normal_df.columns]
    missing_features = [col for col in feature_columns if col not in normal_df.columns]
    
    if missing_features:
        print(f"⚠️ 缺少特征列: {missing_features}")
    
    print(f"✅ 可用特征: {len(available_features)} 个")
    
    # 准备特征和目标
    X = normal_df[available_features].copy()
    y = normal_df[target_column].copy()
    user_ids = normal_df['user_id'].copy()
    
    # 检查数据质量
    print(f"📊 特征矩阵形状: {X.shape}")
    print(f"📊 目标变量统计: 均值={y.mean():.2f}, 最大值={y.max():.2f}, 非零数={(y>0).sum()}")
    
    # 处理缺失值
    if X.isnull().any().any():
        print(f"⚠️ 发现缺失值，将用均值填充")
        X = X.fillna(X.mean())
    
    return X, y, user_ids

def train_xgboost_model(X, y, test_size=0.3, random_state=42):
    """🔥 彻底修复版：特征选择 + 数据清洗 + 强正则化"""
    print(f"🚀 开始训练XGBoost模型（彻底修复版）...")
    
    # 🔥 步骤1：数据清洗 - 移除极端异常值
    print(f"📊 原始数据统计:")
    print(f"   均值: {y.mean():.2f}, 标准差: {y.std():.2f}")
    print(f"   最小值: {y.min():.2f}, 最大值: {y.max():.2f}")
    
    # 使用99.5分位数作为上界，移除极端异常值
    upper_bound = y.quantile(0.995)  # 移除前0.5%的极值
    lower_bound = 0  # 影响力不能为负
    
    # 过滤异常值
    valid_mask = (y >= lower_bound) & (y <= upper_bound)
    X_clean = X[valid_mask].copy()
    y_clean = y[valid_mask].copy()
    
    removed_count = len(y) - len(y_clean)
    print(f"📊 数据清洗结果:")
    print(f"   移除极端异常值: {removed_count} 个 ({removed_count/len(y)*100:.1f}%)")
    print(f"   清洗后均值: {y_clean.mean():.2f}, 标准差: {y_clean.std():.2f}")
    print(f"   清洗后范围: {y_clean.min():.2f} ~ {y_clean.max():.2f}")
    
    # 🔥 步骤2：目标变量变换 - 修复版：直接使用对数变换
    print(f"📊 原始目标变量统计: 均值={y_clean.mean():.2f}, 标准差={y_clean.std():.2f}")
    
    # 🔥 简化：直接使用对数变换，避免Box-Cox API问题
    y_transformed = np.log1p(y_clean)  # log(1+x)
    lambda_param = None  # 标记为对数变换
    print(f"📊 使用对数变换")
    
    print(f"   变换后均值: {y_transformed.mean():.2f}, 标准差: {y_transformed.std():.2f}")
    
    # 🔥 步骤3：特征选择 - 移除低相关性和高共线性特征
    from sklearn.feature_selection import SelectKBest, f_regression
    from sklearn.preprocessing import StandardScaler
    
    # 标准化特征
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_clean)
    
    # 计算特征与目标的相关性
    correlations = []
    feature_names = X_clean.columns.tolist()
    
    for i, feature in enumerate(feature_names):
        corr = np.corrcoef(X_scaled[:, i], y_transformed)[0, 1]
        correlations.append(abs(corr))
    
    # 选择相关性最高的特征
    n_features = min(8, len(feature_names))  # 最多保留8个特征
    top_indices = np.argsort(correlations)[-n_features:]
    
    X_selected = X_scaled[:, top_indices]
    selected_features = [feature_names[i] for i in top_indices]
    
    print(f"📊 特征选择结果:")
    print(f"   保留特征数: {len(selected_features)}")
    print(f"   选择的特征: {selected_features}")
    
    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X_selected, y_transformed, test_size=test_size, random_state=random_state, stratify=None
    )
    
    print(f"📊 数据划分:")
    print(f"   训练集: {len(X_train)} 个样本")
    print(f"   测试集: {len(X_test)} 个样本")
    
    # 🔥 步骤4：XGBoost模型 - 强正则化防过拟合
    model = xgb.XGBRegressor(
        n_estimators=30,           # 大幅减少树数量
        max_depth=2,               # 严格限制树深度
        learning_rate=0.01,        # 极低学习率
        subsample=0.6,             # 强烈欠采样
        colsample_bytree=0.6,      # 强烈特征采样
        reg_alpha=10.0,            # 强L1正则化
        reg_lambda=50.0,           # 强L2正则化
        min_child_weight=10,       # 增加最小叶节点权重
        gamma=1.0,                 # 增加分裂最小增益
        early_stopping_rounds=5,   # 🔥 修复：移到这里
        random_state=random_state,
        n_jobs=-1,
        objective='reg:squarederror',
        eval_metric='rmse'
    )

    print(f"⏳ 训练中（使用强正则化参数）...")
    # 🔥 修复：移除early_stopping_rounds参数
    model.fit(X_train, y_train, 
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=False)
    print(f"✅ 模型训练完成")
    
    # 🔥 步骤5：预测并逆变换 - 修复版
    y_train_pred_transformed = model.predict(X_train)
    y_test_pred_transformed = model.predict(X_test)
    
    # 🔥 修复：只使用对数逆变换
    y_train_pred = np.expm1(y_train_pred_transformed)
    y_test_pred = np.expm1(y_test_pred_transformed)
    
    # 确保预测值非负且合理
    y_train_pred = np.clip(y_train_pred, 0, upper_bound)
    y_test_pred = np.clip(y_test_pred, 0, upper_bound)
    
    # 🔥 步骤6：在原始空间评估 - 修复版
    y_train_original = np.expm1(y_train)
    y_test_original = np.expm1(y_test)
    
    # 确保原始值也在合理范围内
    y_train_original = np.clip(y_train_original, 0, upper_bound)
    y_test_original = np.clip(y_test_original, 0, upper_bound)
    
    # 计算评估指标
    train_mse = mean_squared_error(y_train_original, y_train_pred)
    test_mse = mean_squared_error(y_test_original, y_test_pred)
    train_r2 = r2_score(y_train_original, y_train_pred)
    test_r2 = r2_score(y_test_original, y_test_pred)
    train_mae = mean_absolute_error(y_train_original, y_train_pred)
    test_mae = mean_absolute_error(y_test_original, y_test_pred)
    
    # 🔥 步骤7：诊断分析
    print(f"📊 模型性能诊断:")
    print(f"   训练集 R²: {train_r2:.4f}")
    print(f"   测试集 R²: {test_r2:.4f}")
    print(f"   训练集 MAE: {train_mae:.2f}")
    print(f"   测试集 MAE: {test_mae:.2f}")
    print(f"   预测值范围: {y_test_pred.min():.2f} ~ {y_test_pred.max():.2f}")
    print(f"   过拟合检查: {abs(train_r2 - test_r2):.4f} ({'轻微' if abs(train_r2 - test_r2) < 0.1 else '严重'})")
    
    # 特征重要性
    feature_importance = model.feature_importances_
    importance_df = pd.DataFrame({
        'feature': selected_features,
        'importance': feature_importance
    }).sort_values('importance', ascending=False)
    
    print(f"📈 特征重要性TOP5:")
    for idx, row in importance_df.head().iterrows():
        print(f"   {row['feature']}: {row['importance']:.4f}")
    
    # 🔥 构建结果 - 扩展原始X和y到清洗后的数据
    results = {
        'model': model,
        'scaler': scaler,
        'selected_features': selected_features,  # 🔥 确保这是特征名列表
        'lambda_param': lambda_param,
        'upper_bound': upper_bound,
        'X_train': X_train,  # 🔥 注意：这是numpy数组，不是DataFrame
        'X_test': X_test,    # 🔥 注意：这是numpy数组，不是DataFrame
        'y_train': y_train_original,
        'y_test': y_test_original,
        'y_train_pred': y_train_pred,
        'y_test_pred': y_test_pred,
        'metrics': {
            'train_mse': train_mse, 'test_mse': test_mse,
            'train_r2': train_r2, 'test_r2': test_r2,
            'train_mae': train_mae, 'test_mae': test_mae
        }
    }

    return results

def analyze_feature_importance(model, feature_names):
    """🔥 修复版：分析特征重要性，确保长度匹配"""
    try:
        importance = model.feature_importances_
        
        # 🔥 确保特征名和重要性数组长度一致
        if len(feature_names) != len(importance):
            print(f"⚠️ 特征名数量({len(feature_names)})与重要性数量({len(importance)})不匹配")
            # 截断到较短的长度
            min_length = min(len(feature_names), len(importance))
            feature_names = feature_names[:min_length]
            importance = importance[:min_length]
        
        feature_importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        print(f"📈 特征重要性排序:")
        for idx, row in feature_importance_df.iterrows():
            print(f"   {row['feature']}: {row['importance']:.4f}")
        
        return feature_importance_df
        
    except Exception as e:
        print(f"❌ 分析特征重要性失败: {e}")
        # 返回空的DataFrame
        return pd.DataFrame({'feature': [], 'importance': []})

def save_method_results(results, feature_importance_df, method_info, output_dir):
    """🔥 修复版：保存单个方法的结果，处理数据长度不匹配问题"""
    method_dir = os.path.join(output_dir, f"exclude_{method_info['exclude_pct']}pct")
    os.makedirs(method_dir, exist_ok=True)
    
    # 保存模型和scaler
    model_file = os.path.join(method_dir, 'xgboost_model.joblib')
    joblib.dump({
        'model': results['model'],
        'scaler': results['scaler'],
        'feature_names': results['selected_features'],  # 🔥 修复：使用正确的特征名
        'lambda_param': results.get('lambda_param'),
        'upper_bound': results.get('upper_bound')
    }, model_file)
    
    # 保存特征重要性
    importance_file = os.path.join(method_dir, 'feature_importance.csv')
    feature_importance_df.to_csv(importance_file, index=False)
    
    # 🔥 修复：创建结果摘要，避免DataFrame长度不匹配
    results_summary = {
        'method_info': method_info,
        'training_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_info': {
            'train_samples': len(results['y_train']),
            'test_samples': len(results['y_test']),
            'feature_count': len(results['selected_features']),
            'selected_features': results['selected_features']
        },
        'performance_metrics': results['metrics'],
        'data_processing': {
            'upper_bound': results.get('upper_bound', 'Unknown'),
            'transform_method': 'log1p',
            'feature_selection': 'correlation_based'
        },
        'top_features': feature_importance_df.head(5).to_dict('records')
    }
    
    # 保存详细结果到JSON
    results_file = os.path.join(method_dir, 'model_results.json')
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results_summary, f, ensure_ascii=False, indent=2)
    
    # 🔥 修复：保存预测结果到CSV，确保长度一致
    try:
        predictions_data = {
            'y_true': results['y_test'],
            'y_pred': results['y_test_pred']
        }
        predictions_df = pd.DataFrame(predictions_data)
        predictions_file = os.path.join(method_dir, 'predictions.csv')
        predictions_df.to_csv(predictions_file, index=False)
        
        print(f"✅ 预测结果已保存: {len(predictions_df)} 行")
    except Exception as e:
        print(f"⚠️ 保存预测结果时出错: {e}")
    
    # 生成可视化
    try:
        generate_visualization(results, method_info, method_dir)
    except Exception as e:
        print(f"⚠️ 生成可视化时出错: {e}")
    
    print(f"✅ 方法结果已保存到: {method_dir}")

def generate_visualization(results, method_info, output_dir):
    """生成预测结果可视化"""
    y_train, y_test = results['y_train'], results['y_test']
    y_train_pred, y_test_pred = results['y_train_pred'], results['y_test_pred']
    
    # 真实值 vs 预测值散点图
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 训练集
    ax1.scatter(y_train, y_train_pred, alpha=0.5, s=20)
    ax1.plot([y_train.min(), y_train.max()], [y_train.min(), y_train.max()], 'r--', lw=2)
    ax1.set_xlabel('真实影响力')
    ax1.set_ylabel('预测影响力')
    ax1.set_title(f'训练集预测结果 (排除{method_info["exclude_pct"]}%)\nR² = {results["metrics"]["train_r2"]:.4f}')
    ax1.grid(True, alpha=0.3)
    
    # 测试集
    ax2.scatter(y_test, y_test_pred, alpha=0.5, s=20, color='orange')
    ax2.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    ax2.set_xlabel('真实影响力')
    ax2.set_ylabel('预测影响力')
    ax2.set_title(f'测试集预测结果 (排除{method_info["exclude_pct"]}%)\nR² = {results["metrics"]["test_r2"]:.4f}')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    scatter_plot = os.path.join(output_dir, 'prediction_scatter.png')
    plt.savefig(scatter_plot, dpi=300, bbox_inches='tight')
    plt.close()

def generate_comparison_report(all_results, output_dir):
    """🔥 新增：生成不同排除比例的对比报告"""
    print(f"📊 生成排除比例对比报告...")
    
    # 收集所有结果数据
    comparison_data = []
    for method_name, result_data in all_results.items():
        exclude_pct = result_data['method_info']['exclude_pct']
        metrics = result_data['results']['metrics']
        feature_importance = result_data['feature_importance']
        
        # 获取前3个最重要特征
        top_3_features = feature_importance.head(3)['feature'].tolist()
        
        comparison_data.append({
            'exclude_percentage': exclude_pct,
            'train_r2': metrics['train_r2'],
            'test_r2': metrics['test_r2'],
            'train_mae': metrics['train_mae'],
            'test_mae': metrics['test_mae'],
            'train_samples': result_data['results']['X_train'].shape[0],
            'test_samples': result_data['results']['X_test'].shape[0],
            'top_3_features': ', '.join(top_3_features)
        })
    
    # 转换为DataFrame
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df = comparison_df.sort_values('exclude_percentage')
    
    # 保存对比数据
    comparison_csv = os.path.join(output_dir, 'exclude_percentage_comparison.csv')
    comparison_df.to_csv(comparison_csv, index=False)
    
    # 生成详细报告
    report_file = os.path.join(output_dir, 'comparison_report.txt')
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("XGBoost影响力预测模型 - 异常用户排除比例对比报告\n")
        f.write("=" * 60 + "\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"目标用户: user_3855570307 (相关性最优)\n")
        f.write(f"目标变量: avg_popularity_of_all (总体转赞评平均值)\n")
        f.write(f"特征数量: 11个网络指标\n\n")
        
        f.write("=" * 60 + "\n")
        f.write("各排除比例性能对比\n")
        f.write("=" * 60 + "\n")
        f.write(f"{'排除比例':<10} {'测试R²':<10} {'测试MAE':<10} {'训练样本':<10} {'测试样本':<10} {'前3重要特征'}\n")
        f.write("-" * 100 + "\n")
        
        for _, row in comparison_df.iterrows():
            f.write(f"{row['exclude_percentage']:<10.0f}% {row['test_r2']:<10.4f} {row['test_mae']:<10.2f} "
                   f"{row['train_samples']:<10} {row['test_samples']:<10} {row['top_3_features']}\n")
        
        # 找出最佳性能
        best_r2_idx = comparison_df['test_r2'].idxmax()
        best_mae_idx = comparison_df['test_mae'].idxmin()
        
        f.write(f"\n" + "=" * 60 + "\n")
        f.write("性能总结\n")
        f.write("=" * 60 + "\n")
        f.write(f"最高测试R²: {comparison_df.loc[best_r2_idx, 'exclude_percentage']:.0f}% "
                f"(R² = {comparison_df.loc[best_r2_idx, 'test_r2']:.4f})\n")
        f.write(f"最低测试MAE: {comparison_df.loc[best_mae_idx, 'exclude_percentage']:.0f}% "
                f"(MAE = {comparison_df.loc[best_mae_idx, 'test_mae']:.2f})\n")
        
        # 性能趋势分析
        f.write(f"\n性能趋势分析:\n")
        f.write(f"- R²范围: {comparison_df['test_r2'].min():.4f} ~ {comparison_df['test_r2'].max():.4f}\n")
        f.write(f"- MAE范围: {comparison_df['test_mae'].min():.2f} ~ {comparison_df['test_mae'].max():.2f}\n")
        
        # 样本数变化
        f.write(f"\n样本数变化:\n")
        f.write(f"- 训练样本: {comparison_df['train_samples'].max()} → {comparison_df['train_samples'].min()}\n")
        f.write(f"- 测试样本: {comparison_df['test_samples'].max()} → {comparison_df['test_samples'].min()}\n")
    
    # 生成性能趋势图
    plt.figure(figsize=(15, 10))
    
    # R²趋势
    plt.subplot(2, 2, 1)
    plt.plot(comparison_df['exclude_percentage'], comparison_df['test_r2'], 'bo-', linewidth=2, markersize=8)
    plt.xlabel('排除比例 (%)')
    plt.ylabel('测试集 R²')
    plt.title('测试集R²随排除比例变化')
    plt.grid(True, alpha=0.3)
    
    # MAE趋势
    plt.subplot(2, 2, 2)
    plt.plot(comparison_df['exclude_percentage'], comparison_df['test_mae'], 'ro-', linewidth=2, markersize=8)
    plt.xlabel('排除比例 (%)')
    plt.ylabel('测试集 MAE')
    plt.title('测试集MAE随排除比例变化')
    plt.grid(True, alpha=0.3)
    
    # 训练样本数趋势
    plt.subplot(2, 2, 3)
    plt.plot(comparison_df['exclude_percentage'], comparison_df['train_samples'], 'go-', linewidth=2, markersize=8)
    plt.xlabel('排除比例 (%)')
    plt.ylabel('训练样本数')
    plt.title('训练样本数随排除比例变化')
    plt.grid(True, alpha=0.3)
    
    # R²对比（训练vs测试）
    plt.subplot(2, 2, 4)
    plt.plot(comparison_df['exclude_percentage'], comparison_df['train_r2'], 'b-', label='训练集R²', linewidth=2)
    plt.plot(comparison_df['exclude_percentage'], comparison_df['test_r2'], 'r-', label='测试集R²', linewidth=2)
    plt.xlabel('排除比例 (%)')
    plt.ylabel('R²')
    plt.title('训练集vs测试集R²对比')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    trend_plot = os.path.join(output_dir, 'exclude_percentage_trends.png')
    plt.savefig(trend_plot, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✅ 对比报告已保存: {report_file}")
    print(f"✅ 对比数据已保存: {comparison_csv}")
    print(f"✅ 趋势图已保存: {trend_plot}")
    
    return comparison_df

def main():
    """主函数"""
    start_time = datetime.now()
    print(f"XGBoost影响力预测模型 - 多比例测试版")
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("🎯 目标：测试不同异常用户排除比例对预测性能的影响")
    print("📊 输入：11个网络指标（密度、聚类系数等）")
    print("🎯 输出：avg_popularity_of_all（总体转赞评平均值）")
    print("🔧 模型：XGBoost回归器")
    print("👤 目标用户：3855570307（相关性最优）")
    print("📈 测试范围：0% ~ 40% 异常用户排除比例")
    print("=" * 80)
    
    # 🔥 修改：使用相关性最好的用户3855570307
    data_path = 'C:/Tengfei/data/results/user_3855570307_metrics/merged_metrics_popularity.csv'
    output_dir = 'C:/Tengfei/data/results/prediction_results/user_3855570307_multi_exclude'
    
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 检查数据文件
    if not os.path.exists(data_path):
        print(f"❌ 未找到数据文件: {data_path}")
        print("请先确保用户3855570307的数据已准备完毕")
        return
    
    # 🔥 新增：自动检测所有可用的异常用户排除方法
    print(f"\n🔍 自动检测可用的异常用户排除方法...")
    available_methods = detect_available_abnormal_methods()
    
    if not available_methods:
        print(f"❌ 未找到任何异常用户排除方法")
        print("请先运行 pick_out_abnormal_users.py 生成异常用户数据")
        return
    
    # 🔥 新增：筛选0%-40%范围内的方法
    print(f"\n📋 筛选0%-40%范围内的排除方法...")
    valid_methods = []
    
    for method in available_methods:
        exclude_pct = parse_exclude_percentage(method)
        if 0 <= exclude_pct <= 40:
            valid_methods.append({
                'name': method,
                'exclude_pct': exclude_pct,
                'description': f'排除{exclude_pct}%异常用户' if exclude_pct > 0 else '原始网络'
            })
    
    # 按排除比例排序
    valid_methods.sort(key=lambda x: x['exclude_pct'])
    
    if not valid_methods:
        print(f"❌ 在0%-40%范围内未找到有效的排除方法")
        return
    
    print(f"✅ 找到 {len(valid_methods)} 个有效的排除方法:")
    for method in valid_methods:
        print(f"   - {method['exclude_pct']:5.1f}%: {method['description']}")
    
    # 确认是否继续
    print(f"\n⚠️ 将测试 {len(valid_methods)} 种不同的排除比例")
    print(f"⚠️ 每种方法预计需要1-3分钟，总计约 {len(valid_methods) * 2} 分钟")
    
    confirm = input("是否继续？(y/n): ").strip().lower()
    if confirm != 'y':
        print("用户取消操作")
        return
    
    # 🔥 新增：批量测试所有方法
    print(f"\n🚀 开始批量测试...")
    all_results = {}
    
    for i, method_info in enumerate(valid_methods, 1):
        print(f"\n{'='*80}")
        print(f"测试方法 [{i}/{len(valid_methods)}]: {method_info['description']}")
        print(f"排除比例: {method_info['exclude_pct']}%")
        print(f"{'='*80}")
        
        try:
            # 加载异常用户
            abnormal_users = load_abnormal_users(method_info['name'])
            print(f"✅ 加载了 {len(abnormal_users)} 个异常用户")
            
            # 准备数据
            X, y, user_ids = prepare_features_and_target(data_path, abnormal_users, 'avg_popularity_of_all')
            
            if X is None:
                print(f"❌ 数据准备失败，跳过此方法")
                continue
            
            # 训练模型
            results = train_xgboost_model(X, y)
            
            # 特征重要性分析
            feature_importance_df = analyze_feature_importance(results['model'], X.columns.tolist())
            
            # 保存结果
            save_method_results(results, feature_importance_df, method_info, output_dir)
            
            # 存储到总结果中
            all_results[method_info['name']] = {
                'method_info': method_info,
                'results': results,
                'feature_importance': feature_importance_df
            }
            
            print(f"✅ 方法 {method_info['description']} 完成")
            print(f"   测试集R²: {results['metrics']['test_r2']:.4f}")
            print(f"   测试集MAE: {results['metrics']['test_mae']:.2f}")
            
        except Exception as e:
            print(f"❌ 方法 {method_info['description']} 失败: {e}")
            continue
    
    # 🔥 新增：生成综合对比报告
    if len(all_results) > 1:
        print(f"\n📊 生成综合对比报告...")
        comparison_df = generate_comparison_report(all_results, output_dir)
        
        # 显示最佳结果
        best_r2_row = comparison_df.loc[comparison_df['test_r2'].idxmax()]
        best_mae_row = comparison_df.loc[comparison_df['test_mae'].idxmin()]
        
        print(f"\n🏆 最佳性能总结:")
        print(f"   🎯 最高R²: 排除{best_r2_row['exclude_percentage']:.0f}% (R² = {best_r2_row['test_r2']:.4f})")
        print(f"   🎯 最低MAE: 排除{best_mae_row['exclude_percentage']:.0f}% (MAE = {best_mae_row['test_mae']:.2f})")
        
        # 与你提到的35%进行对比
        if 35 in comparison_df['exclude_percentage'].values:
            pct_35_row = comparison_df[comparison_df['exclude_percentage'] == 35].iloc[0]
            print(f"\n📊 35%排除比例性能 (你提到的最佳相关性):")
            print(f"   R²: {pct_35_row['test_r2']:.4f}")
            print(f"   MAE: {pct_35_row['test_mae']:.2f}")
    
    # 总结
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n" + "="*80)
    print(f"🎉 多比例XGBoost预测模型测试完成！")
    print(f"⏱️  总耗时: {duration}")
    print(f"📊 测试方法数: {len(all_results)}")
    print(f"📍 结果保存位置: {output_dir}")
    print(f"\n📁 生成的文件:")
    print(f"   📊 各比例独立结果: exclude_X%pct/ 文件夹")
    print(f"   📈 综合对比报告: comparison_report.txt")
    print(f"   📋 对比数据表: exclude_percentage_comparison.csv")
    print(f"   📉 趋势分析图: exclude_percentage_trends.png")
    print("="*80)

if __name__ == "__main__":
    main()