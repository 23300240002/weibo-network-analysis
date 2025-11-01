import os
import sys
import pandas as pd
import numpy as np

def read_csv_with_fallback(path):
    encodings = ['utf-8', 'utf-8-sig', 'gb18030']
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc), enc
        except Exception as e:
            last_err = e
    raise last_err

def choose_column_interactively(df):
    print("\n可用列（前20个）:")
    cols = list(df.columns)
    preview = cols[:20]
    for i, c in enumerate(preview, 1):
        print(f"  {i}. {c}")
    col = input("\n请输入要分析的列名: ").strip()
    if col in df.columns:
        return col
    # 容错：大小写不敏感匹配
    lower_map = {c.lower(): c for c in df.columns}
    if col.lower() in lower_map:
        return lower_map[col.lower()]
    print(f"未找到列: {col}")
    # 推荐可能的列
    suggestions = [c for c in df.columns if col.lower() in c.lower()]
    if suggestions:
        print("你是否想找以下列之一：", ", ".join(suggestions))
    return None

def summarize_distribution(series, bins=10):
    series = pd.to_numeric(series, errors='coerce')
    total = len(series)
    non_null = series.notna().sum()
    nan_count = total - non_null
    zero_count = (series == 0).sum()
    zero_pct = zero_count / total * 100 if total > 0 else 0.0
    pos_count = (series > 0).sum()
    neg_count = (series < 0).sum()

    print("\n=== 基本信息 ===")
    print(f"总样本数: {total}")
    print(f"有效(非空)样本数: {non_null} ({non_null/total*100:.2f}%)" if total > 0 else f"有效(非空)样本数: {non_null}")
    print(f"空值/无法解析为数值: {nan_count} ({nan_count/total*100:.2f}%)" if total > 0 else f"空值/无法解析为数值: {nan_count}")
    print(f"为0的样本数: {zero_count} ({zero_pct:.2f}%)")
    print(f">0 的样本数: {pos_count} ({(pos_count/total*100 if total>0 else 0):.2f}%)")
    print(f"<0 的样本数: {neg_count} ({(neg_count/total*100 if total>0 else 0):.2f}%)")

    valid = series.dropna()
    if len(valid) == 0:
        print("\n没有可用于统计的有效数值数据。")
        return

    print("\n=== 描述性统计 ===")
    desc = valid.describe(percentiles=[0.25, 0.5, 0.75])
    # 追加更高分位数
    extra_q = valid.quantile([0.9, 0.95, 0.99])
    for k, v in desc.to_dict().items():
        print(f"{k}: {v:.4f}" if isinstance(v, (int, float, np.floating)) else f"{k}: {v}")
    for q, v in extra_q.to_dict().items():
        print(f"q{int(q*100)}: {v:.4f}")

    print("\n=== 值频次Top 10（离散/常见值）===")
    vc = valid.value_counts(dropna=False)
    head = vc.head(10)
    total_valid = len(valid)
    for val, cnt in head.items():
        pct = cnt / total_valid * 100
        val_str = f"{val:.4f}" if isinstance(val, (int, float, np.floating)) else str(val)
        print(f"{val_str}: {cnt} ({pct:.2f}%)")

    # 简易直方图分布
    print("\n=== 直方图式分布（等宽分箱）===")
    vmin, vmax = float(valid.min()), float(valid.max())
    if vmax == vmin:
        print(f"所有有效值相同: {vmin:.4f}")
        return

    try:
        counts, bin_edges = np.histogram(valid, bins=bins)
        total_counts = counts.sum()
        for i in range(len(counts)):
            left = bin_edges[i]
            right = bin_edges[i+1]
            cnt = int(counts[i])
            pct = cnt / total_counts * 100 if total_counts > 0 else 0.0
            # 右开区间用')'，最后一个箱右端闭合以保证覆盖最大值
            right_bracket = ']' if i == len(counts) - 1 else ')'
            print(f"[{left:.4f}, {right:.4f}{right_bracket}: {cnt} ({pct:.2f}%)")
    except Exception as e:
        print(f"直方图计算失败: {e}")

def main():
    print("列质量快速检测工具")
    print("==================")
    csv_path = input("请输入CSV文件完整路径（例如 C:/Tengfei/data/results/xxx.csv）: ").strip()
    if not csv_path:
        print("未输入路径，退出。")
        return
    if not os.path.exists(csv_path):
        print(f"文件不存在: {csv_path}")
        return

    try:
        df, enc = read_csv_with_fallback(csv_path)
        print(f"文件读取成功（编码: {enc}），共有 {len(df)} 行，{len(df.columns)} 列。")
    except Exception as e:
        print(f"读取CSV失败: {e}")
        return

    col_name = input("请输入要分析的列名（如 avg_popularity_of_all）: ").strip()
    if not col_name:
        col_name = choose_column_interactively(df)
        if not col_name:
            return
    elif col_name not in df.columns:
        # 大小写不敏感
        lower_map = {c.lower(): c for c in df.columns}
        if col_name.lower() in lower_map:
            col_name = lower_map[col_name.lower()]
        else:
            print(f"列名不存在: {col_name}")
            suggestions = [c for c in df.columns if col_name.lower() in c.lower()]
            if suggestions:
                print("你是否想找以下列之一：", ", ".join(suggestions))
            return

    print(f"\n开始分析列: {col_name}")
    summarize_distribution(df[col_name])

if __name__ == "__main__":
    main()