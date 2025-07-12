import csv
import json

def calculate_popularity(csv_path, jsonl_path):
    records = []
    
    with open(csv_path, 'r', encoding='gb18030') as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            user_id = row.get("MD5-作者ID", "").strip()
            is_retweet = row.get("原创/转发", "") == "转发"
            popularity = int((row.get("转", "").strip() or "0")) + int((row.get("赞", "").strip() or "0")) + int((row.get("评", "").strip() or "0"))
            record = {
                "user_id": user_id,
                "is_retweet": is_retweet, # 是否为转发
                "popularity": popularity
            }
            records.append(record)
    
    with open(jsonl_path, 'w', encoding='utf-8') as f_out:
        for record in records:
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    csv_file = "./data/《网络直播营销活动行为规范》7月1日实施.csv"
    output_file = "./results/popularity.jsonl"
    calculate_popularity(csv_file, output_file)
    print("流行度计算完成并保存到 JSONL 文件！")