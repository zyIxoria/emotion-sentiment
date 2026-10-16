#!/usr/bin/env python3
"""
Script: collect_balanced_reviews.py
Mục đích:
  1. Kiểm tra và xử lý dữ liệu từ nguồn Amazon Reviews 2023 - All_Beauty (701.528 records).
  2. Lọc bỏ bản ghi lỗi JSON, thiếu text (<5 ký tự), trùng lặp (dựa trên SHA-256 stable review_id).
  3. Gán nhãn proxy cảm xúc:
     - Rating 1, 2 -> negative (label 2)
     - Rating 3    -> neutral  (label 1)
     - Rating 4, 5 -> positive (label 0)
  4. Tạo 2 bộ dữ liệu:
     - Dataset tự nhiên (Natural Dataset): 100.000 reviews phản ánh đúng phân phối thực tế.
       * Tách riêng tập Test tự nhiên (20.000 reviews) - KHÔNG bị can thiệp phân phối.
       * Tập Train tự nhiên (80.000 reviews).
     - Dataset huấn luyện cân bằng (Balanced Training Dataset):
       * Lấy mẫu cân bằng từ các review có thật (không tạo dữ liệu giả, không lấy từ tập test).
       * Thu thập 15.000 mẫu mỗi lớp = 45.000 mẫu (15K Negative, 15K Neutral, 15K Positive).
  5. Đồng bộ và kiểm tra trên Hadoop HDFS.
"""
import os
import sys
import json
import time
import hashlib
import random
import subprocess
from collections import Counter

# File paths
RAW_FILE = "/mnt/d/Filehoc/big-data/emotion-sentiment/data/raw/All_Beauty_full.jsonl"
WINDOWS_RAW = "data/raw/All_Beauty_full.jsonl"

PROCESSED_DIR = "/mnt/d/Filehoc/big-data/emotion-sentiment/data/processed"
WINDOWS_PROCESSED = "data/processed"

METRICS_JSON = "/mnt/d/Filehoc/big-data/emotion-sentiment/reports/dataset/collection_metrics.json"

TARGET_NATURAL = 100000
TARGET_TEST = 20000
TARGET_BALANCED_PER_CLASS = 15000  # 15.000 x 3 = 45.000 reviews

def get_raw_path():
    if os.path.exists(RAW_FILE):
        return RAW_FILE
    if os.path.exists(WINDOWS_RAW):
        return WINDOWS_RAW
    raise FileNotFoundError("Không tìm thấy file All_Beauty_full.jsonl")

def get_processed_dir():
    if os.path.exists("/mnt/d/Filehoc/big-data/emotion-sentiment"):
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        return PROCESSED_DIR
    os.makedirs(WINDOWS_PROCESSED, exist_ok=True)
    return WINDOWS_PROCESSED

def generate_stable_id(rec, full_text):
    user_id = str(rec.get("user_id", "") or "")
    asin = str(rec.get("asin", "") or "")
    timestamp = str(rec.get("timestamp", "") or "")
    key = f"{user_id}_{asin}_{timestamp}_{full_text}".encode('utf-8')
    return hashlib.sha256(key).hexdigest()[:16]

def main():
    print("=" * 70)
    print(" BẮT ĐẦU THU THẬP & XỬ LÝ DỮ LIỆU PHASE 2.1 — ALL_BEAUTY")
    print("=" * 70)
    
    raw_path = get_raw_path()
    processed_dir = get_processed_dir()
    
    out_natural_100k = os.path.join(processed_dir, "all_beauty_100k_natural.jsonl")
    out_natural_test = os.path.join(processed_dir, "all_beauty_natural_test.jsonl")
    out_natural_train = os.path.join(processed_dir, "all_beauty_natural_train.jsonl")
    out_balanced_train = os.path.join(processed_dir, "all_beauty_balanced_train.jsonl")
    
    start_time = time.time()
    
    # Bộ đếm chất lượng và thống kê
    total_lines = 0
    json_errors = 0
    missing_text_errors = 0
    invalid_rating_errors = 0
    duplicate_errors = 0
    
    star_counter_all = Counter()
    sentiment_counter_all = Counter()
    
    seen_ids = set()
    
    # Danh sách phục vụ tạo tập Natural 100k
    natural_100k = []
    
    # Bể chứa ứng viên cho tập Balanced Train (loại trừ các review thuộc tập Test)
    # Chia theo 3 lớp: 'positive', 'neutral', 'negative'
    train_pool = {
        'positive': [],
        'neutral': [],
        'negative': []
    }
    
    print(f"Nguồn dữ liệu: {raw_path}")
    print("Đang quét và làm sạch dữ liệu theo stream...")
    
    with open(raw_path, 'r', encoding='utf-8') as f:
        for line_idx, line in enumerate(f, 1):
            total_lines += 1
            line = line.strip()
            if not line:
                continue
                
            try:
                rec = json.loads(line)
            except Exception:
                json_errors += 1
                continue
                
            # Kiểm tra rating
            try:
                rating = float(rec.get("rating", 0))
                if rating not in (1.0, 2.0, 3.0, 4.0, 5.0):
                    invalid_rating_errors += 1
                    continue
            except (ValueError, TypeError):
                invalid_rating_errors += 1
                continue
                
            # Kiểm tra text
            title = str(rec.get("title", "") or "").strip()
            text = str(rec.get("text", "") or "").strip()
            full_text = f"{title} {text}".strip()
            
            if len(full_text) < 5:
                missing_text_errors += 1
                continue
                
            # Tạo Stable ID & Kiểm tra trùng lặp
            review_id = generate_stable_id(rec, full_text)
            if review_id in seen_ids:
                duplicate_errors += 1
                continue
            seen_ids.add(review_id)
            
            # Gán nhãn proxy sentiment
            int_rating = int(rating)
            if int_rating >= 4:
                sentiment = "positive"
                numeric_label = 0
            elif int_rating == 3:
                sentiment = "neutral"
                numeric_label = 1
            else:
                sentiment = "negative"
                numeric_label = 2
                
            star_counter_all[int_rating] += 1
            sentiment_counter_all[sentiment] += 1
            
            # Đóng gói bản ghi chuẩn hóa
            clean_rec = {
                "review_id": review_id,
                "user_id": rec.get("user_id", ""),
                "asin": rec.get("asin", ""),
                "rating": int_rating,
                "sentiment": sentiment,
                "label": numeric_label,
                "title": title,
                "text": text,
                "full_text": full_text,
                "timestamp": rec.get("timestamp", 0),
                "helpful_vote": rec.get("helpful_vote", 0),
                "verified_purchase": rec.get("verified_purchase", False)
            }
            
            # Phân bổ vào tập dữ liệu
            if len(natural_100k) < TARGET_NATURAL:
                natural_100k.append(clean_rec)
            else:
                # Sau khi đã đủ 100k tập tự nhiên, các review tiếp theo trong file
                # sẽ được nạp vào training candidate pool để tìm thêm neutral & negative
                train_pool[sentiment].append(clean_rec)
                
            if total_lines % 100000 == 0:
                print(f"  Đã quét {total_lines:,} dòng | Hợp lệ duy nhất: {len(seen_ids):,} | Natural: {len(natural_100k):,}")
                
    valid_unique_count = len(seen_ids)
    scan_time = time.time() - start_time
    print(f"\n✓ Hoàn thành quét file trong {scan_time:.2f}s!")
    print(f"  - Tổng số dòng quét      : {total_lines:,}")
    print(f"  - Lỗi JSON                : {json_errors:,}")
    print(f"  - Thiếu text (<5 ký tự)  : {missing_text_errors:,}")
    print(f"  - Rating không hợp lệ    : {invalid_rating_errors:,}")
    print(f"  - Bản ghi trùng lặp       : {duplicate_errors:,}")
    print(f"  - Bản ghi hợp lệ & duy nhất: {valid_unique_count:,}")
    
    # -------------------------------------------------------------
    # 1. Phân chia tập Natural 100k thành Test (20k) và Train (80k)
    # -------------------------------------------------------------
    # Cố định seed ngẫu nhiên để tái hiện hoàn toàn
    random.seed(42)
    shuffled_natural = list(natural_100k)
    random.shuffle(shuffled_natural)
    
    test_set = shuffled_natural[:TARGET_TEST]
    natural_train_set = shuffled_natural[TARGET_TEST:]
    
    # Đưa các review trong natural_train_set vào train_pool
    for r in natural_train_set:
        train_pool[r['sentiment']].append(r)
        
    print("\n" + "=" * 50)
    print(" THỐNG KÊ TẬP ỨNG VIÊN HUẤN LUYỆN (NGOÀI TẬP TEST)")
    print("=" * 50)
    for s in ['positive', 'neutral', 'negative']:
        print(f"  - Ứng viên {s:<8}: {len(train_pool[s]):,} mẫu có thật")
        
    # -------------------------------------------------------------
    # 2. Lấy mẫu Balanced Training Set từ train_pool
    # -------------------------------------------------------------
    # Xác định số lượng mẫu cân bằng tối đa có thể lấy
    max_balanced = min(len(train_pool['positive']), len(train_pool['neutral']), len(train_pool['negative']))
    actual_balanced_per_class = min(TARGET_BALANCED_PER_CLASS, max_balanced)
    
    print(f"\n-> Mục tiêu cân bằng: {TARGET_BALANCED_PER_CLASS:,} mẫu/lớp.")
    print(f"-> Khả năng thực tế : {max_balanced:,} mẫu/lớp.")
    print(f"-> Quyết định lấy   : {actual_balanced_per_class:,} mẫu/lớp (Tổng: {actual_balanced_per_class * 3:,} mẫu)")
    
    balanced_train_set = []
    for s in ['positive', 'neutral', 'negative']:
        samples = random.sample(train_pool[s], actual_balanced_per_class)
        balanced_train_set.extend(samples)
    random.shuffle(balanced_train_set)
    
    # -------------------------------------------------------------
    # 3. Ghi các file kết quả
    # -------------------------------------------------------------
    print("\nĐang xuất các file dữ liệu...")
    
    # Ghi 100k Natural
    with open(out_natural_100k, 'w', encoding='utf-8') as f:
        for r in natural_100k:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    print(f"✓ Ghi {len(natural_100k):,} reviews vào {out_natural_100k}")
    
    # Ghi 20k Natural Test
    with open(out_natural_test, 'w', encoding='utf-8') as f:
        for r in test_set:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    print(f"✓ Ghi {len(test_set):,} reviews vào {out_natural_test}")
    
    # Ghi 80k Natural Train
    with open(out_natural_train, 'w', encoding='utf-8') as f:
        for r in natural_train_set:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    print(f"✓ Ghi {len(natural_train_set):,} reviews vào {out_natural_train}")
    
    # Ghi Balanced Train
    with open(out_balanced_train, 'w', encoding='utf-8') as f:
        for r in balanced_train_set:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    print(f"✓ Ghi {len(balanced_train_set):,} reviews vào {out_balanced_train}")
    
    # -------------------------------------------------------------
    # 4. Tính toán Metrics chi tiết
    # -------------------------------------------------------------
    def get_distribution(dataset):
        star_c = Counter(r['rating'] for r in dataset)
        sent_c = Counter(r['sentiment'] for r in dataset)
        n = len(dataset)
        return {
            "total": n,
            "stars": {s: star_c.get(s, 0) for s in [1, 2, 3, 4, 5]},
            "stars_pct": {s: round(star_c.get(s, 0) / n * 100, 2) if n else 0 for s in [1, 2, 3, 4, 5]},
            "sentiment": {s: sent_c.get(s, 0) for s in ['positive', 'neutral', 'negative']},
            "sentiment_pct": {s: round(sent_c.get(s, 0) / n * 100, 2) if n else 0 for s in ['positive', 'neutral', 'negative']}
        }
        
    metrics = {
        "execution_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "raw_source": "All_Beauty.jsonl (McAuley-Lab/Amazon-Reviews-2023)",
        "cleaning": {
            "total_lines_scanned": total_lines,
            "json_errors": json_errors,
            "missing_text_errors": missing_text_errors,
            "invalid_rating_errors": invalid_rating_errors,
            "duplicate_errors": duplicate_errors,
            "valid_unique_records": valid_unique_count
        },
        "full_dataset_distribution": get_distribution([{"rating": k, "sentiment": "positive" if k>=4 else ("neutral" if k==3 else "negative")} for k, v in star_counter_all.items() for _ in range(v)]),
        "natural_100k_distribution": get_distribution(natural_100k),
        "natural_test_20k_distribution": get_distribution(test_set),
        "natural_train_80k_distribution": get_distribution(natural_train_set),
        "balanced_train_distribution": get_distribution(balanced_train_set),
        "train_pool_available": {
            "positive": len(train_pool['positive']),
            "neutral": len(train_pool['neutral']),
            "negative": len(train_pool['negative'])
        }
    }
    
    # Điều chỉnh full_dataset_distribution để chính xác từ bộ đếm
    metrics["full_dataset_distribution"] = {
        "total": valid_unique_count,
        "stars": {s: star_counter_all.get(s, 0) for s in [1, 2, 3, 4, 5]},
        "stars_pct": {s: round(star_counter_all.get(s, 0) / valid_unique_count * 100, 2) for s in [1, 2, 3, 4, 5]},
        "sentiment": {s: sentiment_counter_all.get(s, 0) for s in ['positive', 'neutral', 'negative']},
        "sentiment_pct": {s: round(sentiment_counter_all.get(s, 0) / valid_unique_count * 100, 2) for s in ['positive', 'neutral', 'negative']}
    }
    
    os.makedirs(os.path.dirname(METRICS_JSON), exist_ok=True)
    with open(METRICS_JSON, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"✓ Thống kê metrics đã lưu tại {METRICS_JSON}")
    
    print("\n" + "=" * 70)
    print(" KẾT QUẢ PHÂN PHỐI LỚP (TỔNG KẾT)")
    print("=" * 70)
    print("\n[A] Toàn bộ dữ liệu hợp lệ (Full Cleaned Dataset - 100%):")
    for s in ['positive', 'neutral', 'negative']:
        c = metrics["full_dataset_distribution"]["sentiment"][s]
        p = metrics["full_dataset_distribution"]["sentiment_pct"][s]
        print(f"  - {s.capitalize():<8}: {c:>8,} ({p:>5.2f}%)")
        
    print("\n[B] Tập tự nhiên 100k (Natural 100k):")
    for s in ['positive', 'neutral', 'negative']:
        c = metrics["natural_100k_distribution"]["sentiment"][s]
        p = metrics["natural_100k_distribution"]["sentiment_pct"][s]
        print(f"  - {s.capitalize():<8}: {c:>8,} ({p:>5.2f}%)")

    print("\n[C] Tập kiểm thử tự nhiên 20k (Test Set - Strictly Natural):")
    for s in ['positive', 'neutral', 'negative']:
        c = metrics["natural_test_20k_distribution"]["sentiment"][s]
        p = metrics["natural_test_20k_distribution"]["sentiment_pct"][s]
        print(f"  - {s.capitalize():<8}: {c:>8,} ({p:>5.2f}%)")

    print("\n[D] Tập huấn luyện cân bằng (Balanced Train Set - 100% Real Samples):")
    for s in ['positive', 'neutral', 'negative']:
        c = metrics["balanced_train_distribution"]["sentiment"][s]
        p = metrics["balanced_train_distribution"]["sentiment_pct"][s]
        print(f"  - {s.capitalize():<8}: {c:>8,} ({p:>5.2f}%)")

    print("\n" + "=" * 70)
    print(" THU THẬP & XỬ LÝ HOÀN TẤT THÀNH CÔNG")
    print("=" * 70)

if __name__ == '__main__':
    main()
