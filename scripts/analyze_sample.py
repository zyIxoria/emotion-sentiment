"""
Script: analyze_sample.py
Mục đích: Phân tích chất lượng dữ liệu mẫu 1000 review All_Beauty
Đầu vào: data/samples/all_beauty_sample_1000.jsonl
Đầu ra: reports/dataset_statistics.csv + in kết quả phân tích
"""
import json
import os
import csv
import statistics
from datetime import datetime
from collections import Counter

SAMPLE_PATH = 'data/samples/all_beauty_sample_1000.jsonl'
CSV_OUTPUT = 'reports/dataset_statistics.csv'
os.makedirs('reports', exist_ok=True)

# Đọc dữ liệu
records = []
with open(SAMPLE_PATH, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            records.append(json.loads(line))

N = len(records)
print(f'=== PHÂN TÍCH CHẤT LƯỢNG DỮ LIỆU ===')
print(f'Số bản ghi đọc được: {N}')
print(f'Nguồn: {SAMPLE_PATH}')
print()

# ========== 7.1. THỐNG KÊ CƠ BẢN ==========
print('--- 7.1 THỐNG KÊ CƠ BẢN ---')
print(f'Số cột: {len(records[0].keys())}')
print(f'Các trường: {list(records[0].keys())}')
print()

# Null counts
fields = list(records[0].keys())
null_counts = {f: 0 for f in fields}
for r in records:
    for f in fields:
        v = r.get(f)
        if v is None or v == '' or v == []:
            null_counts[f] += 1

print('Số giá trị null/rỗng theo trường:')
for f, cnt in null_counts.items():
    pct = cnt/N*100
    print(f'  {f}: {cnt} ({pct:.1f}%)')
print()

# Rating distribution
ratings = [r.get('rating') for r in records if r.get('rating') is not None]
rating_counts = Counter([int(r) for r in ratings])
print('Phân bố rating:')
for star in [1, 2, 3, 4, 5]:
    cnt = rating_counts.get(star, 0)
    pct = cnt/N*100
    bar = '█' * int(pct/2)
    print(f'  {star} sao: {cnt:4d} ({pct:5.1f}%) {bar}')
print()

# Invalid ratings
invalid_ratings = [r for r in records if r.get('rating') not in [1.0, 2.0, 3.0, 4.0, 5.0, 1, 2, 3, 4, 5]]
print(f'Rating không hợp lệ: {len(invalid_ratings)}')

# Missing both title and text
missing_text_count = sum(
    1 for r in records
    if (not r.get('title') or str(r.get('title','')).strip() == '')
    and (not r.get('text') or str(r.get('text','')).strip() == '')
)
print(f'Review thiếu cả title và text: {missing_text_count}')

# Empty content after strip
empty_title = sum(1 for r in records if not str(r.get('title','')).strip())
empty_text = sum(1 for r in records if not str(r.get('text','')).strip())
print(f'Title rỗng (sau strip): {empty_title}')
print(f'Text rỗng (sau strip): {empty_text}')

# Unique products
unique_asin = len(set(r.get('asin') for r in records if r.get('asin')))
unique_parent = len(set(r.get('parent_asin') for r in records if r.get('parent_asin')))
unique_users = len(set(r.get('user_id') for r in records if r.get('user_id')))
print(f'Số asin khác nhau: {unique_asin}')
print(f'Số parent_asin khác nhau: {unique_parent}')
print(f'Số user_id khác nhau: {unique_users}')

# Timestamp range
timestamps = [r.get('timestamp') for r in records if r.get('timestamp')]
if timestamps:
    ts_min = min(timestamps) / 1000  # milliseconds to seconds
    ts_max = max(timestamps) / 1000
    dt_min = datetime.utcfromtimestamp(ts_min).strftime('%Y-%m-%d')
    dt_max = datetime.utcfromtimestamp(ts_max).strftime('%Y-%m-%d')
    print(f'Khoảng thời gian review: {dt_min} đến {dt_max}')
print()

# ========== 7.2. KIỂM TRA NỘI DUNG VĂN BẢN ==========
print('--- 7.2 KIỂM TRA NỘI DUNG VĂN BẢN ---')

# Combine title + text
def get_review_text(r):
    title = str(r.get('title', '') or '').strip()
    text = str(r.get('text', '') or '').strip()
    parts = [p for p in [title, text] if p]
    return ' '.join(parts)

review_texts = [get_review_text(r) for r in records]
text_lengths = [len(t.split()) for t in review_texts]  # word count

print('Độ dài review (số từ - title + text):')
print(f'  Min:    {min(text_lengths)}')
print(f'  Max:    {max(text_lengths)}')
print(f'  Mean:   {statistics.mean(text_lengths):.1f}')
print(f'  Median: {statistics.median(text_lengths):.1f}')
sorted_len = sorted(text_lengths)
p95_idx = int(0.95 * len(sorted_len))
print(f'  P95:    {sorted_len[p95_idx]}')
print()

# Examples
print('Ví dụ review ngắn (<= 5 từ):')
short_examples = [(i, r) for i, (r, l) in enumerate(zip(records, text_lengths)) if l <= 5][:3]
for i, r in short_examples:
    print(f'  [{i}] Rating={r["rating"]}: "{get_review_text(r)}"')

print()
print('Ví dụ review trung bình (40-60 từ):')
mid_examples = [(i, r) for i, (r, l) in enumerate(zip(records, text_lengths)) if 40 <= l <= 60][:2]
for i, r in mid_examples:
    print(f'  [{i}] Rating={r["rating"]}: "{get_review_text(r)[:150]}..."')

print()
print('Ví dụ review dài (>= 150 từ):')
long_examples = [(i, r) for i, (r, l) in enumerate(zip(records, text_lengths)) if l >= 150][:2]
for i, r in long_examples:
    print(f'  [{i}] Rating={r["rating"]}: "{get_review_text(r)[:150]}..."')
print()

# Special char only
def is_noise(text):
    stripped = ''.join(c for c in text if c.isalnum())
    return len(stripped) < 3

noise_count = sum(1 for t in review_texts if is_noise(t))
print(f'Review chỉ có ký tự đặc biệt/khoảng trắng: {noise_count}')

# Duplicate check (by user_id + asin)
pairs = [(r.get('user_id',''), r.get('asin','')) for r in records]
duplicate_pairs = N - len(set(pairs))
print(f'Bản ghi trùng (cùng user_id + asin): {duplicate_pairs}')
print()

# ========== 7.3. PHÂN TÍCH NHÃN DỰ KIẾN ==========
print('--- 7.3 PHÂN TÍCH NHÃN DỰ KIẾN ---')
print('Quy tắc gán nhãn:')
print('  Rating 1-2 → negative')
print('  Rating 3   → neutral')
print('  Rating 4-5 → positive')
print()

def get_sentiment(rating):
    r = float(rating)
    if r <= 2:
        return 'negative'
    elif r == 3:
        return 'neutral'
    else:
        return 'positive'

sentiment_counts = Counter(get_sentiment(r['rating']) for r in records if r.get('rating'))
total_labeled = sum(sentiment_counts.values())
print('Phân bố nhãn dự kiến trên mẫu 1000:')
for label in ['negative', 'neutral', 'positive']:
    cnt = sentiment_counts.get(label, 0)
    pct = cnt/total_labeled*100
    bar = '█' * int(pct/2)
    print(f'  {label:10s}: {cnt:4d} ({pct:5.1f}%) {bar}')
print()

# Class imbalance check
max_cnt = max(sentiment_counts.values())
min_cnt = min(sentiment_counts.values())
imbalance_ratio = max_cnt / max(min_cnt, 1)
print(f'Tỷ lệ mất cân bằng lớp (max/min): {imbalance_ratio:.1f}x')
if imbalance_ratio > 5:
    print('  ⚠ Cảnh báo: Lệch lớp NGHIÊM TRỌNG (> 5x)')
elif imbalance_ratio > 3:
    print('  ⚠ Cảnh báo: Lệch lớp ĐÁng lo ngại (> 3x)')
else:
    print('  ✓ Lệch lớp ở mức chấp nhận được')
print()

# Potential rating-text contradictions (simplified check)
contradictions = 0
for r in records:
    rating = float(r.get('rating', 3))
    text = get_review_text(r).lower()
    negative_words = ['terrible', 'awful', 'horrible', 'hate', 'worst', 'broken', 'disappointed']
    positive_words = ['love', 'excellent', 'amazing', 'perfect', 'great', 'wonderful', 'fantastic']
    has_neg_words = any(w in text for w in negative_words)
    has_pos_words = any(w in text for w in positive_words)
    if rating >= 4 and has_neg_words and not has_pos_words:
        contradictions += 1
    elif rating <= 2 and has_pos_words and not has_neg_words:
        contradictions += 1
print(f'Trường hợp rating-text có thể mâu thuẫn (ước tính): {contradictions}')
print()

# ========== LƯU CSV ==========
stats_data = [
    {'metric': 'total_records_in_sample', 'value': N},
    {'metric': 'total_columns', 'value': len(fields)},
    {'metric': 'null_rating', 'value': null_counts.get('rating', 0)},
    {'metric': 'null_title', 'value': null_counts.get('title', 0)},
    {'metric': 'null_text', 'value': null_counts.get('text', 0)},
    {'metric': 'null_timestamp', 'value': null_counts.get('timestamp', 0)},
    {'metric': 'null_user_id', 'value': null_counts.get('user_id', 0)},
    {'metric': 'null_parent_asin', 'value': null_counts.get('parent_asin', 0)},
    {'metric': 'missing_both_title_text', 'value': missing_text_count},
    {'metric': 'empty_title', 'value': empty_title},
    {'metric': 'empty_text', 'value': empty_text},
    {'metric': 'invalid_ratings', 'value': len(invalid_ratings)},
    {'metric': 'unique_asin', 'value': unique_asin},
    {'metric': 'unique_parent_asin', 'value': unique_parent},
    {'metric': 'unique_users', 'value': unique_users},
    {'metric': 'rating_1star', 'value': rating_counts.get(1, 0)},
    {'metric': 'rating_2star', 'value': rating_counts.get(2, 0)},
    {'metric': 'rating_3star', 'value': rating_counts.get(3, 0)},
    {'metric': 'rating_4star', 'value': rating_counts.get(4, 0)},
    {'metric': 'rating_5star', 'value': rating_counts.get(5, 0)},
    {'metric': 'label_negative', 'value': sentiment_counts.get('negative', 0)},
    {'metric': 'label_neutral', 'value': sentiment_counts.get('neutral', 0)},
    {'metric': 'label_positive', 'value': sentiment_counts.get('positive', 0)},
    {'metric': 'text_len_min_words', 'value': min(text_lengths)},
    {'metric': 'text_len_max_words', 'value': max(text_lengths)},
    {'metric': 'text_len_mean_words', 'value': round(statistics.mean(text_lengths), 1)},
    {'metric': 'text_len_median_words', 'value': statistics.median(text_lengths)},
    {'metric': 'text_len_p95_words', 'value': sorted_len[p95_idx]},
    {'metric': 'noise_reviews', 'value': noise_count},
    {'metric': 'duplicate_user_asin_pairs', 'value': duplicate_pairs},
    {'metric': 'class_imbalance_ratio', 'value': round(imbalance_ratio, 2)},
    {'metric': 'potential_contradictions', 'value': contradictions},
    {'metric': 'timestamp_min', 'value': dt_min},
    {'metric': 'timestamp_max', 'value': dt_max},
    {'metric': 'full_dataset_review_count', 'value': '632000 (approximate)'},
    {'metric': 'full_dataset_file_size_mb', 'value': 311.5},
    {'metric': 'full_dataset_compressed_mb', 'value': 31.6},
]

with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['metric', 'value'])
    writer.writeheader()
    writer.writerows(stats_data)

print(f'✓ Đã lưu thống kê vào: {CSV_OUTPUT}')
print()
print('=== PHÂN TÍCH HOÀN TẤT ===')
