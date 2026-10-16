"""
Script: stream_sample.py
Mục đích: Tải mẫu 1000 review từ HuggingFace Hub (streaming, không tải full dataset)
Dataset: Amazon Reviews 2023 - All_Beauty
Phương thức: HTTP streaming với urllib (không cần thư viện datasets)
"""
import urllib.request
import ssl
import json
import time
import os
import sys

# Tạo thư mục output
os.makedirs('data/samples', exist_ok=True)

# Bỏ qua SSL verification (workaround cho môi trường Windows không có cert bundle)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

URL = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl'
OUTPUT_PATH = 'data/samples/all_beauty_sample_1000.jsonl'
TARGET_N = 1000
MAX_BYTES = 12 * 1024 * 1024  # Đọc tối đa 12MB (file 311MB, cần ~0.5MB/1000 reviews)

print(f'=== Amazon Reviews 2023 - All_Beauty Sample Download ===')
print(f'URL: {URL}')
print(f'Target: {TARGET_N} records')
print(f'Output: {OUTPUT_PATH}')
print()

start = time.time()
records = []
bytes_read = 0
buf = b''

try:
    req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
        print(f'HTTP Status: {response.status}')
        content_length = response.headers.get('Content-Length')
        if content_length:
            print(f'Full file size: {int(content_length)/1024/1024:.1f} MB')
        print()

        while True:
            chunk = response.read(65536)  # 64KB chunks
            if not chunk:
                break
            buf += chunk
            bytes_read += len(chunk)

            # Xử lý từng dòng JSONL hoàn chỉnh
            while b'\n' in buf:
                line_bytes, buf = buf.split(b'\n', 1)
                line = line_bytes.strip()
                if line:
                    try:
                        rec = json.loads(line)
                        records.append(rec)
                        if len(records) % 100 == 0:
                            print(f'  Loaded {len(records)} records... ({bytes_read/1024:.1f} KB read)')
                    except json.JSONDecodeError:
                        pass

            if len(records) >= TARGET_N:
                break
            if bytes_read > MAX_BYTES:
                print(f'Warning: Stopped at {MAX_BYTES/1024/1024:.1f} MB safety limit')
                break

except urllib.error.URLError as e:
    print(f'URL Error: {e}')
    sys.exit(1)
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
    sys.exit(1)

elapsed = time.time() - start
print()
print(f'=== Download Complete ===')
print(f'Records loaded: {len(records)}')
print(f'Bytes transferred: {bytes_read/1024:.1f} KB ({bytes_read/1024/1024:.2f} MB)')
print(f'Time elapsed: {elapsed:.1f}s')

if not records:
    print('ERROR: No records loaded!')
    sys.exit(1)

# In schema
print()
print('=== SCHEMA (Fields) ===')
print('Fields:', list(records[0].keys()))

# In 2 ví dụ
print()
print('=== SAMPLE RECORD [0] ===')
print(json.dumps(records[0], indent=2, ensure_ascii=False))

print()
print('=== SAMPLE RECORD [1] ===')
print(json.dumps(records[1], indent=2, ensure_ascii=False))

# Lưu file
records_to_save = records[:TARGET_N]
with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
    for r in records_to_save:
        f.write(json.dumps(r, ensure_ascii=False) + '\n')

file_size = os.path.getsize(OUTPUT_PATH)
print()
print(f'=== Saved ===')
print(f'File: {OUTPUT_PATH}')
print(f'Records: {len(records_to_save)}')
print(f'File size: {file_size/1024:.1f} KB ({file_size/1024/1024:.2f} MB)')
