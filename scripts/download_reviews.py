"""
Script: download_reviews.py
Mục đích: Tải 25.000 reviews từ HuggingFace Hub bằng HTTP streaming
Dataset: Amazon Reviews 2023 - All_Beauty
Đầu ra: data/raw/all_beauty_25k.jsonl
"""
import urllib.request
import ssl
import json
import time
import os
import sys

TARGET_COUNT = 25000
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'all_beauty_25k.jsonl')
URL = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl'

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    print(f"=== Amazon Reviews 2023 - Streaming {TARGET_COUNT:,} Reviews ===")
    print(f"Source URL : {URL}")
    print(f"Output File: {OUTPUT_FILE}")
    print()

    start_time = time.time()
    count = 0
    bytes_read = 0
    buf = b""

    req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
        with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
            content_length = resp.headers.get('Content-Length')
            if content_length:
                print(f"Full remote file size: {int(content_length)/1024/1024:.2f} MB")
            
            while True:
                chunk = resp.read(131072) # 128 KB
                if not chunk:
                    break
                buf += chunk
                bytes_read += len(chunk)

                while b'\n' in buf:
                    line_bytes, buf = buf.split(b'\n', 1)
                    line_str = line_bytes.decode('utf-8', errors='ignore').strip()
                    if line_str:
                        out_f.write(line_str + '\n')
                        count += 1
                        if count % 2500 == 0:
                            elapsed = time.time() - start_time
                            speed = bytes_read / (1024 * 1024 * max(elapsed, 0.001))
                            print(f"  [{count:>6,}/{TARGET_COUNT:,}] reviews written | {bytes_read/1024/1024:.2f} MB ({speed:.2f} MB/s) | {elapsed:.1f}s")
                        
                        if count >= TARGET_COUNT:
                            break
                
                if count >= TARGET_COUNT:
                    break

    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    print()
    print("=== Tải dữ liệu hoàn tất ===")
    print(f"Tổng số review đã lưu : {count:,}")
    print(f"Kích thước file lưu   : {file_size_mb:.2f} MB")
    print(f"Tổng dung lượng đọc   : {bytes_read/1024/1024:.2f} MB")
    print(f"Thời gian tải         : {elapsed:.2f} giây")

if __name__ == '__main__':
    main()
