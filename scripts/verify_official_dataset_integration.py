#!/usr/bin/env python3
"""
Script: verify_official_dataset_integration.py
Mục đích:
  Kiểm chứng và xác minh toàn diện khả năng tích hợp PySpark - HDFS trên TẬP DỮ LIỆU THẬT:
  - Raw JSONL: /bigdata/reviews/raw/All_Beauty_full.jsonl (701.528 bản ghi, 311.5 MB)
  - Natural 100k Parquet: /bigdata/reviews/processed/all_beauty_100k_natural.parquet (100.000 bản ghi)
  - Natural Test 20k Parquet: /bigdata/reviews/processed/all_beauty_natural_test.parquet (20.000 bản ghi)
  - Balanced Train 45k Parquet: /bigdata/reviews/processed/all_beauty_balanced_train.parquet (45.000 bản ghi)
  - Kiểm tra Schema, Phân phối nhãn (3 lớp), Phân phối rating (1-5 sao)
  - Kiểm tra tính toàn vẹn block HDFS và xuất báo cáo đối soát.
"""

import sys
import os
import time
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, executable="/bin/bash")
    return res.returncode, res.stdout.strip()

def main():
    print("=" * 75)
    print(" BẮT ĐẦU ĐỐI SOÁT TÍCH HỢP DATASET CHÍNH THỨC PYSPARK & HDFS")
    print("=" * 75)
    start_time = time.time()

    # 1. Khởi tạo SparkSession kết nối HDFS
    print("[1/5] Khởi tạo SparkSession kết nối HDFS...")
    spark = SparkSession.builder \
        .appName("VerifyOfficialDatasetIntegration") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
        .config("spark.sql.warehouse.dir", f"{HDFS_URI}/user/spark/warehouse") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("✓ SparkSession kết nối thành công với HDFS master tại", HDFS_URI)

    # 2. Kiểm tra Master Raw JSONL trên HDFS
    raw_path = f"{HDFS_URI}/bigdata/reviews/raw/All_Beauty_full.jsonl"
    print(f"\n[2/5] Đọc Master Raw JSONL từ HDFS: {raw_path}")
    raw_df = spark.read.text(raw_path)
    raw_count = raw_df.count()
    print(f"✓ Master Raw File tồn tại trên HDFS: Tổng số dòng = {raw_count:,}")

    # 3. Đọc và xác minh 100k Natural Parquet từ HDFS
    natural_parquet_path = f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_100k_natural.parquet"
    print(f"\n[3/5] Đọc Natural 100k Parquet từ HDFS: {natural_parquet_path}")
    nat_df = spark.read.parquet(natural_parquet_path)
    nat_count = nat_df.count()
    print(f"✓ Đã nạp thành công DataFrame từ HDFS: Tổng số dòng = {nat_count:,}")
    print("Schema thực tế của Natural 100k Parquet:")
    nat_df.printSchema()

    # Phân phối nhãn & rating của 100k Natural
    print("\nPhân phối nhãn sentiment (Natural 100k):")
    nat_sentiment_dist = nat_df.groupBy("sentiment").agg(
        F.count("*").alias("count"),
        F.round(F.count("*") * 100.0 / nat_count, 2).alias("percentage")
    ).orderBy(F.desc("count")).collect()
    for row in nat_sentiment_dist:
        print(f"  - {row['sentiment']:<10}: {row['count']:,} ({row['percentage']}%)")

    print("\nPhân phối rating 1-5 sao (Natural 100k):")
    nat_rating_dist = nat_df.groupBy("rating").agg(
        F.count("*").alias("count"),
        F.round(F.count("*") * 100.0 / nat_count, 2).alias("percentage")
    ).orderBy("rating").collect()
    for row in nat_rating_dist:
        print(f"  - {row['rating']} ⭐: {row['count']:,} ({row['percentage']}%)")

    # 4. Đọc và xác minh 20k Natural Test Set từ HDFS
    test_parquet_path = f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_natural_test.parquet"
    print(f"\n[4/5] Đọc Natural Test 20k Parquet từ HDFS: {test_parquet_path}")
    test_df = spark.read.parquet(test_parquet_path)
    test_count = test_df.count()
    print(f"✓ Đã nạp thành công Test Set từ HDFS: Tổng số dòng = {test_count:,}")
    test_sentiment_dist = test_df.groupBy("sentiment").agg(
        F.count("*").alias("count"),
        F.round(F.count("*") * 100.0 / test_count, 2).alias("percentage")
    ).orderBy(F.desc("count")).collect()
    for row in test_sentiment_dist:
        print(f"  - {row['sentiment']:<10}: {row['count']:,} ({row['percentage']}%)")

    # 5. Đọc và xác minh 45k Balanced Train Set từ HDFS
    bal_parquet_path = f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_balanced_train.parquet"
    print(f"\n[5/5] Đọc Balanced Train 45k Parquet từ HDFS: {bal_parquet_path}")
    bal_df = spark.read.parquet(bal_parquet_path)
    bal_count = bal_df.count()
    print(f"✓ Đã nạp thành công Balanced Train từ HDFS: Tổng số dòng = {bal_count:,}")
    bal_sentiment_dist = bal_df.groupBy("sentiment").agg(
        F.count("*").alias("count"),
        F.round(F.count("*") * 100.0 / bal_count, 2).alias("percentage")
    ).orderBy("sentiment").collect()
    for row in bal_sentiment_dist:
        print(f"  - {row['sentiment']:<10}: {row['count']:,} ({row['percentage']}%)")

    # Sample rows verification
    print("\nKiểm tra 2 bản ghi mẫu từ HDFS Parquet:")
    samples = nat_df.select("review_id", "rating", "sentiment", "title", F.substring("text", 1, 60).alias("text_snippet")).limit(2).collect()
    for s in samples:
        print(f"  - ID: {s['review_id']} | {s['rating']}⭐ | {s['sentiment']} | Title: {s['title']} | Text: {s['text_snippet']}...")

    # Stop Spark
    spark.stop()
    print("\n✓ Đã đóng SparkSession an toàn.")

    # 6. Kiểm tra HDFS Du & Block Health (fsck)
    print("\n[6/6] Kiểm tra dung lượng vật lý và tính toàn vẹn Block HDFS:")
    code_du, out_du = run_cmd("hdfs dfs -du -h /bigdata/reviews")
    print("Dung lượng HDFS:")
    print(out_du)

    code_fsck, out_fsck = run_cmd("hdfs fsck /bigdata/reviews -summary")
    print("\nTrạng thái sức khỏe HDFS (FSCK Summary):")
    print(out_fsck)

    duration = round(time.time() - start_time, 2)
    print(f"\n" + "=" * 75)
    print(f" HOÀN TẤT ĐỐI SOÁT TÍCH HỢP DATASET CHÍNH THỨC TRONG {duration} GIÂY")
    print("=" * 75)

if __name__ == "__main__":
    main()
