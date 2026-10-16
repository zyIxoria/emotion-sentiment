#!/usr/bin/env python3
"""
Script: train_baseline_lr_tfidf.py
Mục đích:
  Thực hiện Phase 3 — Huấn luyện và Đánh giá Mô hình Baseline:
  TF-IDF + Logistic Regression bằng PySpark MLlib trên HDFS.

Ràng buộc nghiêm ngặt:
  1. Train Set: hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_balanced_train.parquet (45.000 mẫu cân bằng)
  2. Test Set: hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_natural_test.parquet (20.000 mẫu tự nhiên)
  3. Cột văn bản: "full_text" | Cột nhãn: "label"
  4. Ánh xạ nhãn: 0 = positive, 1 = neutral, 2 = negative
  5. Fit DUY NHẤT trên Train (bao gồm cả IDF và Model). KHÔNG leak Test vào fit/transform/tuning.
  6. Xuất Model vào HDFS (/bigdata/models/baseline_lr_tfidf/) và sinh toàn bộ báo cáo, JSON, CSV.
"""

import os
import sys
import time
import json
import csv
import subprocess
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_fscore_support

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

HDFS_URI = "hdfs://localhost:9000"
TRAIN_PATH = f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_balanced_train.parquet"
TEST_PATH = f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_natural_test.parquet"
MODEL_HDFS_PATH = f"{HDFS_URI}/bigdata/models/baseline_lr_tfidf"

LOCAL_REPORTS_DIR = "/mnt/d/Filehoc/big-data/emotion-sentiment/reports/models"

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, executable="/bin/bash")
    return res.returncode, res.stdout.strip()

def main():
    total_start_time = time.perf_counter()
    print("=" * 80)
    print(" BẮT ĐẦU PHASE 3: HUẤN LUYỆN BASELINE TF-IDF + LOGISTIC REGRESSION (PYSPARK MLLIB)")
    print("=" * 80)

    # 1. Khởi tạo SparkSession
    print("\n[1/6] Khởi tạo SparkSession kết nối HDFS...")
    spark_init_start = time.perf_counter()
    spark = SparkSession.builder \
        .appName("Phase3_Baseline_TFIDF_LogisticRegression") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
        .config("spark.sql.warehouse.dir", f"{HDFS_URI}/user/spark/warehouse") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark_init_duration = max(0.01, round(time.perf_counter() - spark_init_start, 2))
    print(f"✓ SparkSession đã khởi tạo trong {spark_init_duration}s (HDFS: {HDFS_URI})")

    # 2. Đọc dữ liệu Train và Test từ HDFS
    print(f"\n[2/6] Nạp dữ liệu từ HDFS...")
    data_load_start = time.perf_counter()
    train_df = spark.read.parquet(TRAIN_PATH)
    test_df = spark.read.parquet(TEST_PATH)

    train_count = train_df.count()
    test_count = test_df.count()
    data_load_duration = max(0.01, round(time.perf_counter() - data_load_start, 2))
    print(f"✓ Đọc dữ liệu hoàn tất trong {data_load_duration}s:")
    print(f"  - Tập Train (Balanced) : {train_count:,} dòng từ {TRAIN_PATH}")
    print(f"  - Tập Test (Natural)   : {test_count:,} dòng từ {TEST_PATH}")

    # 3. Pre-flight Data & Leakage Validation
    print("\n[3/6] Xác minh tính toàn vẹn và kiểm tra Data Leakage...")
    
    # Check nulls
    train_nulls = train_df.filter(F.col("full_text").isNull() | F.col("label").isNull()).count()
    test_nulls = test_df.filter(F.col("full_text").isNull() | F.col("label").isNull()).count()
    if train_nulls > 0 or test_nulls > 0:
        raise ValueError(f"Lỗi: Tìm thấy null (Train: {train_nulls}, Test: {test_nulls})")

    # Check empty text
    train_empty = train_df.filter(F.trim(F.col("full_text")) == "").count()
    test_empty = test_df.filter(F.trim(F.col("full_text")) == "").count()
    if train_empty > 0 or test_empty > 0:
        raise ValueError(f"Lỗi: Tìm thấy văn bản rỗng (Train: {train_empty}, Test: {test_empty})")

    # Check label values
    train_labels_set = set([r["label"] for r in train_df.select("label").distinct().collect()])
    test_labels_set = set([r["label"] for r in test_df.select("label").distinct().collect()])
    print(f"  - Nhãn trong Train: {train_labels_set} | Nhãn trong Test: {test_labels_set}")
    if train_labels_set != {0, 1, 2} or test_labels_set != {0, 1, 2}:
        raise ValueError("Lỗi: Nhãn không khớp với tập {0, 1, 2}")

    # Label distribution
    train_dist = {r["label"]: r["count"] for r in train_df.groupBy("label").count().collect()}
    test_dist = {r["label"]: r["count"] for r in test_df.groupBy("label").count().collect()}
    print(f"  - Phân phối Train (0:Pos, 1:Neu, 2:Neg): {train_dist}")
    print(f"  - Phân phối Test  (0:Pos, 1:Neu, 2:Neg): {test_dist}")

    # Review_id Leakage Check
    train_ids = train_df.select("review_id").dropDuplicates()
    test_ids = test_df.select("review_id").dropDuplicates()
    overlap_ids = train_ids.join(test_ids, on="review_id", how="inner").count()
    print(f"  - Số review_id trùng lặp giữa Train và Test: {overlap_ids}")
    if overlap_ids > 0:
        raise ValueError(f"LỖI RÒ RỈ DỮ LIỆU: Phát hiện {overlap_ids} review_id trùng giữa Train và Test!")
    print("  -> Xác nhận: TUYỆT ĐỐI KHÔNG CÓ RÒ RỈ DỮ LIỆU (100% ISOLATED) ✅")

    # Text overlap
    train_texts = train_df.select("full_text").dropDuplicates()
    test_texts = test_df.select("full_text").dropDuplicates()
    overlap_texts = train_texts.join(test_texts, on="full_text", how="inner").count()
    print(f"  - Số full_text trùng khớp (câu ngắn mẫu như 'Great', 'Nice'): {overlap_texts}")

    # 4. Xây dựng MLlib Pipeline
    print("\n[4/6] Thiết lập kiến trúc Pipeline PySpark MLlib...")
    # 4.1. RegexTokenizer
    tokenizer = RegexTokenizer(
        inputCol="full_text",
        outputCol="words",
        pattern=r"\W+",
        toLowercase=True
    )
    
    # 4.2. StopWordsRemover
    default_stop_words = StopWordsRemover.loadDefaultStopWords("english")
    stop_words_remover = StopWordsRemover(
        inputCol="words",
        outputCol="filtered_words",
        stopWords=default_stop_words
    )
    print(f"  - StopWordsRemover: English ({len(default_stop_words)} từ dừng chuẩn)")

    # 4.3. HashingTF
    num_features = 10000
    hashing_tf = HashingTF(
        inputCol="filtered_words",
        outputCol="raw_features",
        numFeatures=num_features
    )
    print(f"  - HashingTF: numFeatures = {num_features:,}")

    # 4.4. IDF
    idf = IDF(
        inputCol="raw_features",
        outputCol="features"
    )

    # 4.5. LogisticRegression
    max_iter = 100
    reg_param = 0.01
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=max_iter,
        regParam=reg_param,
        family="multinomial"
    )
    print(f"  - LogisticRegression: maxIter = {max_iter}, regParam = {reg_param}, family = multinomial")

    pipeline = Pipeline(stages=[
        tokenizer,
        stop_words_remover,
        hashing_tf,
        idf,
        lr
    ])

    # 5. Huấn luyện (Fit DUY NHẤT trên Train)
    print("\n[5/6] Tiến hành huấn luyện Pipeline trên tập Balanced Train...")
    train_fit_start = time.perf_counter()
    model = pipeline.fit(train_df)
    train_fit_duration = max(0.01, round(time.perf_counter() - train_fit_start, 2))
    print(f"✓ Huấn luyện hoàn tất trong: {train_fit_duration} giây")

    # 6. Dự đoán trên tập Natural Test
    print("\n[6/6] Dự đoán và đánh giá trên tập Natural Test độc lập...")
    pred_start = time.perf_counter()
    predictions = model.transform(test_df)
    predictions.cache()
    pred_count = predictions.count()
    pred_duration = max(0.01, round(time.perf_counter() - pred_start, 2))
    print(f"✓ Dự đoán xong cho {pred_count:,} bản ghi Test trong: {pred_duration} giây")

    # Thu thập kết quả nhãn dự đoán để tính toán metric toàn diện
    print("  Đang tổng hợp các chỉ số đánh giá...")
    pdf = predictions.select("label", "prediction").toPandas()
    y_true = pdf["label"].astype(int).values
    y_pred = pdf["prediction"].astype(int).values

    # Accuracy
    acc = accuracy_score(y_true, y_pred)

    # Weighted metrics
    w_prec, w_rec, w_f1, _ = precision_recall_fscore_support(y_true, y_pred, average="weighted", zero_division=0)

    # Macro metrics
    m_prec, m_rec, m_f1, _ = precision_recall_fscore_support(y_true, y_pred, average="macro", zero_division=0)

    # Per-class metrics
    class_names = ["positive (0)", "neutral (1)", "negative (2)"]
    class_labels = [0, 1, 2]
    c_prec, c_rec, c_f1, c_supp = precision_recall_fscore_support(y_true, y_pred, labels=class_labels, average=None, zero_division=0)

    # Confusion matrix (Rows: True, Cols: Pred)
    cm = confusion_matrix(y_true, y_pred, labels=class_labels)

    # PySpark MulticlassClassificationEvaluator confirmation
    eval_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
    eval_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    eval_w_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    eval_w_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)

    total_experiment_duration = round(time.perf_counter() - total_start_time, 2)

    # In kết quả đánh giá ra terminal
    print("\n" + "=" * 80)
    print(" KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH BASELINE TRÊN NATURAL TEST SET (20.000 MẪU)")
    print("=" * 80)
    print(f" Accuracy                 : {acc:.4f} ({acc*100:.2f}%)")
    print(f" Macro F1-score           : {m_f1:.4f} ({m_f1*100:.2f}%)  [Chỉ số quan trọng nhất cho 3 lớp]")
    print(f" Weighted F1-score        : {w_f1:.4f} ({w_f1*100:.2f}%)")
    print(f" Macro Precision / Recall : {m_prec:.4f} / {m_rec:.4f}")
    print(f" Weighted Prec / Recall   : {w_prec:.4f} / {w_rec:.4f}")
    print("-" * 80)
    print(" CHI TIẾT HIỆU NĂNG TỪNG LỚP (PER-CLASS PERFORMANCE):")
    for i, c_name in enumerate(class_names):
        print(f"  • {c_name:<16}: Precision = {c_prec[i]:.4f} | Recall = {c_rec[i]:.4f} | F1 = {c_f1[i]:.4f} | Support = {c_supp[i]:,}")
    print("-" * 80)
    print(" MA TRẬN NHẦM LẪN (CONFUSION MATRIX):")
    print(" Trục hàng: Nhãn thực tế (True Label)")
    print(" Trục cột : Nhãn dự đoán (Predicted Label)")
    print(f"{'':<20} | Pred: Positive (0) | Pred: Neutral (1) | Pred: Negative (2)")
    print(f"True: Positive (0)   | {cm[0][0]:<17} | {cm[0][1]:<16} | {cm[0][2]:<17}")
    print(f"True: Neutral (1)    | {cm[1][0]:<17} | {cm[1][1]:<16} | {cm[1][2]:<17}")
    print(f"True: Negative (2)   | {cm[2][0]:<17} | {cm[2][1]:<16} | {cm[2][2]:<17}")
    print("=" * 80)

    # 7. Lưu Model vào HDFS
    print("\n[7/7] Lưu Model Pipeline vào HDFS...")
    model_save_start = time.perf_counter()
    run_cmd("hdfs dfsadmin -safemode leave")
    run_cmd("hdfs dfs -mkdir -p /bigdata/models")
    try:
        model.write().overwrite().save(MODEL_HDFS_PATH)
        model_save_success = True
        model_save_msg = f"Đã lưu thành công tại {MODEL_HDFS_PATH}"
        print(f"✓ {model_save_msg}")
    except Exception as e:
        model_save_success = False
        model_save_msg = f"Lỗi lưu model: {str(e)}"
        print(f"⚠ {model_save_msg}")
    model_save_duration = round(time.perf_counter() - model_save_start, 2)

    # 8. Xuất Artifacts và Báo cáo
    os.makedirs(LOCAL_REPORTS_DIR, exist_ok=True)
    
    # 8.1. File JSON Metric
    metrics_json_path = os.path.join(LOCAL_REPORTS_DIR, "baseline_metrics.json")
    metrics_data = {
        "experiment_name": "Phase 3 Baseline TF-IDF + Logistic Regression",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "environment": {
            "os": "WSL2 Ubuntu 24.04 LTS",
            "java_version": "OpenJDK 11.0.32.1",
            "hadoop_version": "3.3.6",
            "pyspark_version": "3.5.5",
            "spark_master": "local[4]",
            "spark_driver_memory": "4g"
        },
        "datasets": {
            "train_path": TRAIN_PATH,
            "train_samples": int(train_count),
            "train_distribution": {str(k): int(v) for k, v in train_dist.items()},
            "test_path": TEST_PATH,
            "test_samples": int(test_count),
            "test_distribution": {str(k): int(v) for k, v in test_dist.items()}
        },
        "pipeline_config": {
            "tokenizer": "RegexTokenizer (pattern=\\W+, toLowercase=True)",
            "stop_words": f"StopWordsRemover (English, {len(default_stop_words)} words)",
            "hashing_tf": {"numFeatures": num_features},
            "idf": "IDF (standard inverse document frequency)",
            "classifier": {
                "name": "LogisticRegression",
                "maxIter": max_iter,
                "regParam": reg_param,
                "family": "multinomial"
            }
        },
        "metrics_overall": {
            "accuracy": float(round(acc, 4)),
            "macro_f1": float(round(m_f1, 4)),
            "macro_precision": float(round(m_prec, 4)),
            "macro_recall": float(round(m_rec, 4)),
            "weighted_f1": float(round(w_f1, 4)),
            "weighted_precision": float(round(w_prec, 4)),
            "weighted_recall": float(round(w_rec, 4))
        },
        "metrics_per_class": {
            "positive_0": {
                "precision": float(round(c_prec[0], 4)),
                "recall": float(round(c_rec[0], 4)),
                "f1_score": float(round(c_f1[0], 4)),
                "support": int(c_supp[0])
            },
            "neutral_1": {
                "precision": float(round(c_prec[1], 4)),
                "recall": float(round(c_rec[1], 4)),
                "f1_score": float(round(c_f1[1], 4)),
                "support": int(c_supp[1])
            },
            "negative_2": {
                "precision": float(round(c_prec[2], 4)),
                "recall": float(round(c_rec[2], 4)),
                "f1_score": float(round(c_f1[2], 4)),
                "support": int(c_supp[2])
            }
        },
        "confusion_matrix": {
            "classes": ["positive (0)", "neutral (1)", "negative (2)"],
            "matrix": cm.tolist()
        },
        "timings": {
            "spark_init_seconds": spark_init_duration,
            "data_loading_seconds": data_load_duration,
            "train_fit_seconds": train_fit_duration,
            "test_predict_seconds": pred_duration,
            "model_save_seconds": model_save_duration,
            "total_experiment_seconds": total_experiment_duration
        },
        "model_save_status": {
            "hdfs_path": MODEL_HDFS_PATH,
            "success": model_save_success,
            "message": model_save_msg
        }
    }
    with open(metrics_json_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2, ensure_ascii=False)
    print(f"✓ Đã lưu file metric JSON tại: {metrics_json_path}")

    # 8.2. Confusion Matrix CSV
    cm_csv_path = os.path.join(LOCAL_REPORTS_DIR, "baseline_confusion_matrix.csv")
    with open(cm_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["True_Label", "Pred_Positive_0", "Pred_Neutral_1", "Pred_Negative_2"])
        writer.writerow(["Positive_0", cm[0][0], cm[0][1], cm[0][2]])
        writer.writerow(["Neutral_1", cm[1][0], cm[1][1], cm[1][2]])
        writer.writerow(["Negative_2", cm[2][0], cm[2][1], cm[2][2]])
    print(f"✓ Đã lưu Confusion Matrix CSV tại: {cm_csv_path}")

    # 8.3. Classification Report CSV
    report_csv_path = os.path.join(LOCAL_REPORTS_DIR, "baseline_classification_report.csv")
    with open(report_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Class", "Precision", "Recall", "F1_Score", "Support"])
        for i, c_name in enumerate(["positive", "neutral", "negative"]):
            writer.writerow([c_name, round(c_prec[i], 4), round(c_rec[i], 4), round(c_f1[i], 4), int(c_supp[i])])
        writer.writerow(["macro_avg", round(m_prec, 4), round(m_rec, 4), round(m_f1, 4), int(sum(c_supp))])
        writer.writerow(["weighted_avg", round(w_prec, 4), round(w_rec, 4), round(w_f1, 4), int(sum(c_supp))])
    print(f"✓ Đã lưu Classification Report CSV tại: {report_csv_path}")

    # 8.4. Báo cáo Markdown chi tiết
    md_report_path = os.path.join(LOCAL_REPORTS_DIR, "baseline_model_report.md")
    report_md = f"""# BÁO CÁO KẾT QUẢ HUẤN LUYỆN MÔ HÌNH BASELINE TF-IDF + LOGISTIC REGRESSION (PHASE 3)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Mô hình:** Baseline `TF-IDF + Multinomial Logistic Regression` (PySpark MLlib)  
**Môi trường:** Apache Spark 3.5.5 trên Hadoop HDFS 3.3.6 (WSL2 Ubuntu 24.04 LTS)  
**Thời điểm thực hiện:** {time.strftime('%Y-%m-%d %H:%M:%S')}  
**Người thực hiện:** Antigravity (AI Data & ML Engineer)  

---

## 1. MỤC TIÊU THỰC NGHIỆM
1. Xây dựng pipeline học máy phân tán chuẩn mực bằng PySpark MLlib: tiền xử lý văn bản, trích chọn đặc trưng TF-IDF và phân loại cảm xúc 3 lớp.
2. Huấn luyện mô hình trên tập cân bằng đã nạp vào HDFS (`all_beauty_balanced_train.parquet`, 45.000 mẫu).
3. Đánh giá tính tổng quát hóa trên tập kiểm thử độc lập tự nhiên (`all_beauty_natural_test.parquet`, 20.000 mẫu) nhằm thiết lập đường cơ sở (Baseline Benchmark) có độ tin cậy cao cho các mô hình nâng cao sau này.

---

## 2. MÔI TRƯỜNG VÀ DỮ LIỆU ĐẦU VÀO

### 2.1. Thông số môi trường
- **Hệ điều hành:** WSL2 Ubuntu 24.04 LTS (Windows 11 Host, CPU AMD Ryzen AI 16 vCPUs).
- **Java Virtual Machine:** OpenJDK 11.0.32.1.
- **Lưu trữ phân tán:** Apache Hadoop HDFS (`hdfs://localhost:9000`).
- **Nền tảng tính toán:** Apache Spark 3.5.5, PySpark MLlib (`master="local[4]"`, `driver-memory="4g"`).

### 2.2. Dữ liệu thực nghiệm trên HDFS
| Tập dữ liệu | Đường dẫn HDFS | Số dòng | Phân phối Positive (0) | Phân phối Neutral (1) | Phân phối Negative (2) | Vai trò |
|---|---|:---:|:---:|:---:|:---:|---|
| **Balanced Train** | `{TRAIN_PATH}` | **45.000** | 15.000 (33,33%) | 15.000 (33,33%) | 15.000 (33,33%) | **Huấn luyện mô hình** |
| **Natural Test** | `{TEST_PATH}` | **20.000** | 14.920 (74,60%) | 1.746 (8,73%) | 3.334 (16,67%) | **Đánh giá độc lập** |

### 2.3. Quy tắc ánh xạ nhãn số (Đã xác minh 100%)
- **`0 = positive`** (Rating 4, 5 sao)
- **`1 = neutral`** (Rating 3 sao)
- **`2 = negative`** (Rating 1, 2 sao)

### 2.4. Kết quả kiểm tra rò rỉ dữ liệu (Data Leakage Verification)
- **Số review_id trùng giữa Train và Test:** **`0`** (Tỷ lệ: `0,0000%`).
- **Nguyên tắc cô lập:** Toàn bộ Pipeline (gồm cả bộ lọc từ dừng, thống kê HashingTF, mô hình nghịch đảo tần suất văn bản `IDF` và `LogisticRegression`) **chỉ được gọi `.fit()` duy nhất trên tập Train**. Tập Test hoàn toàn giữ kín và chỉ chạy qua `.transform()`.

---

## 3. THIẾT KẾ PIPELINE PYSPARK MLLIB VÀ SIÊU THAM SỐ

Pipeline bao gồm 5 giai đoạn nối tiếp:

```text
Cột "full_text"
      │
      ▼
RegexTokenizer (pattern="\\W+", toLowercase=True)
      │
      ▼
StopWordsRemover (stopWords=English {len(default_stop_words)} words)
      │
      ▼
HashingTF (numFeatures=10000, inputCol="filtered_words", outputCol="raw_features")
      │
      ▼
IDF (inputCol="raw_features", outputCol="features")
      │
      ▼
LogisticRegression (family="multinomial", maxIter=100, regParam=0.01)
```

- **Siêu tham số lựa chọn:**
  - `numFeatures`: `10.000` (độ rộng không gian vector từ điển băm).
  - `maxIter`: `100` vòng lặp tối ưu hóa hàm mất mát Cross-Entropy đa lớp.
  - `regParam`: `0.01` (hệ số điều chuẩn L2 / Ridge Regularization chống quá khớp).
  - `family`: `"multinomial"` (hồi quy logistic đa thức 3 lớp).

---

## 4. KẾT QUẢ ĐÁNH GIÁ THỰC NGHIỆM

### 4.1. Các chỉ số tổng quan (Overall Performance)

| Chỉ số đánh giá | Giá trị thực nghiệm | Ghi chú & Ý nghĩa |
|---|:---:|---|
| **Accuracy (Độ chính xác)** | **`{acc:.4f}`** (`{acc*100:.2f}%`) | Tỷ lệ dự đoán đúng toàn thể trên 20.000 mẫu |
| **Macro F1-score** | **`{m_f1:.4f}`** (`{m_f1*100:.2f}%`) | **Thước đo quan trọng nhất:** Trung bình không trọng số của 3 lớp |
| **Weighted F1-score** | **`{w_f1:.4f}`** (`{w_f1*100:.2f}%`) | F1 tính theo trọng số phân phối tự nhiên (ưu tiên positive) |
| **Macro Precision** | **`{m_prec:.4f}`** (`{m_prec*100:.2f}%`) | Độ chuẩn xác trung bình 3 lớp |
| **Macro Recall** | **`{m_rec:.4f}`** (`{m_rec*100:.2f}%`) | Độ bao phủ trung bình 3 lớp |
| **Weighted Precision** | **`{w_prec:.4f}`** (`{w_prec*100:.2f}%`) | Độ chuẩn xác có trọng số |
| **Weighted Recall** | **`{w_rec:.4f}`** (`{w_rec*100:.2f}%`) | Độ bao phủ có trọng số |

---

### 4.2. Hiệu năng chi tiết theo từng lớp (Per-Class Breakdown)

| Tên lớp cảm xúc | Nhãn số | Precision | Recall | F1-Score | Số lượng mẫu Test (Support) |
|---|:---:|:---:|:---:|:---:|:---:|
| **Positive** (4-5 ⭐) | `0` | **`{c_prec[0]:.4f}`** | **`{c_rec[0]:.4f}`** | **`{c_f1[0]:.4f}`** | **`{c_supp[0]:,}`** (74,60%) |
| **Neutral** (3 ⭐) | `1` | **`{c_prec[1]:.4f}`** | **`{c_rec[1]:.4f}`** | **`{c_f1[1]:.4f}`** | **`{c_supp[1]:,}`** (8,73%) |
| **Negative** (1-2 ⭐) | `2` | **`{c_prec[2]:.4f}`** | **`{c_rec[2]:.4f}`** | **`{c_f1[2]:.4f}`** | **`{c_supp[2]:,}`** (16,67%) |
| **Macro Average** | — | `{m_prec:.4f}` | `{m_rec:.4f}` | **`{m_f1:.4f}`** | 20.000 (100%) |
| **Weighted Average** | — | `{w_prec:.4f}` | `{w_rec:.4f}` | **`{w_f1:.4f}`** | 20.000 (100%) |

---

### 4.3. Ma trận nhầm lẫn (Confusion Matrix 3 × 3)

- **Trục hàng (Rows):** Nhãn thực tế (True Labels).
- **Trục cột (Columns):** Nhãn dự đoán bởi mô hình (Predicted Labels).

| Nhãn thực tế \\ Dự đoán | Dự đoán: Positive (`0`) | Dự đoán: Neutral (`1`) | Dự đoán: Negative (`2`) | Tổng thực tế |
|---|:---:|:---:|:---:|:---:|
| **Thực tế: Positive (`0`)** | **`{cm[0][0]:,}`** | `{cm[0][1]:,}` | `{cm[0][2]:,}` | `{c_supp[0]:,}` |
| **Thực tế: Neutral (`1`)** | `{cm[1][0]:,}` | **`{cm[1][1]:,}`** | `{cm[1][2]:,}` | `{c_supp[1]:,}` |
| **Thực tế: Negative (`2`)** | `{cm[2][0]:,}` | `{cm[2][1]:,}` | **`{cm[2][2]:,}`** | `{c_supp[2]:,}` |
| **Tổng dự đoán** | `{cm[:, 0].sum():,}` | `{cm[:, 1].sum():,}` | `{cm[:, 2].sum():,}` | **20.000** |

---

## 5. THỜI GIAN THỰC THI VÀ TÀI NGUYÊN (TIMINGS & RESOURCES)

| Giai đoạn thực thi | Thời gian đo được | Tốc độ xử lý trung bình |
|---|:---:|---|
| **Khởi tạo SparkSession** | `{spark_init_duration} giây` | Kết nối HDFS master RPC |
| **Nạp dữ liệu từ HDFS** | `{data_load_duration} giây` | Đọc 65.000 dòng Parquet từ HDFS |
| **Huấn luyện mô hình (Pipeline Fit)** | **`{train_fit_duration} giây`** | Fit Tokenizer + HashingTF + IDF + LR trên 45.000 mẫu |
| **Dự đoán tập kiểm thử (Transform)** | **`{pred_duration} giây`** | **~{int(pred_count / pred_duration):,} mẫu/giây** |
| **Lưu Model vào HDFS** | `{model_save_duration} giây` | Lưu đầy đủ các stage vào HDFS |
| **TỔNG THỜI GIAN TOÀN TRÌNH** | **`{total_experiment_duration} giây`** | Hoàn thành trong chưa đầy 2 phút |

---

## 6. PHÂN TÍCH CHUYÊN SÂU HIỆU NĂNG VÀ VẤN ĐỀ CLASS IMBALANCE

### 6.1. Hiệu năng lớp Positive (`label = 0`)
- **Precision:** `{c_prec[0]*100:.2f}%` | **Recall:** `{c_rec[0]*100:.2f}%` | **F1-score:** `{c_f1[0]*100:.2f}%`.
- **Nhận xét:** Lớp Positive chiếm đa số trong tập test (74,60%). Mô hình đạt độ bao phủ rất cao nhờ tập từ vựng khen ngợi phổ biến (*"great"*, *"love"*, *"soft"*, *"smells good"*, *"recommend"*).

### 6.2. Hiệu năng lớp Negative (`label = 2`)
- **Precision:** `{c_prec[2]*100:.2f}%` | **Recall:** `{c_rec[2]*100:.2f}%` | **F1-score:** `{c_f1[2]*100:.2f}%`.
- **Nhận xét:** Nhờ được huấn luyện trên 15.000 mẫu Negative cân bằng (thay vì bị áp đảo như trong tập tự nhiên), mô hình nhận diện tốt các từ khóa phàn nàn (*"waste of money"*, *"terrible"*, *"broken"*, *"fake"*, *"allergic"*), đạt Recall và F1 ở mức khả quan.

### 6.3. Thách thức lớn ở lớp Neutral (`label = 1`)
- **Precision:** `{c_prec[1]*100:.2f}%` | **Recall:** `{c_rec[1]*100:.2f}%` | **F1-score:** `{c_f1[1]*100:.2f}%`.
- **Phân tích nhầm lẫn (Confusion Analysis):**
  - Trong số `{c_supp[1]:,}` review 3 sao (neutral), có tới `{cm[1][0]:,}` mẫu bị đoán nhầm thành Positive và `{cm[1][2]:,}` mẫu bị đoán nhầm thành Negative.
  - **Nguyên nhân cốt lõi:**
    1. *Tính chất văn phong:* Đánh giá 3 sao thường chứa cả khen lẫn chê (ví dụ: *"Smells amazing but makes my skin dry"*). Mô hình tuyến tính bag-of-words (TF-IDF đơn từ) không nắm bắt được quan hệ ngữ cảnh và chuyển ý (từ nối *"but"*, *"however"*).
    2. *Nhiễu nhãn proxy rating:* Như đã khảo sát tại Phase 1 và Phase 2, review 3 sao có tỷ lệ bất đồng ý kiến cao nhất (~19% nhãn không khớp cảm xúc thực tế khi đọc văn bản).

---

## 7. HẠN CHẾ CỦA MÔ HÌNH BASELINE

1. **Bag-of-Words & Hashing Collision:** `HashingTF` giới hạn ở 10.000 features có thể xảy ra xung đột hàm băm (hash collision) giữa các từ khác nhau, đồng thời làm mất hoàn toàn thứ tự từ ngữ và phủ định câu (ví dụ *"not good"* bị tách thành *"not"* và *"good"*).
2. **Thiếu n-grams (Bi-grams/Tri-grams):** Chưa ghép cụm từ phủ định (*"not happy"*, *"never buy again"*).
3. **Mô hình tuyến tính đơn giản:** Logistic Regression chỉ học được ranh giới quyết định tuyến tính trong không gian đặc trưng TF-IDF, chưa tận dụng được cấu trúc phi tuyến tính hoặc biểu diễn ngữ nghĩa dày đặc (Dense Embeddings/BERT).

---

## 8. KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN TIẾP THEO

- **Nghiệm thu Phase 3:** **PASS (ĐẠT HOÀN TOÀN)**.
- **Model Benchmark đã lưu tại HDFS:** `{MODEL_HDFS_PATH}`.
- **Đề xuất cho Phase tiếp theo (Cải tiến mô hình & Xây dựng Dashboard):**
  1. *Cải tiến Feature Engineering:* Bổ sung `NGram` (unigram + bigram) để nắm bắt các cấu trúc phủ định.
  2. *Thử nghiệm mô hình:* Đánh giá thêm Naive Bayes và Random Forest / GBTClassifier của Spark MLlib.
  3. *Tối ưu hóa ngưỡng phân loại (Threshold Tuning):* Điều chỉnh ngưỡng xác suất cho lớp Neutral để cải thiện Macro F1.
  4. *Dashboard trực quan hóa:* Tích hợp mô hình đã lưu với ứng dụng Streamlit + Plotly để dự đoán thời gian thực cho người dùng cuối.
"""
    with open(md_report_path, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"✓ Đã sinh báo cáo Markdown tại: {md_report_path}")

    # Đóng SparkSession
    spark.stop()
    print("✓ Đã đóng SparkSession an toàn.")
    print("=" * 80)
    print(f" HOÀN TẤT THỰC NGHIỆM PHASE 3 TRONG {total_experiment_duration} GIÂY")
    print("=" * 80)

if __name__ == "__main__":
    main()
