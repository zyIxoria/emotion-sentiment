"""
Script: test_pyspark_pipeline.py
Mục đích: Pipeline PySpark kiểm thử toàn trình từ HDFS -> Preprocessing -> TF-IDF -> Logistic Regression
Hỗ trợ: Xử lý class imbalance qua weightCol (Inverse Class Frequency)
"""
import sys
import time
import os

def run_pipeline():
    print("=" * 60)
    print(" KHỞI CHẠY PYSPARK SENTIMENT ANALYSIS PIPELINE")
    print("=" * 60)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    # 1. Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("AmazonReviewsSentiment-TestPipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ Master: {spark.sparkContext.master}")

    # 2. Đọc dữ liệu từ HDFS
    hdfs_url = "hdfs://localhost:9000/data/amazon-reviews/raw/all_beauty_25k.jsonl"
    local_url = "data/raw/all_beauty_25k.jsonl"

    print(f"\n[1/5] Đang nạp dữ liệu...")
    try:
        df = spark.read.json(hdfs_url)
        print(f"✓ Đã đọc thành công từ HDFS: {hdfs_url}")
    except Exception as e:
        print(f"⚠ Không kết nối được HDFS ({e}), chuyển sang đọc file cục bộ: {local_url}")
        df = spark.read.json(local_url)

    total_records = df.count()
    print(f"✓ Tổng số bản ghi nạp vào Spark: {total_records:,}")

    # 3. Tiền xử lý dữ liệu và Gán nhãn cảm xúc
    print(f"\n[2/5] Tiền xử lý và gán nhãn 3 lớp...")
    # Ghép title và text thành full_text
    df_clean = df.withColumn("title_clean", F.coalesce(F.col("title"), F.lit(""))) \
                 .withColumn("text_clean", F.coalesce(F.col("text"), F.lit(""))) \
                 .withColumn("full_text", F.trim(F.concat_ws(" ", F.col("title_clean"), F.col("text_clean")))) \
                 .filter(F.length(F.col("full_text")) > 5)

    # Gán nhãn 3 lớp: Positive=0.0, Neutral=1.0, Negative=2.0
    df_labeled = df_clean.withColumn("rating_num", F.col("rating").cast(DoubleType())) \
                         .withColumn("label", 
                             F.when(F.col("rating_num") > 3.0, 0.0)
                              .when(F.col("rating_num") == 3.0, 1.0)
                              .otherwise(2.0)
                         )

    # Thống kê phân bố lớp
    class_counts = df_labeled.groupBy("label").count().collect()
    count_map = {int(r["label"]): r["count"] for r in class_counts}
    n_pos = count_map.get(0, 0)
    n_neu = count_map.get(1, 0)
    n_neg = count_map.get(2, 0)
    n_total = n_pos + n_neu + n_neg

    print(f"  - Positive (0.0): {n_pos:>6,} ({n_pos/n_total*100:.1f}%)")
    print(f"  - Neutral  (1.0): {n_neu:>6,} ({n_neu/n_total*100:.1f}%)")
    print(f"  - Negative (2.0): {n_neg:>6,} ({n_neg/n_total*100:.1f}%)")

    # 4. Tính toán Trọng số xử lý Class Imbalance (Inverse Class Frequency)
    # W_c = N / (K * N_c)
    K = 3.0
    w_pos = n_total / (K * max(n_pos, 1))
    w_neu = n_total / (K * max(n_neu, 1))
    w_neg = n_total / (K * max(n_neg, 1))

    print(f"\n[3/5] Áp dụng trọng số lớp (Class Weights):")
    print(f"  - Weight Positive : {w_pos:.2f}")
    print(f"  - Weight Neutral  : {w_neu:.2f}")
    print(f"  - Weight Negative : {w_neg:.2f}")

    df_weighted = df_labeled.withColumn("class_weight",
        F.when(F.col("label") == 0.0, F.lit(w_pos))
         .when(F.col("label") == 1.0, F.lit(w_neu))
         .otherwise(F.lit(w_neg))
    )

    # 5. Phân chia Train/Test 80-20
    train_df, test_df = df_weighted.randomSplit([0.8, 0.2], seed=42)
    print(f"\n[4/5] Chia tập Train ({train_df.count():,} mẫu) và Test ({test_df.count():,} mẫu)...")

    # 6. Pipeline: Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
    print(f"\n[5/5] Xây dựng ML Pipeline & Huấn luyện...")
    tokenizer = RegexTokenizer(inputCol="full_text", outputCol="words", pattern="\\W+", minTokenLength=2)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=50,
        regParam=0.01,
        elasticNetParam=0.0
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

    start_train = time.time()
    model = pipeline.fit(train_df)
    train_time = time.time() - start_train
    print(f"✓ Huấn luyện hoàn tất trong {train_time:.2f} giây!")

    # 7. Đánh giá mô hình trên tập Test
    print("\n" + "=" * 60)
    print(" KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH TRÊN TẬP TEST")
    print("=" * 60)

    predictions = model.transform(test_df)

    eval_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    eval_f1_weighted = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    acc = eval_acc.evaluate(predictions)
    f1_weighted = eval_f1_weighted.evaluate(predictions)

    # Tính Precision, Recall, F1 cho từng lớp
    labels = [0.0, 1.0, 2.0]
    names = {0.0: "Positive", 1.0: "Neutral", 2.0: "Negative"}
    f1_scores = []

    print(f"\n{'Class':<12} | {'Precision':<10} | {'Recall':<10} | {'F1-score':<10}")
    print("-" * 50)

    for l in labels:
        eval_p = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="precisionByLabel", metricLabel=l)
        eval_r = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="recallByLabel", metricLabel=l)
        eval_f = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1", metricLabel=l)

        p = eval_p.evaluate(predictions)
        r = eval_r.evaluate(predictions)
        f = eval_f.evaluate(predictions)
        f1_scores.append(f)
        print(f"{names[l]:<12} | {p*100:>8.2f}% | {r*100:>8.2f}% | {f*100:>8.2f}%")

    macro_f1 = sum(f1_scores) / len(f1_scores)

    print("-" * 50)
    print(f"Overall Accuracy : {acc*100:.2f}%")
    print(f"Weighted F1-score: {f1_weighted*100:.2f}%")
    print(f"Macro F1-score   : {macro_f1*100:.2f}% (Metric trọng tâm chống lệch lớp)")
    print("=" * 60)

    spark.stop()

if __name__ == '__main__':
    run_pipeline()
