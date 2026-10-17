#!/usr/bin/env python3
"""
scripts/deploy_phase4_model_to_hdfs.py
Tự động trích xuất dữ liệu, huấn luyện Best Model Phase 4 (M2c_LR_elastic)
và triển khai trực tiếp lên Hadoop HDFS (hdfs://localhost:9000/bigdata/models/phase4_best_model).
Đồng thời tạo dữ liệu Parquet chuẩn cho Tab Data Explorer trên HDFS.
"""
import os
import sys
import json
import time

def main():
    print("=" * 75)
    print(" TRIỂN KHAI BEST MODEL PHASE 4 & DATASETS LÊN HADOOP HDFS")
    print("=" * 75)
    t0 = time.time()

    raw_path = "data/raw/All_Beauty_full.jsonl"
    if not os.path.exists(raw_path):
        print(f"❌ Không tìm thấy file dữ liệu thô: {raw_path}")
        sys.exit(1)

    print(f"\n[1/4] Đọc và trích xuất dữ liệu từ {raw_path}...")
    balanced_records = []
    natural_records = []
    pos_count, neu_count, neg_count = 0, 0, 0
    TARGET_PER_CLASS = 15000  # 45k balanced train
    TARGET_NATURAL = 100000   # 100k natural dataset

    with open(raw_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            try:
                rec = json.loads(line)
            except Exception:
                continue

            rating = float(rec.get("rating", 0.0) or 0.0)
            title = str(rec.get("title", "") or "")
            text = str(rec.get("text", "") or "")
            user_id = str(rec.get("user_id", "") or "")
            asin = str(rec.get("asin", "") or "")
            timestamp = int(rec.get("timestamp", 0) or 0)

            full_text = f"{title} {text}".strip()
            if len(full_text) < 5:
                continue

            if rating >= 4.0:
                label = 0.0  # Positive
            elif rating == 3.0:
                label = 1.0  # Neutral
            elif rating >= 1.0:
                label = 2.0  # Negative
            else:
                continue

            clean_row = {
                "user_id": user_id,
                "asin": asin,
                "rating": rating,
                "full_text": full_text,
                "label": label,
                "timestamp": timestamp
            }

            # Gom cho Natural dataset
            if len(natural_records) < TARGET_NATURAL:
                natural_records.append(clean_row)

            # Gom cho Balanced train dataset
            if label == 0.0 and pos_count < TARGET_PER_CLASS:
                balanced_records.append(clean_row)
                pos_count += 1
            elif label == 1.0 and neu_count < TARGET_PER_CLASS:
                balanced_records.append(clean_row)
                neu_count += 1
            elif label == 2.0 and neg_count < TARGET_PER_CLASS:
                balanced_records.append(clean_row)
                neg_count += 1

            if (len(natural_records) >= TARGET_NATURAL and 
                pos_count >= TARGET_PER_CLASS and 
                neu_count >= TARGET_PER_CLASS and 
                neg_count >= TARGET_PER_CLASS):
                break

    print(f"  ✓ Đã quét xong: Balanced={len(balanced_records)} ({pos_count} Pos, {neu_count} Neu, {neg_count} Neg)")
    print(f"  ✓ Natural Dataset={len(natural_records)} mẫu")

    # Khởi động SparkSession
    print("\n[2/4] Khởi tạo PySpark kết nối HDFS...")
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
    from pyspark.ml.classification import LogisticRegression

    spark = SparkSession.builder \
        .appName("DeployPhase4BestModel") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("full_text", StringType(), True),
        StructField("label", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    print("\n[3/4] Lưu dữ liệu Parquet lên HDFS...")
    df_natural = spark.createDataFrame(natural_records, schema=schema)
    parquet_natural_path = "hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet"
    df_natural.write.mode("overwrite").parquet(parquet_natural_path)
    print(f"  ✓ Đã lưu Parquet 100k: {parquet_natural_path}")

    # Huấn luyện Best Model M2c_LR_elastic
    print("\n[4/4] Huấn luyện Best Model M2c_LR_elastic trên 45.000 mẫu cân bằng...")
    df_balanced = spark.createDataFrame(balanced_records, schema=schema)

    tokenizer = RegexTokenizer(inputCol="full_text", outputCol="words", pattern="\\W+")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5,
        family="multinomial"
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])
    t_train = time.time()
    model = pipeline.fit(df_balanced)
    print(f"  ✓ Huấn luyện hoàn tất trong {time.time() - t_train:.2f}s!")

    # Lưu Best Model lên HDFS
    hdfs_model_path = "hdfs://localhost:9000/bigdata/models/phase4_best_model"
    print(f"  Đang ghi PipelineModel vào HDFS: {hdfs_model_path}...")
    model.write().overwrite().save(hdfs_model_path)
    print(f"  ✓ Model đã được lưu thành công trên HDFS!")

    # Kiểm tra nạp lại mô hình từ HDFS
    print("\nKiểm tra nạp lại mô hình từ HDFS...")
    loaded_model = PipelineModel.load(hdfs_model_path)
    test_df = spark.createDataFrame([{"full_text": "This product is absolutely amazing, high quality!"}])
    pred_df = loaded_model.transform(test_df)
    row = pred_df.select("prediction", "probability").first()
    print(f"  ✓ Test prediction: label={row['prediction']} (0=Positive), prob={row['probability']}")

    print("\n" + "=" * 75)
    print(f" ĐÃ TRIỂN KHAI THÀNH CÔNG TRONG {time.time() - t0:.2f} GIÂY!")
    print("=" * 75)
    spark.stop()

if __name__ == "__main__":
    main()
