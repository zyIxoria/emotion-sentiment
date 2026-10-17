"""
Module: src.dashboard.data_service
Truy vấn dữ liệu tổng hợp từ HDFS qua Spark và đọc báo cáo metrics từ Phase 3-4.
"""
import os
import json
import logging
from typing import Dict, Any, Optional, Tuple, List
import pandas as pd

logger = logging.getLogger("data_service")

def get_base_dir() -> str:
    """Trả về thư mục gốc của project (emotion-sentiment)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_offline_reports(base_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Nạp toàn bộ số liệu thực nghiệm đã ghi nhận từ Phase 3 và Phase 4.
    Đảm bảo KHÔNG chạy lại test set khi mở dashboard.
    """
    if base_dir is None:
        base_dir = get_base_dir()

    reports_dir = os.path.join(base_dir, "reports", "models")
    cm_dir = os.path.join(reports_dir, "phase4_confusion_matrices")

    result = {
        "baseline_metrics": None,
        "baseline_cm": None,
        "phase4_metrics_df": None,
        "final_test_cm": None,
        "available": False,
        "error": None
    }

    try:
        # 1. Đọc Baseline Metrics JSON
        baseline_json_path = os.path.join(reports_dir, "baseline_metrics.json")
        if os.path.exists(baseline_json_path):
            with open(baseline_json_path, "r", encoding="utf-8") as f:
                result["baseline_metrics"] = json.load(f)

        # 2. Đọc Baseline Confusion Matrix CSV
        baseline_cm_path = os.path.join(reports_dir, "baseline_confusion_matrix.csv")
        if os.path.exists(baseline_cm_path):
            result["baseline_cm"] = pd.read_csv(baseline_cm_path)

        # 3. Đọc Phase 4 Metrics CSV
        phase4_csv_path = os.path.join(reports_dir, "phase4_metrics.csv")
        if os.path.exists(phase4_csv_path):
            result["phase4_metrics_df"] = pd.read_csv(phase4_csv_path)

        # 4. Đọc Phase 4 Final Test Confusion Matrix CSV
        final_cm_path = os.path.join(cm_dir, "cm_final_test_best_model.csv")
        if os.path.exists(final_cm_path):
            result["final_test_cm"] = pd.read_csv(final_cm_path)

        result["available"] = (
            result["baseline_metrics"] is not None and
            result["phase4_metrics_df"] is not None
        )
    except Exception as e:
        logger.error("Lỗi khi nạp báo cáo metrics: %s", e)
        result["error"] = str(e)

    return result

def get_hdfs_dataset_overview(
    spark_session,
    parquet_path: str = "hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet"
) -> Dict[str, Any]:
    """
    Đọc dữ liệu Parquet từ HDFS và tính toán các chỉ số tổng hợp phân tán.
    LƯU Ý: Tuyệt đối không gọi .toPandas() hoặc .collect() trên toàn bộ dataset lớn.
    Chỉ gom nhóm (groupBy) trên Spark rồi mới chuyển bảng tổng hợp nhỏ sang Pandas.
    """
    try:
        from pyspark.sql import functions as F
    except ImportError:
        return {"success": False, "error": "PySpark không khả dụng."}

    try:
        df = spark_session.read.parquet(parquet_path)

        # 1. Tổng số dòng
        total_count = df.count()

        # 2. Phân phối Sentiment (3 lớp)
        if "sentiment" in df.columns:
            sentiment_agg = df.groupBy("sentiment").count().orderBy(F.col("count").desc())
            df_sentiment = sentiment_agg.toPandas()
        elif "label" in df.columns:
            sentiment_agg = df.groupBy("label").count().orderBy("label")
            df_sentiment = sentiment_agg.toPandas()
            label_map = {0.0: "Positive", 1.0: "Neutral", 2.0: "Negative", 0: "Positive", 1: "Neutral", 2: "Negative"}
            df_sentiment["sentiment"] = df_sentiment["label"].map(label_map)
        else:
            df_sentiment = pd.DataFrame(columns=["sentiment", "count"])

        # 3. Phân phối Rating (1-5 sao)
        rating_agg = df.groupBy("rating").count().orderBy("rating")
        df_rating = rating_agg.toPandas()

        # 4. Lấy Schema dạng danh sách trường
        schema_info = [{"Field": f.name, "Type": str(f.dataType), "Nullable": f.nullable} for f in df.schema.fields]

        return {
            "success": True,
            "total_count": total_count,
            "sentiment_distribution": df_sentiment,
            "rating_distribution": df_rating,
            "schema_fields": schema_info,
            "path": parquet_path
        }
    except Exception as e:
        logger.error("Lỗi khi đọc Parquet từ HDFS: %s", e)
        return {
            "success": False,
            "error": f"Không thể đọc dataset từ HDFS ({parquet_path}): {str(e)}"
        }

def mask_user_id(val: Any) -> str:
    """Che giấu mã định danh user_id để bảo vệ quyền riêng tư."""
    s = str(val or "")
    if len(s) > 8:
        return f"{s[:4]}...{s[-4:]}"
    return s

def get_data_explorer_sample(
    spark_session,
    parquet_path: str = "hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet",
    n_samples: int = 15,
    sentiment_filter: Optional[str] = None,
    rating_filter: Optional[int] = None
) -> Dict[str, Any]:
    """
    Trích xuất một mẫu nhỏ phục vụ duyệt dữ liệu (Data Explorer).
    Áp dụng filter trực tiếp trên Spark DataFrame và chỉ .limit() số dòng cần thiết.
    """
    try:
        from pyspark.sql import functions as F
    except ImportError:
        return {"success": False, "error": "PySpark không khả dụng."}

    try:
        df = spark_session.read.parquet(parquet_path)

        # Áp dụng bộ lọc
        if sentiment_filter and sentiment_filter != "Tất cả":
            if "sentiment" in df.columns:
                df = df.filter(F.col("sentiment") == sentiment_filter)
            elif "label" in df.columns:
                label_filter_map = {"Positive": 0.0, "Neutral": 1.0, "Negative": 2.0}
                if sentiment_filter in label_filter_map:
                    df = df.filter(F.col("label") == label_filter_map[sentiment_filter])

        if rating_filter is not None and rating_filter != 0:
            df = df.filter(F.col("rating") == rating_filter)

        # Giới hạn số lượng mẫu an toàn
        safe_limit = max(1, min(int(n_samples), 100))
        sample_df = df.limit(safe_limit).toPandas()

        if "sentiment" not in sample_df.columns and "label" in sample_df.columns:
            label_map = {0.0: "Positive", 1.0: "Neutral", 2.0: "Negative", 0: "Positive", 1: "Neutral", 2: "Negative"}
            sample_df["sentiment"] = sample_df["label"].map(label_map)

        # Che user_id nếu có
        if "user_id" in sample_df.columns:
            sample_df["user_id"] = sample_df["user_id"].apply(mask_user_id)

        # Chọn các cột trọng tâm nếu có
        display_cols = [c for c in ["review_id", "rating", "sentiment", "title", "text", "full_text", "user_id", "verified_purchase"] if c in sample_df.columns]
        if display_cols:
            sample_df = sample_df[display_cols]

        return {
            "success": True,
            "sample_df": sample_df,
            "returned_count": len(sample_df)
        }
    except Exception as e:
        logger.error("Lỗi khi trích xuất mẫu dữ liệu: %s", e)
        return {
            "success": False,
            "error": f"Lỗi khi trích xuất mẫu từ HDFS: {str(e)}"
        }
