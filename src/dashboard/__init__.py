"""
Package: src.dashboard
Module initialization for Streamlit Sentiment Analysis Dashboard.
"""

LABEL_MAPPING = {
    0: "Positive",
    1: "Neutral",
    2: "Negative"
}

SENTIMENT_TO_LABEL = {
    "Positive": 0,
    "Neutral": 1,
    "Negative": 2
}

DEFAULT_HDFS_URI = "hdfs://localhost:9000"
DEFAULT_MODEL_PATH = "hdfs://localhost:9000/bigdata/models/phase4_best_model"
DEFAULT_NATURAL_100K_PATH = "hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet"
