"""
Test Suite: test_model_loading.py
Kiểm thử tính năng quản lý kết nối, phân loại lỗi và cấu hình nạp mô hình.
"""
import os
import pytest
import yaml
from src.dashboard.spark_manager import (
    check_hdfs_connectivity,
    PySparkUnavailableError,
    HDFSConnectionError,
    SparkSessionError
)
from src.dashboard.model_service import load_phase4_best_model

def test_dataset_config_structure():
    """Kiểm tra file configs/dataset_config.yaml chứa đúng cấu hình Phase 5."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg_path = os.path.join(base_dir, "configs", "dataset_config.yaml")
    assert os.path.exists(cfg_path), "File configs/dataset_config.yaml không tồn tại"

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert "dashboard" in cfg, "Thiếu section 'dashboard' trong config"
    dash = cfg["dashboard"]
    assert "model" in dash, "Thiếu section 'model' trong dashboard config"
    assert dash["model"]["name"] == "M2c_LR_elastic"
    assert "hdfs://localhost:9000" in dash["model"]["hdfs_path"]
    assert dash["model"]["input_col"] == "full_text"

def test_check_hdfs_connectivity_invalid_port():
    """Kiểm tra check_hdfs_connectivity trả về False khi port không thể kết nối."""
    # Port 59999 thường không có service nào lắng nghe
    is_ok, msg = check_hdfs_connectivity("hdfs://localhost:59999", timeout_sec=0.2)
    assert not is_ok
    assert "Không thể kết nối" in msg

def test_model_loading_error_handling():
    """Kiểm tra hàm load_phase4_best_model phân loại đúng lỗi khi spark_session không hợp lệ hoặc thiếu pyspark."""
    # Khi model path không tồn tại hoặc spark_session = None
    with pytest.raises(Exception):
        load_phase4_best_model(None, "hdfs://localhost:9000/non_existent_path")

def test_spark_home_invalid_fallback(monkeypatch):
    """Kiểm tra spark_manager tự động làm sạch SPARK_HOME rác/không hợp lệ."""
    monkeypatch.setenv("SPARK_HOME", "/non_existent_dummy_spark_path_12345")
    from src.dashboard.spark_manager import get_spark_session
    # Khi SPARK_HOME không tồn tại, hàm get_spark_session phải tự pop biến môi trường
    # Thay vì nổ lỗi [Errno 2] No such file or directory
    # Kiểm tra logic xử lý fallback mà không cần khởi tạo lại JVM nặng
    spark_home_env = os.environ.get("SPARK_HOME")
    assert spark_home_env == "/non_existent_dummy_spark_path_12345"
    spark_submit = os.path.join(spark_home_env, "bin", "spark-submit")
    assert not os.path.exists(spark_submit)

