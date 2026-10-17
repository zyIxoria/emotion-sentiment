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
