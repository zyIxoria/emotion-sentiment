"""
Test Suite: test_prediction.py
Kiểm thử tiền xử lý đầu vào, các trường hợp biên và tính toàn vẹn của dữ liệu báo cáo Phase 3-4.
"""
import os
import pytest
from src.dashboard.model_service import validate_review_input
from src.dashboard.data_service import load_offline_reports

def test_validate_input_normal():
    """Kiểm tra đầu vào review thông thường."""
    is_valid, cleaned, warning = validate_review_input("This is a great product!")
    assert is_valid
    assert cleaned == "This is a great product!"
    assert warning is None

def test_validate_input_empty_and_whitespace():
    """Kiểm tra xử lý văn bản rỗng hoặc chỉ chứa khoảng trắng."""
    is_valid, msg, warning = validate_review_input("")
    assert not is_valid
    assert "trống" in msg

    is_valid, msg, warning = validate_review_input("    \n\t   ")
    assert not is_valid
    assert "trống" in msg

    is_valid, msg, warning = validate_review_input(None)
    assert not is_valid
    assert "trống" in msg

def test_validate_input_special_characters_and_emojis():
    """Kiểm tra văn bản chứa emoji và ký tự đặc biệt được bảo toàn."""
    text = "Amazing cream! 😍✨ Highly recommended for dry skin (10/10) - $15.99."
    is_valid, cleaned, warning = validate_review_input(text)
    assert is_valid
    assert cleaned == text
    assert "😍" in cleaned
    assert warning is None

def test_validate_input_long_text_truncation():
    """Kiểm tra xử lý văn bản vượt quá giới hạn ký tự tối đa."""
    long_text = "Good " * 2000  # 10.000 ký tự
    is_valid, cleaned, warning = validate_review_input(long_text, max_chars=1000)
    assert is_valid
    assert len(cleaned) == 1000
    assert warning is not None
    assert "vượt quá 1000 ký tự" in warning

def test_offline_reports_loading():
    """Kiểm tra việc nạp dữ liệu báo cáo thực nghiệm từ Phase 3 và Phase 4."""
    reports = load_offline_reports()
    assert reports["available"] is True, f"Không nạp được reports: {reports.get('error')}"

    # Kiểm tra Baseline Metrics
    b_metrics = reports["baseline_metrics"]
    assert b_metrics is not None
    assert "metrics_overall" in b_metrics
    assert b_metrics["metrics_overall"]["accuracy"] == 0.7355
    assert b_metrics["metrics_overall"]["macro_f1"] == 0.6088

    # Kiểm tra Phase 4 Metrics DataFrame
    df_p4 = reports["phase4_metrics_df"]
    assert df_p4 is not None
    assert len(df_p4) >= 9  # 8 mô hình validation + 1 final test
    assert "M2c_LR_elastic" in df_p4["Experiment_ID"].values

    # Kiểm tra Final Test Confusion Matrix
    cm_df = reports["final_test_cm"]
    assert cm_df is not None
    assert cm_df.shape[0] == 3  # 3 hàng
    # Kiểm tra các cột trong Confusion Matrix
    assert "True_Label" in cm_df.columns
    assert "Pred_Positive_0" in cm_df.columns
    assert "Pred_Neutral_1" in cm_df.columns
    assert "Pred_Negative_2" in cm_df.columns
