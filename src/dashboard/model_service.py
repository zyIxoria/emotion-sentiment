"""
Module: src.dashboard.model_service
Dịch vụ tải mô hình PySpark PipelineModel, tiền xử lý và suy luận cảm xúc (Sentiment Prediction).
"""
import os
import re
import logging
from typing import Dict, Any, Tuple, Optional, List

logger = logging.getLogger("model_service")

# Ánh xạ nhãn chuẩn hóa xuyên suốt đồ án
LABEL_NAMES = {
    0: "Positive",
    1: "Neutral",
    2: "Negative"
}

LABEL_EMOJIS = {
    0: "😊 Tích cực (Positive)",
    1: "😐 Trung tính (Neutral)",
    2: "😞 Tiêu cực (Negative)"
}

def rating_to_sentiment(rating: int) -> Tuple[int, str]:
    """
    Quy tắc chuyển đổi nhãn từ số sao đánh giá (Ground-truth heuristic):
    - 1, 2 sao: Negative (label 2)
    - 3 sao: Neutral (label 1)
    - 4, 5 sao: Positive (label 0)
    """
    if rating in (1, 2):
        return 2, "Negative"
    elif rating == 3:
        return 1, "Neutral"
    elif rating in (4, 5):
        return 0, "Positive"
    else:
        raise ValueError(f"Rating không hợp lệ: {rating}. Chỉ chấp nhận số nguyên từ 1 đến 5.")

def validate_review_input(text: Optional[str], max_chars: int = 5000) -> Tuple[bool, str, Optional[str]]:
    """
    Kiểm tra và chuẩn hóa chuỗi review đầu vào:
    - Loại bỏ khoảng trắng thừa.
    - Phát hiện review rỗng hoặc chỉ có khoảng trắng.
    - Xử lý văn bản quá dài.
    - Bảo toàn ký tự đặc biệt / emoji để RegexTokenizer xử lý.
    """
    if text is None:
        return False, "Nội dung đánh giá không được để trống.", None

    cleaned = text.strip()
    if not cleaned:
        return False, "Nội dung đánh giá không được để trống (hoặc chỉ chứa khoảng trắng).", None

    # Kiểm tra độ dài tối đa
    warning = None
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]
        warning = f"Văn bản vượt quá {max_chars} ký tự, đã được cắt ngắn an toàn để tối ưu suy luận."

    return True, cleaned, warning

def load_phase4_best_model(spark_session, model_path: Optional[str] = None):
    """
    Tải PipelineModel tối ưu từ Phase 4 (M2c_LR_elastic).
    Đường dẫn ưu tiên: Tham số truyền vào > Biến môi trường MODEL_HDFS_PATH > Mặc định HDFS.
    """
    if model_path is None:
        model_path = os.environ.get(
            "MODEL_HDFS_PATH",
            "hdfs://localhost:9000/bigdata/models/phase4_best_model"
        )

    try:
        from pyspark.ml import PipelineModel
    except ImportError as e:
        raise RuntimeError("PySpark ML chưa được cài đặt trong môi trường hiện tại.") from e

    try:
        logger.info("Đang nạp PipelineModel từ: %s", model_path)
        model = PipelineModel.load(model_path)
        return model, model_path
    except Exception as e:
        err_msg = str(e)
        if "ConnectException" in err_msg or "Connection refused" in err_msg:
            raise ConnectionError(
                f"Không thể kết nối HDFS khi tải model tại '{model_path}'. "
                "Vui lòng kiểm tra HDFS NameNode (port 9000) đã hoạt động chưa."
            ) from e
        elif "does not exist" in err_msg.lower() or "filenotfoundexception" in err_msg.lower() or "invalidinputexception" in err_msg.lower():
            raise FileNotFoundError(
                f"Không tìm thấy thư mục model tại '{model_path}'. "
                "Mô hình Phase 4 chưa được lưu lên HDFS. Vui lòng chạy script huấn luyện/lưu model lên HDFS."
            ) from e
        else:
            raise RuntimeError(f"Lỗi khi đọc PipelineModel từ '{model_path}': {err_msg}") from e

def predict_single_review(
    spark_session,
    model,
    review_text: str,
    selected_rating: Optional[int] = None
) -> Dict[str, Any]:
    """
    Thực hiện suy luận cảm xúc cho một đoạn văn bản review:
    1. Kiểm tra tính hợp lệ của input.
    2. Tạo Spark DataFrame 1 dòng với cột 'full_text'.
    3. Chạy qua PipelineModel (RegexTokenizer -> StopWords -> HashingTF -> IDF -> LogisticRegression).
    4. Trích xuất nhãn dự đoán, xác suất từng lớp (softmax) và so sánh với rating (nếu có).
    """
    is_valid, cleaned_text, length_warning = validate_review_input(review_text)
    if not is_valid:
        return {
            "success": False,
            "error": cleaned_text
        }

    try:
        # Tạo DataFrame đầu vào đúng schema với tên cột 'full_text' theo đúng PipelineModel Phase 4
        input_df = spark_session.createDataFrame([(cleaned_text,)], ["full_text"])

        # Thực thi biến đổi toàn trình qua PipelineModel
        result_df = model.transform(input_df)

        # Thu thập kết quả suy luận
        row = result_df.select("prediction", "probability", "rawPrediction").first()

        pred_idx = int(row["prediction"])
        pred_label = LABEL_NAMES.get(pred_idx, "Unknown")
        pred_label_display = LABEL_EMOJIS.get(pred_idx, pred_label)

        # Lấy xác suất softmax từ probability vector
        prob_vector = row["probability"].toArray()
        probabilities = {
            "Positive": float(prob_vector[0]),
            "Neutral": float(prob_vector[1]),
            "Negative": float(prob_vector[2])
        }
        confidence = float(prob_vector[pred_idx])

        # Lấy raw prediction margin values
        raw_pred = [float(v) for v in row["rawPrediction"].toArray()]

        # Đối chiếu với rating nếu người dùng cung cấp
        rating_comparison = None
        if selected_rating is not None and 1 <= selected_rating <= 5:
            r_idx, r_label = rating_to_sentiment(selected_rating)
            matches_rating = (r_idx == pred_idx)
            rating_comparison = {
                "selected_rating": selected_rating,
                "rating_derived_label": r_label,
                "rating_derived_idx": r_idx,
                "matches_prediction": matches_rating
            }

        return {
            "success": True,
            "input_text": cleaned_text,
            "prediction_index": pred_idx,
            "predicted_label": pred_label,
            "predicted_display": pred_label_display,
            "confidence": confidence,
            "probabilities": probabilities,
            "raw_prediction": raw_pred,
            "rating_comparison": rating_comparison,
            "warning": length_warning
        }
    except Exception as e:
        logger.error("Lỗi khi suy luận mô hình: %s", e)
        return {
            "success": False,
            "error": f"Lỗi trong quá trình suy luận mô hình: {str(e)}"
        }
