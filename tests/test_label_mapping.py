"""
Test Suite: test_label_mapping.py
Kiểm thử tính đúng đắn của quy tắc ánh xạ nhãn cảm xúc và số sao rating.
"""
import pytest
from src.dashboard.model_service import rating_to_sentiment, LABEL_NAMES

def test_positive_ratings():
    """Kiểm tra rating 4 và 5 sao phải ánh xạ sang Positive (label 0)."""
    idx_4, label_4 = rating_to_sentiment(4)
    assert idx_4 == 0
    assert label_4 == "Positive"

    idx_5, label_5 = rating_to_sentiment(5)
    assert idx_5 == 0
    assert label_5 == "Positive"

def test_neutral_ratings():
    """Kiểm tra rating 3 sao phải ánh xạ sang Neutral (label 1)."""
    idx_3, label_3 = rating_to_sentiment(3)
    assert idx_3 == 1
    assert label_3 == "Neutral"

def test_negative_ratings():
    """Kiểm tra rating 1 và 2 sao phải ánh xạ sang Negative (label 2)."""
    idx_1, label_1 = rating_to_sentiment(1)
    assert idx_1 == 2
    assert label_1 == "Negative"

    idx_2, label_2 = rating_to_sentiment(2)
    assert idx_2 == 2
    assert label_2 == "Negative"

def test_invalid_ratings():
    """Kiểm tra ngoại lệ khi rating ngoài khoảng [1, 5]."""
    with pytest.raises(ValueError):
        rating_to_sentiment(0)

    with pytest.raises(ValueError):
        rating_to_sentiment(6)

    with pytest.raises(ValueError):
        rating_to_sentiment(-1)

def test_label_names_consistency():
    """Kiểm tra tính nhất quán của LABEL_NAMES với Phase 4."""
    assert LABEL_NAMES[0] == "Positive"
    assert LABEL_NAMES[1] == "Neutral"
    assert LABEL_NAMES[2] == "Negative"
    assert len(LABEL_NAMES) == 3
