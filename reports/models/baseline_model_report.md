# BÁO CÁO KẾT QUẢ HUẤN LUYỆN MÔ HÌNH BASELINE TF-IDF + LOGISTIC REGRESSION (PHASE 3)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Mô hình:** Baseline `TF-IDF + Multinomial Logistic Regression` (PySpark MLlib)  
**Môi trường:** Apache Spark 3.5.5 trên Hadoop HDFS 3.3.6 (WSL2 Ubuntu 24.04 LTS)  
**Thời điểm thực hiện:** 2026-10-16 05:26:54  
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
| **Balanced Train** | `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_balanced_train.parquet` | **45.000** | 15.000 (33,33%) | 15.000 (33,33%) | 15.000 (33,33%) | **Huấn luyện mô hình** |
| **Natural Test** | `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_natural_test.parquet` | **20.000** | 14.920 (74,60%) | 1.746 (8,73%) | 3.334 (16,67%) | **Đánh giá độc lập** |

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
RegexTokenizer (pattern="\W+", toLowercase=True)
      │
      ▼
StopWordsRemover (stopWords=English 181 words)
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
| **Accuracy (Độ chính xác)** | **`0.7355`** (`73.55%`) | Tỷ lệ dự đoán đúng toàn thể trên 20.000 mẫu |
| **Macro F1-score** | **`0.6088`** (`60.88%`) | **Thước đo quan trọng nhất:** Trung bình không trọng số của 3 lớp |
| **Weighted F1-score** | **`0.7707`** (`77.07%`) | F1 tính theo trọng số phân phối tự nhiên (ưu tiên positive) |
| **Macro Precision** | **`0.6001`** (`60.01%`) | Độ chuẩn xác trung bình 3 lớp |
| **Macro Recall** | **`0.6746`** (`67.46%`) | Độ bao phủ trung bình 3 lớp |
| **Weighted Precision** | **`0.8348`** (`83.48%`) | Độ chuẩn xác có trọng số |
| **Weighted Recall** | **`0.7355`** (`73.55%`) | Độ bao phủ có trọng số |

---

### 4.2. Hiệu năng chi tiết theo từng lớp (Per-Class Breakdown)

| Tên lớp cảm xúc | Nhãn số | Precision | Recall | F1-Score | Số lượng mẫu Test (Support) |
|---|:---:|:---:|:---:|:---:|:---:|
| **Positive** (4-5 ⭐) | `0` | **`0.9551`** | **`0.7667`** | **`0.8506`** | **`14,920`** (74,60%) |
| **Neutral** (3 ⭐) | `1` | **`0.2338`** | **`0.5802`** | **`0.3333`** | **`1,746`** (8,73%) |
| **Negative** (1-2 ⭐) | `2` | **`0.6115`** | **`0.6770`** | **`0.6426`** | **`3,334`** (16,67%) |
| **Macro Average** | — | `0.6001` | `0.6746` | **`0.6088`** | 20.000 (100%) |
| **Weighted Average** | — | `0.8348` | `0.7355` | **`0.7707`** | 20.000 (100%) |

---

### 4.3. Ma trận nhầm lẫn (Confusion Matrix 3 × 3)

- **Trục hàng (Rows):** Nhãn thực tế (True Labels).
- **Trục cột (Columns):** Nhãn dự đoán bởi mô hình (Predicted Labels).

| Nhãn thực tế \ Dự đoán | Dự đoán: Positive (`0`) | Dự đoán: Neutral (`1`) | Dự đoán: Negative (`2`) | Tổng thực tế |
|---|:---:|:---:|:---:|:---:|
| **Thực tế: Positive (`0`)** | **`11,439`** | `2,473` | `1,008` | `14,920` |
| **Thực tế: Neutral (`1`)** | `307` | **`1,013`** | `426` | `1,746` |
| **Thực tế: Negative (`2`)** | `231` | `846` | **`2,257`** | `3,334` |
| **Tổng dự đoán** | `11,977` | `4,332` | `3,691` | **20.000** |

---

## 5. THỜI GIAN THỰC THI VÀ TÀI NGUYÊN (TIMINGS & RESOURCES)

| Giai đoạn thực thi | Thời gian đo được | Tốc độ xử lý trung bình |
|---|:---:|---|
| **Khởi tạo SparkSession** | `3.44 giây` | Kết nối HDFS master RPC |
| **Nạp dữ liệu từ HDFS** | `4.14 giây` | Đọc 65.000 dòng Parquet từ HDFS |
| **Huấn luyện mô hình (Pipeline Fit)** | **`6.42 giây`** | Fit Tokenizer + HashingTF + IDF + LR trên 45.000 mẫu |
| **Dự đoán tập kiểm thử (Transform)** | **`0.94 giây`** | **~21,276 mẫu/giây** |
| **Lưu Model vào HDFS** | `7.19 giây` | Lưu đầy đủ các stage vào HDFS |
| **TỔNG THỜI GIAN TOÀN TRÌNH** | **`21.23 giây`** | Hoàn thành trong chưa đầy 2 phút |

---

## 6. PHÂN TÍCH CHUYÊN SÂU HIỆU NĂNG VÀ VẤN ĐỀ CLASS IMBALANCE

### 6.1. Hiệu năng lớp Positive (`label = 0`)
- **Precision:** `95.51%` | **Recall:** `76.67%` | **F1-score:** `85.06%`.
- **Nhận xét:** Lớp Positive chiếm đa số trong tập test (74,60%). Mô hình đạt độ bao phủ rất cao nhờ tập từ vựng khen ngợi phổ biến (*"great"*, *"love"*, *"soft"*, *"smells good"*, *"recommend"*).

### 6.2. Hiệu năng lớp Negative (`label = 2`)
- **Precision:** `61.15%` | **Recall:** `67.70%` | **F1-score:** `64.26%`.
- **Nhận xét:** Nhờ được huấn luyện trên 15.000 mẫu Negative cân bằng (thay vì bị áp đảo như trong tập tự nhiên), mô hình nhận diện tốt các từ khóa phàn nàn (*"waste of money"*, *"terrible"*, *"broken"*, *"fake"*, *"allergic"*), đạt Recall và F1 ở mức khả quan.

### 6.3. Thách thức lớn ở lớp Neutral (`label = 1`)
- **Precision:** `23.38%` | **Recall:** `58.02%` | **F1-score:** `33.33%`.
- **Phân tích nhầm lẫn (Confusion Analysis):**
  - Trong số `1,746` review 3 sao (neutral), có tới `307` mẫu bị đoán nhầm thành Positive và `426` mẫu bị đoán nhầm thành Negative.
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
- **Model Benchmark đã lưu tại HDFS:** `hdfs://localhost:9000/bigdata/models/baseline_lr_tfidf`.
- **Đề xuất cho Phase tiếp theo (Cải tiến mô hình & Xây dựng Dashboard):**
  1. *Cải tiến Feature Engineering:* Bổ sung `NGram` (unigram + bigram) để nắm bắt các cấu trúc phủ định.
  2. *Thử nghiệm mô hình:* Đánh giá thêm Naive Bayes và Random Forest / GBTClassifier của Spark MLlib.
  3. *Tối ưu hóa ngưỡng phân loại (Threshold Tuning):* Điều chỉnh ngưỡng xác suất cho lớp Neutral để cải thiện Macro F1.
  4. *Dashboard trực quan hóa:* Tích hợp mô hình đã lưu với ứng dụng Streamlit + Plotly để dự đoán thời gian thực cho người dùng cuối.
