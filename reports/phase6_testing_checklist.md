# CHECKLIST VÀ KẾ HOẠCH KIỂM THỬ TOÀN DIỆN PHASE 6
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến (Amazon Reviews 2023)
**Hệ thống:** Apache Hadoop HDFS 3.3.6, Apache Spark 3.5.5, Streamlit, Plotly, PyTest  
**Thời điểm thiết lập:** 2026-10-17  
**Trạng thái tiến độ:** `0/5 mục hoàn thành`

---

## TỔNG QUAN TIẾN ĐỘ KIỂM THỬ PHASE 6

```text
[ ] 6.1. Kiểm thử end-to-end (HDFS → Spark → Model → Predict → Dashboard)
[ ] 6.2. Đánh giá chất lượng mô hình (Accuracy, Macro F1, Per-class, Confusion Matrix)
[ ] 6.3. Benchmark hiệu năng (Latency, Throughput, Hardware & Resource Specs)
[ ] 6.4. Kiểm thử độ ổn định (Edge Cases, Fault Tolerance, Restart & Lifecycle)
[ ] 6.5. Hoàn thiện báo cáo (Architecture, Pipeline, Results, Limitations, Roadmap)
```

---

## 1. MỤC 6.1: KIỂM THỬ END-TO-END (E2E PIPELINE INTEGRATION)
*Mục tiêu: Đảm bảo toàn bộ luồng dữ liệu từ HDFS, qua xử lý phân tán Spark, nạp mô hình M2c_LR_elastic, thực hiện suy luận và hiển thị trên Streamlit Dashboard.*

- [ ] **6.1.1. Luồng dữ liệu Tab 1 (Overview) từ HDFS:**
  - Kiểm tra kết nối HDFS RPC (cổng `9000`) qua socket check (`src/dashboard/spark_manager.py:check_hdfs_connectivity`).
  - Đọc trực tiếp tập Parquet `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet`.
  - Đối chiếu số dòng đếm phân tán (`count = 100.000`) và tỷ lệ 3 lớp cảm xúc hiển thị đúng trên Donut Chart: **Positive ~73.74%**, **Neutral ~9.11%**, **Negative ~17.16%**.
  - Kiểm tra cơ chế Fallback: Khi HDFS offline, Tab 1 tự động chuyển sang số liệu tĩnh đã kiểm định từ Phase 2 mà không gây gián đoạn ứng dụng.

- [ ] **6.1.2. Luồng suy luận thời gian thực Tab 2 (Live Prediction):**
  - Khởi tạo SparkSession singleton và nạp mô hình tốt nhất từ HDFS (`/bigdata/models/phase4_best_model/`).
  - Hàm `predict_single_review` nhận văn bản, tạo Spark DataFrame 1 dòng cột `full_text`, chuyển qua Pipeline:
    $$\text{RegexTokenizer} \longrightarrow \text{StopWordsRemover} \longrightarrow \text{HashingTF} \longrightarrow \text{IDF} \longrightarrow \text{LogisticRegression (ElasticNet)}$$
  - Trả về nhãn dự đoán (`Positive`, `Neutral`, `Negative`), phân phối xác suất 3 lớp (tổng xác suất = 1.0) và thẻ đối chiếu số sao rating (Ground-truth heuristic qua `rating_to_sentiment`).

- [ ] **6.1.3. Luồng hiển thị báo cáo Tab 3 (Model Evaluation):**
  - Hàm `load_offline_reports` đọc chính xác:
    - `reports/models/baseline_metrics.json`
    - `reports/models/phase4_metrics.csv`
    - `reports/models/phase4_confusion_matrices/cm_final_test_best_model.csv`
  - 4 thẻ KPI đối đầu hiển thị đúng mức cải thiện: Accuracy (+1.85%), Macro F1 (+2.14%), Weighted F1 (+1.26%), Neutral F1 (+2.58%).
  - Render tương tác 2 Heatmap Plotly và bảng so sánh 8 mô hình thực nghiệm.

- [ ] **6.1.4. Luồng truy vấn an toàn Tab 4 (Data Explorer):**
  - Hiển thị đầy đủ Schema 9 trường dữ liệu chuẩn hóa của bảng Parquet trên HDFS.
  - Kiểm tra các bộ lọc tương tác theo nhãn cảm xúc và số sao rating (1–5 sao).
  - Trích xuất mẫu phân tán thông qua `.limit(N)` (từ 5 đến 50 dòng), không kéo toàn bộ dataset về driver memory.
  - Tự động che giấu mã định danh người dùng: `user_id` hiển thị dưới dạng ẩn danh (ví dụ `AGKH...7INQ`).

---

## 2. MỤC 6.2: ĐÁNH GIÁ CHẤT LƯỢNG MÔ HÌNH (MODEL QUALITY VERIFICATION)
*Mục tiêu: Tổng hợp và xác thực các chỉ số định lượng trên tập kiểm thử độc lập 20.000 mẫu Natural Test.*

- [ ] **6.2.1. Đánh giá kiểm chứng trên tập độc lập (Strict Data Isolation):**
  - Xác minh tập kiểm thử `all_beauty_natural_test.parquet` (20.000 mẫu tự nhiên) hoàn toàn cách ly với 45.000 mẫu `all_beauty_balanced_train.parquet` (đạt 0% rò rỉ dữ liệu).

- [ ] **6.2.2. Bảng chỉ số tổng thể (Overall Metrics):**
  - **Accuracy:** Đạt **`75.40%`** (Baseline Phase 3: `73.55%`, tăng `+1.85%`).
  - **Macro F1-score:** Đạt **`63.02%`** (chỉ số cốt lõi, tăng **`+2.14%`** so với Baseline `60.88%`).
  - **Weighted F1-score:** Đạt **`78.33%`** (tăng **`+1.26%`** so với Baseline `77.07%`).

- [ ] **6.2.3. Bóc tách chi tiết Precision, Recall và F1-score từng lớp:**
  - **Lớp 0 — Positive (14.920 mẫu):**
    - Precision: **`95.86%`** | Recall: **`77.53%`** | F1-score: **`85.73%`** (Baseline: `85.06%`).
  - **Lớp 1 — Neutral (1.746 mẫu - Khó nhất):**
    - Precision: **`26.21%`** | Recall: **`56.99%`** | F1-score: **`35.91%`** (Baseline: `33.33%`, tăng **`+2.58%`**).
  - **Lớp 2 — Negative (3.334 mẫu):**
    - Precision: **`60.88%`** | Recall: **`75.52%`** | F1-score: **`67.42%`** (Baseline: `64.26%`, tăng **`+3.16%`**).

- [ ] **6.2.4. Xác thực ma trận nhầm lẫn cuối cùng (3x3 Confusion Matrix):**
  - Đối soát 20.000 mẫu thực tế so với nhãn dự đoán:
    $$\begin{pmatrix} 
    \text{Thực tế \ Dự đoán} & \text{Positive (0)} & \text{Neutral (1)} & \text{Negative (2)} \\
    \text{Positive (0)} & \mathbf{11.568} & 2.191 & 1.161 \\
    \text{Neutral (1)} & 294 & \mathbf{995} & 457 \\
    \text{Negative (2)} & 206 & 610 & \mathbf{2.518}
    \end{pmatrix}$$
  - Phân tích nguyên nhân nhầm lẫn: Nghịch lý tỷ lệ nền (Base Rate Fallacy) khiến 2.191 đánh giá Positive bị dự đoán thành Neutral do chứa từ ngữ trung tính mang tính mô tả.

---

## 3. MỤC 6.3: BENCHMARK HIỆU NĂNG (PERFORMANCE & RESOURCE BENCHMARKING)
*Mục tiêu: Định lượng thời gian xử lý qua từng chặng và ghi lại cấu hình tài nguyên thực tế.*

- [ ] **6.3.1. Ghi nhận thông số phần cứng và môi trường vận hành:**
  - **Hệ thống máy chủ:** Windows 11 64-bit, WSL2 Ubuntu 24.04.4 LTS.
  - **Tài nguyên vi xử lý & RAM:** 16 vCPUs, 15.2 GB RAM (Spark driver memory: 4 GB, master `local[*]`).
  - **Phiên bản nền tảng:** OpenJDK 11.0.32.1 (`JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`), Apache Hadoop 3.3.6, Apache Spark 3.5.5, Python 3.12.3 (WSL) / Python 3.13.0 (Host).

- [ ] **6.3.2. Đo thời gian đọc dữ liệu và I/O phân tán:**
  - Thời gian Spark khởi tạo phiên làm việc đầu tiên: $\le 10\text{s}$.
  - Thời gian đọc và đếm 100.000 dòng Parquet từ HDFS: $\le 3\text{s}$.
  - I/O đọc batch dữ liệu test 20.000 dòng: $\le 2\text{s}$.

- [ ] **6.3.3. Đo thời gian tiền xử lý và huấn luyện (Training Benchmark):**
  - `M0_Baseline`: `12.81s`
  - `M2c_LR_elastic`: **`8.57s`**
  - `M4_NaiveBayes`: `1.80s`
  - `M1_Ngram` (20.000 features): `19.56s`
  - Đảm bảo thời gian huấn luyện mô hình ElasticNet trên 45.000 dòng duy trì dưới 15 giây.

- [ ] **6.3.4. Đo độ trễ suy luận (Inference Latency):**
  - **Suy luận đơn lẻ trên Dashboard:** $\le 450\text{ms}$ (nhờ caching `@st.cache_resource`).
  - **Suy luận theo lô (Batch Inference):** Dự đoán trên toàn bộ 20.000 mẫu kiểm thử: $\le 8\text{s}$ trên `local[*]`.

---

## 4. MỤC 6.4: KIỂM THỬ ĐỘ ỔN ĐỊNH VÀ CHỊU LỖI (ROBUSTNESS & FAULT TOLERANCE)
*Mục tiêu: Đảm bảo hệ thống xử lý hoàn hảo dữ liệu biên, ngoại lệ hạ tầng và quản lý vòng đời bộ nhớ.*

- [ ] **6.4.1. Kiểm thử biên đầu vào người dùng (Edge Cases):**
  - Chạy toàn bộ bộ test tự động (`python -m pytest tests/ -v`) đạt 13/13 PASSED.
  - Chuỗi rỗng `""`, chỉ chứa khoảng trắng/ký tự xuống dòng `"   \n\t  "`, giá trị `None`: Báo lỗi thân thiện, không gửi request lỗi sang Spark.
  - Review siêu dài (> 5.000 ký tự): Tự động cắt tỉa an toàn tại 5.000 ký tự kèm thông báo `st.warning`.
  - Review chứa Emoji phức tạp và ký tự đặc biệt (`😍✨`, `$15.99`, `(10/10)`): Giữ nguyên định dạng Unicode, nạp thành công vào `RegexTokenizer`.
  - Văn bản chỉ có ký tự đặc biệt: Xử lý vector rỗng an toàn, không gây crash `IDFModel`.

- [ ] **6.4.2. Kiểm thử lỗi hạ tầng HDFS & Kết nối mạng:**
  - Khi HDFS NameNode chưa khởi động: Socket check timeout trong vòng 1.5 giây, UI không bị treo.
  - Hiển thị banner cảnh báo kèm câu lệnh khởi động `sudo systemctl start hadoop-hdfs`.
  - Tab 1 và Tab 3 tự động chuyển chế độ hiển thị báo cáo tĩnh mà không bị crash.

- [ ] **6.4.3. Kiểm thử lỗi nạp mô hình:**
  - Bắt đúng ngoại lệ `FileNotFoundError` khi đường dẫn mô hình sai hoặc chưa có trên HDFS, kèm hướng dẫn chạy `deploy_phase4_model_to_hdfs.py`.
  - Bắt đúng `PySparkUnavailableError` khi môi trường thiếu PySpark.

- [ ] **6.4.4. Kiểm thử giải phóng tài nguyên và khởi động lại:**
  - Nút *"Giải phóng Bộ nhớ & Dọn Cache"* trên Sidebar dừng tiến trình SparkContext sạch sẽ và dọn dẹp heap Java.
  - Khởi động lại ứng dụng `streamlit run app.py` không gặp lỗi kẹt port 8501 hay lock file.

---

## 5. MỤC 6.5: HOÀN THIỆN BÁO CÁO VÀ HỒ SƠ ĐỒ ÁN (DOCUMENTATION & HANDOVER)
*Mục tiêu: Tổng kết toàn diện kiến trúc, thực nghiệm, hạn chế và định hướng mở rộng.*

- [ ] **6.5.1. Tổng kết kiến trúc hệ thống dữ liệu lớn:**
  - Cập nhật sơ đồ luồng dữ liệu toàn trình: Hugging Face $\rightarrow$ HDFS 3.3.6 $\rightarrow$ Parquet $\rightarrow$ PySpark MLlib $\rightarrow$ Streamlit / Plotly.
  - Cập nhật danh mục mã nguồn và hướng dẫn thực thi trong `README.md`.

- [ ] **6.5.2. Tổng hợp quy trình xử lý và phương pháp luận:**
  - Phương pháp thu thập và làm sạch 701.528 review thô.
  - Chiến lược cân bằng lớp: Tập `all_beauty_balanced_train.parquet` (45.000 dòng, tỷ lệ 1:1:1) để triệt tiêu thiên kiến lớp áp đảo.
  - Quy chuẩn phân chia dữ liệu nghiêm ngặt (Train 80% / Val 20% / Test 20k tự nhiên độc lập).
  - Quá trình thực nghiệm so sánh 8 cấu hình mô hình.

- [ ] **6.5.3. Báo cáo so sánh kết quả thực nghiệm toàn diện:**
  - Bảng so sánh đối đầu Baseline Phase 3 vs Best Model Phase 4.
  - Phân tích chi tiết mức tăng trưởng Macro F1 (+2.14%) và Neutral F1 (+2.58%).
  - Đánh giá vai trò của ElasticNet trong việc triệt tiêu đặc trưng nhiễu.

- [ ] **6.5.4. Đánh giá hạn chế của giải pháp hiện tại:**
  - Giới hạn của Bag-of-Words / TF-IDF đối với ngữ cảnh câu phức và sắc thái trung tính (3 sao).
  - Tỷ lệ lớp Neutral tự nhiên thấp (9.1%) làm giảm Precision của lớp này.
  - Giới hạn tài nguyên single-node WSL2 so với cụm phân tán đa máy thực tế.

- [ ] **6.5.5. Đề xuất định hướng phát triển:**
  - Tích hợp mô hình ngôn ngữ ngữ cảnh (RoBERTa / DistilBERT qua Spark NLP hoặc ONNX).
  - Nâng cấp luồng dữ liệu thời gian thực với **Spark Structured Streaming** và **Apache Kafka**.
  - Đóng gói Docker Compose / Kubernetes phục vụ triển khai Cloud.
