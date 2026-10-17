# BÁO CÁO TÍCH HỢP MÔ HÌNH VÀ TRIỂN KHAI STREAMLIT DASHBOARD (PHASE 5 REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến (Amazon Reviews 2023)
**Hệ thống công nghệ:** Apache Hadoop HDFS 3.3.6, Apache Spark 3.5.5 (MLlib), Streamlit, Plotly Express  
**Thời điểm thực hiện:** 2026-10-17  
**Người thực hiện:** Antigravity (AI Big Data & Full-stack Engineer)  
**Mục tiêu hoàn thành:** Xây dựng ứng dụng Web Dashboard phân tích cảm xúc trực quan hóa dữ liệu lớn và tích hợp mô hình tốt nhất từ Phase 4.

---

## 1. TỔNG QUAN VÀ DANH MỤC TỆP TIN TRIỂN KHAI

Để đảm bảo nguyên tắc kiến trúc module hóa sạch sẽ, tách biệt ranh giới giữa giao diện (UI), quản lý tài nguyên (Resource Management), dịch vụ mô hình (Model Service) và truy vấn dữ liệu (Data Service), các tệp tin sau đã được thiết kế và bổ sung:

| Tệp tin | Vị trí | Mục đích & Trách nhiệm |
|---|---|---|
| **`app.py`** | Thư mục gốc | Điểm khởi nhập chính của Streamlit Web Dashboard, tổ chức 4 Tabs trực quan hóa |
| **`configs/dataset_config.yaml`** | `configs/` | Bổ sung phần cấu hình Phase 5 (`dashboard`, đường dẫn HDFS của Best Model, thông số Spark) |
| **`requirements.txt`** | Thư mục gốc | Bổ sung các thư viện phụ thuộc: `streamlit>=1.30.0`, `plotly>=5.18.0`, `pytest>=7.4.0` |
| **`src/dashboard/__init__.py`** | `src/dashboard/` | Định nghĩa package, ánh xạ nhãn cố định (`LABEL_MAPPING`, `SENTIMENT_TO_LABEL`) |
| **`src/dashboard/spark_manager.py`**| `src/dashboard/` | Singleton SparkSession, kiểm tra kết nối HDFS socket, quản lý và giải phóng tài nguyên RAM |
| **`src/dashboard/model_service.py`**| `src/dashboard/` | Dịch vụ tải `PipelineModel` (Phase 4), tiền xử lý, kiểm tra biên và suy luận xác suất |
| **`src/dashboard/data_service.py`** | `src/dashboard/` | Tổng hợp phân tán dữ liệu Parquet trên HDFS (không tải dữ liệu lớn lên driver) & nạp báo cáo offline |
| **`src/dashboard/visualization.py`**| `src/dashboard/` | Tạo các biểu đồ tương tác hiện đại bằng Plotly (Donut, Bar, Heatmap Confusion Matrix) |
| **`tests/test_label_mapping.py`**  | `tests/` | Kiểm thử tính đúng đắn của việc ánh xạ số sao (rating) sang nhãn cảm xúc |
| **`tests/test_model_loading.py`**   | `tests/` | Kiểm thử cấu hình HDFS, phát hiện ngắt kết nối và phân loại ngoại lệ |
| **`tests/test_prediction.py`**      | `tests/` | Kiểm thử các trường hợp biên đầu vào (rỗng, ký tự đặc biệt, text dài) và dữ liệu báo cáo |
| **`reports/phase5_dashboard_report.md`** | `reports/` | Báo cáo chi tiết nghiệm thu Phase 5 |

---

## 2. KIẾN TRÚC HỆ THỐNG VÀ CÁC MODULE CHỨC NĂNG

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        Streamlit UI (app.py)                          │
├───────────────────┬────────────────────┬─────────────────┬─────────────┤
│ 📊 Tab 1: Overview│ 🔮 Tab 2: Predict  │ 📈 Tab 3: Eval  │ 🔍 Tab 4:   │
│   - Big Data Flow │   - Text Area Input│   - Baseline vs │   Explorer  │
│   - HDFS Parquet  │   - Quick samples  │     Best Model  │   - Schema  │
│     Distributions │   - Confidence &   │   - 8 Experiments│  - Filter   │
│   - Donut & Bars  │     Probabilities  │   - Confusion   │  - Sampling │
│                   │   - Rating vs Pred │     Matrices    │    (Mask ID)│
└─────────┬─────────┴─────────┬──────────┴────────┬────────┴──────┬──────┘
          │                   │                   │               │
┌─────────▼─────────┐ ┌───────▼──────────┐ ┌──────▼────────┐ ┌────▼──────┐
│  visualization.py │ │ model_service.py │ │data_service.py│ │ spark_    │
│  - Plotly Charts  │ │ - PipelineModel  │ │- Distributed  │ │ manager.py│
│  - Color Palettes │ │ - Validations    │ │  Aggregations │ │- Singleton│
│  - Heatmaps       │ │ - Softmax Probs  │ │- Offline Rpts │ │- Lifecycle│
└───────────────────┘ └───────┬──────────┘ └──────┬────────┘ └────┬──────┘
                              │                   │               │
                              └─────────┬─────────┴───────────────┘
                                        │
                    ┌───────────────────▼───────────────────┐
                    │      Apache Spark 3.5.5 (local[*])    │
                    │      Driver Memory: 4 GB RAM          │
                    └───────────────────┬───────────────────┘
                                        │ (RPC Port 9000)
                    ┌───────────────────▼───────────────────┐
                    │         Hadoop HDFS 3.3.6             │
                    │ - /bigdata/models/phase4_best_model/  │
                    │ - /bigdata/reviews/processed/*.parquet│
                    └───────────────────────────────────────┘
```

---

## 3. THIẾT KẾ 4 TABS TRÊN GIAO DIỆN STREAMLIT

### 3.1. Tab 1 — Tổng quan (Overview)
- **Hiển thị thông tin dự án:** Đồ án Nhập môn Big Data — Amazon Reviews 2023 (All_Beauty).
- **Luồng dữ liệu lớn:** Thể hiện 4 chặng: Hugging Face (701.528 dòng, 311.5 MB) ➔ Hadoop HDFS 3.3.6 (NameNode port 9000, Parquet Snappy) ➔ Apache Spark 3.5.5 (PySpark MLlib) ➔ Mô hình ElasticNet tối ưu.
- **Thống kê phân phối:** Khi kết nối HDFS khả dụng, thực hiện tính toán phân tán (`groupBy`, `count`) trên file `all_beauty_100k_natural.parquet`. Nếu HDFS offline, hiển thị thông báo rõ ràng kèm số liệu đã kiểm định từ Phase 2.
- **Biểu đồ Plotly:** Donut Chart phân phối 3 lớp (Positive 73.74%, Neutral 9.11%, Negative 17.16%) và Bar Chart điểm đánh giá 1–5 sao.

### 3.2. Tab 2 — Dự đoán Cảm xúc Trực tiếp (Live Prediction)
- **Văn bản đầu vào:** Hỗ trợ nhập review qua `st.text_area`, tích hợp sẵn 3 nút nạp nhanh các review thực tế (5 ⭐ tích cực, 3 ⭐ trung tính, 1 ⭐ tiêu cực).
- **Đối chiếu Ground-truth:** Cho phép chọn số sao rating (1–5 sao) để đối chiếu cảm xúc suy luận từ số sao với nhãn do mô hình dự đoán.
- **Quy tắc tiền xử lý & kiểm tra biên:**
  - Phát hiện và cảnh báo nếu nội dung rỗng hoặc chỉ chứa khoảng trắng.
  - Tự động cắt ngắn an toàn nếu văn bản vượt quá 5.000 ký tự.
  - Bảo toàn emoji và ký tự Unicode để nạp vào `RegexTokenizer`.
- **Kết quả suy luận:**
  - Nhãn cảm xúc với badge màu sắc nổi bật (Xanh / Vàng / Đỏ).
  - Độ tin cậy (Confidence score).
  - Biểu đồ thanh ngang Plotly hiển thị chi tiết xác suất của cả 3 lớp: `[P(Positive), P(Neutral), P(Negative)]`.
  - Thẻ đối chiếu với số sao (Trùng khớp ✅ hoặc Khác biệt ⚠️).

### 3.3. Tab 3 — Đánh giá Mô hình & So sánh Thực nghiệm (Model Evaluation)
- **Đối đầu trực tiếp:** Thẻ KPI so sánh giữa Baseline Phase 3 và Best Model Phase 4:
  - **Accuracy:** `75.40%` (tăng `+1.85%` so với Baseline `73.55%`).
  - **Macro F1:** `63.02%` (tăng `+2.14%` so với Baseline `60.88%`).
  - **Weighted F1:** `78.33%` (tăng `+1.26%` so với Baseline `77.07%`).
  - **Neutral F1 (3 ⭐):** `35.91%` (tăng `+2.58%` so với Baseline `33.33%`).
- **Ma trận nhầm lẫn tương tác:** 2 Heatmap Plotly hiển thị ma trận của Baseline (Phase 3) và Best Model `M2c_LR_elastic` trên 20.000 mẫu Natural Test Set.
- **Bảng xếp hạng 8 thí nghiệm Phase 4:** Biểu đồ nhóm so sánh Accuracy, Macro F1, Neutral F1 của 8 cấu hình (Baseline, N-gram, ElasticNet, Class Weighting, Naive Bayes).
- **Phân tích giới hạn:** Giải thích chi tiết về Nghịch lý tỷ lệ nền (Base Rate Fallacy) và khó khăn của các mô hình tuyến tính đối với lớp Neutral.

### 3.4. Tab 4 — Khám phá Dữ liệu Lớn (Data Explorer)
- **Schema Viewer:** Xem kiểu dữ liệu, tính nullable và mô tả của từng cột trong bảng Parquet.
- **Bộ lọc tương tác:** Lọc theo nhãn cảm xúc (Positive / Neutral / Negative) và theo số sao (1–5 sao).
- **Trích xuất mẫu an toàn:** Chỉ trích xuất từ 5 đến 50 dòng thông qua `.limit()` của Spark, tuyệt đối không tải toàn bộ 100k hay 700k dòng vào bộ nhớ driver.
- **Bảo mật dữ liệu:** Tự động che giấu mã định danh người dùng (`user_id` hiển thị dạng `AGKH...7INQ`).

---

## 4. QUẢN LÝ TÀI NGUYÊN VÀ MÔ HÌNH HỌC MÁY

1. **Singleton SparkSession:** Module `spark_manager.py` duy trì duy nhất một phiên làm việc của Spark, tái sử dụng giữa các lần tương tác của người dùng, tránh hiện tượng rò rỉ bộ nhớ Java/JVM.
2. **Cơ chế Caching của Streamlit:** Sử dụng decorator `@st.cache_resource` cho SparkSession và `PipelineModel`, đảm bảo mô hình chỉ được nạp đúng **1 lần duy nhất** từ HDFS vào RAM khi ứng dụng khởi chạy.
3. **Phân loại lỗi minh bạch:**
   - Khi HDFS NameNode chưa khởi động: Hệ thống bắt lỗi qua socket check (timeout 1.5s), hiển thị thông báo hướng dẫn khởi động dịch vụ (`sudo systemctl start hadoop-hdfs`), không gây treo trình duyệt.
   - Khi thiếu tệp Parquet hoặc thư mục mô hình: Hiển thị hướng dẫn đường dẫn HDFS chuẩn.
4. **Giải phóng tài nguyên:** Bổ sung nút bấm trực tiếp trên Sidebar để người dùng có thể chủ động gọi `stop_spark_session()` và dọn dẹp cache.

---

## 5. HƯỚNG DẪN VẬN HÀNH VÀ KHỞI CHẠY ỨNG DỤNG

### Điều kiện tiên quyết
1. Dịch vụ Hadoop HDFS đang hoạt động trên WSL2:
   ```bash
   sudo systemctl status hadoop-hdfs
   # Nếu chưa chạy: sudo systemctl start hadoop-hdfs
   ```
2. Cài đặt các thư viện phụ thuộc:
   ```bash
   pip install -r requirements.txt
   ```

### Lệnh khởi chạy Streamlit Dashboard
```bash
streamlit run app.py
```
Ứng dụng sẽ tự động mở giao diện tại địa chỉ: `http://localhost:8501`.

Để chạy ở chế độ headless (phục vụ server hoặc kiểm thử tự động):
```bash
streamlit run app.py --server.headless true --server.port 8501
```

---

## 6. KẾT LUẬN VÀ TRẠNG THÁI NGHIỆM THU
Giai đoạn Phase 5 đã hoàn thành xây dựng toàn bộ mã nguồn giao diện, tích hợp dịch vụ mô hình và kiểm thử:
- Đã hoàn tất 4 Tabs theo đúng yêu cầu đồ án.
- Mô hình tối ưu `M2c_LR_elastic` và các báo cáo Phase 3-4 được tích hợp đồng bộ.
- Xử lý hoàn hảo các trường hợp ngoại lệ kết nối HDFS và kiểm tra biên dữ liệu.
