# Amazon Reviews 2023 — All_Beauty Sentiment Analysis
## Đồ án Nhập môn Big Data

Dự án xây dựng pipeline phân tích cảm xúc từ đánh giá sản phẩm làm đẹp trên Amazon bằng hệ sinh thái Big Data: **Hadoop HDFS** (lưu trữ phân tán) và **Apache Spark / PySpark** (xử lý phân tán và học máy) trên nền tảng **WSL2 Ubuntu 24.04**.

---

## 1. Cấu trúc thư mục dự án

```text
emotion-sentiment/
├── README.md
├── requirements.txt                # Thư viện Python phụ thuộc
├── configs/
│   └── dataset_config.yaml         # Cấu hình dataset và tham số pipeline
├── data/
│   ├── samples/
│   │   └── all_beauty_sample_1000.jsonl  # Mẫu 1.000 reviews (Phase 1 smoke test)
│   ├── raw/
│   │   ├── All_Beauty_full.jsonl         # 701.528 reviews thô gốc (311.5 MB)
│   │   └── all_beauty_25k.jsonl          # 25.000 reviews mẫu
│   └── processed/
│       ├── all_beauty_100k_natural.jsonl # 100.000 reviews phân phối tự nhiên (68.4 MB)
│       ├── all_beauty_natural_test.jsonl # 20.000 reviews kiểm thử tự nhiên (13.7 MB)
│       ├── all_beauty_natural_train.jsonl# 80.000 reviews huấn luyện tự nhiên (54.7 MB)
│       └── all_beauty_balanced_train.jsonl# 45.000 reviews huấn luyện cân bằng 1:1:1 (28.7 MB)
├── reports/
│   ├── environment/
│   │   ├── environment_report.md         # Báo cáo hiện trạng phần cứng & phần mềm
│   │   ├── hdfs_test_report.md           # Báo cáo kiểm thử HDFS (Bước C)
│   │   ├── spark_test_report.md          # Báo cáo kiểm thử Spark độc lập (Bước D)
│   │   └── spark_hdfs_integration_report.md # Báo cáo tích hợp Spark - HDFS (Bước E)
│   └── dataset/
│       ├── dataset_class_distribution.md # Báo cáo phân phối lớp & sampling
│       ├── data_collection_report.md     # Quá trình thu thập & đồng bộ HDFS
│       ├── data_quality_report.md        # Thẩm định chất lượng dữ liệu chi tiết
│       ├── dataset_ingestion_report.md   # Nạp dữ liệu chính thức & chuyển Parquet (Bước F)
│       └── collection_metrics.json       # Metrics thống kê số liệu dạng JSON
└── scripts/
    ├── environment_check.py              # Script kiểm tra hiện trạng môi trường (Bước A)
    ├── hdfs_smoke_test.py                # Script smoke test HDFS (Bước C)
    ├── spark_smoke_test.py               # Script smoke test Spark độc lập (Bước D)
    ├── spark_hdfs_integration_test.py    # Script kiểm thử tích hợp Spark-HDFS (Bước E)
    ├── dataset_ingestion.py              # Nạp dataset lên HDFS & chuyển Parquet (Bước F)
    ├── collect_balanced_reviews.py       # Thu thập, làm sạch & cân bằng dữ liệu
    └── setup_bigdata_stack.sh            # Script tự động cài đặt stack môi trường
```

---

## 2. Thông số môi trường công nghệ (Phase 2 Verified)

| Thành phần | Phiên bản thực tế | Mô tả kiến trúc |
|---|---|---|
| **Hệ điều hành máy chủ** | Windows 11 64-bit | Host OS |
| **Môi trường Big Data** | WSL2 Ubuntu 24.04.4 LTS | Single-node / Pseudo-distributed |
| **Java JDK** | OpenJDK 11 (`11.0.32.1`) | `JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64` |
| **Hadoop HDFS** | Apache Hadoop 3.3.6 | Quản lý qua Systemd `hadoop-hdfs.service`, RPC port `9000` |
| **Apache Spark / PySpark** | PySpark 3.5.5 | Local master `local[*]`, driver memory 4G |
| **Python** | Python 3.12.3 | Môi trường hệ thống trong Ubuntu 24.04 |
| **Phần cứng** | 16 vCPUs, 15.2 GB RAM | Ổ D: còn trống >345 GB |

---

## 3. Cấu trúc lưu trữ dữ liệu trên Hadoop HDFS

Hệ thống lưu trữ trên HDFS (`hdfs://localhost:9000`) được tổ chức phân cấp chuẩn:

```text
hdfs://localhost:9000/
└── bigdata/
    └── reviews/
        ├── raw/
        │   └── All_Beauty_full.jsonl       (311.5 MB - Master Raw File, 701.528 dòng)
        ├── processed/
        │   ├── all_beauty_100k_natural.jsonl (68.4 MB - 100k mẫu tự nhiên)
        │   ├── all_beauty_100k_natural.parquet (Parquet nén Snappy - 100.000 dòng)
        │   ├── all_beauty_natural_test.jsonl (13.7 MB - 20k mẫu test tự nhiên)
        │   ├── all_beauty_natural_test.parquet (Parquet nén Snappy - 20.000 dòng)
        │   ├── all_beauty_balanced_train.jsonl (28.7 MB - 45k mẫu train cân bằng 1:1:1)
        │   └── all_beauty_balanced_train.parquet (Parquet nén Snappy - 45.000 dòng)
        └── test/
            └── (Thư mục phục vụ các kiểm thử tạm thời)
```

---

## 4. Hướng dẫn vận hành và chạy kiểm thử (Step-by-Step)

Mọi thao tác dưới đây được thực hiện trong **WSL2 Ubuntu 24.04**:

### 4.1. Quản lý dịch vụ Hadoop HDFS
Dịch vụ Hadoop HDFS đã được tích hợp vào `systemd`, tự động duy trì 24/7:
```bash
# Kiểm tra trạng thái dịch vụ
sudo systemctl status hadoop-hdfs

# Khởi động lại dịch vụ nếu cần
sudo systemctl restart hadoop-hdfs

# Kiểm tra các tiến trình Java HDFS đang chạy
jps
# Kết quả mong đợi: NameNode, DataNode, SecondaryNameNode
```

### 4.2. Chạy Bước A — Kiểm tra hiện trạng môi trường
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/environment_check.py
```
*Báo cáo xuất tại:* `reports/environment/environment_report.md`

### 4.3. Chạy Bước C — Kiểm thử HDFS Smoke Test
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/hdfs_smoke_test.py
```
*Báo cáo xuất tại:* `reports/environment/hdfs_test_report.md`

### 4.4. Chạy Bước D — Kiểm thử Spark độc lập
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/spark_smoke_test.py
```
*Báo cáo xuất tại:* `reports/environment/spark_test_report.md`

### 4.5. Chạy Bước E — Kiểm thử tích hợp Spark và HDFS
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/spark_hdfs_integration_test.py
```
*Báo cáo xuất tại:* `reports/environment/spark_hdfs_integration_report.md`

### 4.6. Chạy Bước F — Nạp Dataset chính thức & chuyển Parquet trên HDFS
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/dataset_ingestion.py
```
*Báo cáo xuất tại:* `reports/dataset/dataset_ingestion_report.md`

---

## 5. Các lỗi thường gặp và cách khắc phục (Troubleshooting)

### 1. Lỗi NameNode đang ở chế độ Safe Mode (`Name node is in safe mode`)
- **Triệu chứng:** Khi vừa khởi động Hadoop, lệnh `hdfs dfs -put` hoặc Spark ghi dữ liệu báo lỗi `Cannot create file... Name node is in safe mode`.
- **Nguyên nhân:** NameNode chờ các DataNode gửi báo cáo block trong 30 giây đầu tiên.
- **Cách khắc phục:**
  ```bash
  hdfs dfsadmin -safemode leave
  ```

### 2. Lỗi kết nối SSH Localhost (`Connection refused port 22`)
- **Triệu chứng:** `start-dfs.sh` báo lỗi `localhost: Connection refused`.
- **Cách khắc phục:**
  ```bash
  sudo service ssh restart
  ssh-keyscan -H localhost >> ~/.ssh/known_hosts
  ```

### 3. Cảnh báo Native Hadoop Library (`Unable to load native-hadoop library`)
- **Triệu chứng:** Log hiển thị `WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes`.
- **Bản chất:** Đây là cảnh báo thông thường của Hadoop khi chạy x86_64 mà không biên dịch C library riêng; Hadoop tự động chuyển sang pure Java implementation hoàn toàn ổn định, không ảnh hưởng đến tính đúng đắn.

---

## 6. Kết quả nghiệm thu Phase 2 (Acceptance Checklist)

| # | Hạng mục kiểm tra | Trạng thái | Bằng chứng thực tế |
|---|---|:---:|---|
| 1 | Môi trường Ubuntu 24.04 / WSL2 | **PASS** | `Ubuntu 24.04.4 LTS`, 16 vCPUs, 15.2 GB RAM |
| 2 | Phiên bản Java và Hadoop tương thích | **PASS** | `OpenJDK 11.0.32.1`, `Hadoop 3.3.6` |
| 3 | NameNode / DataNode khởi động ổn định | **PASS** | Systemd `hadoop-hdfs.service`, tiến trình `653 NameNode`, `834 DataNode` |
| 4 | HDFS upload và đọc file thành công | **PASS** | `hdfs_smoke_test.py` hoàn tất, checksum MD5 khớp 100% |
| 5 | PySpark chạy DataFrame smoke test | **PASS** | `spark_smoke_test.py` tạo, đếm, biến đổi và dừng sạch sẽ trong 11.25s |
| 6 | Parquet round-trip thành công | **PASS** | Ghi và đọc lại Parquet khớp 100% số dòng và schema |
| 7 | PySpark đọc dữ liệu trực tiếp từ HDFS | **PASS** | `spark.read.json("hdfs://localhost:9000/...")` đọc 5 dòng test và 100.000 dòng natural |
| 8 | PySpark ghi Parquet lên HDFS & đọc lại | **PASS** | Xuất thành công Parquet lên HDFS, đọc lại đối soát khớp 100% |
| 9 | Dataset Phase 1.1 được kiểm tra & chọn đúng | **PASS** | Master raw 701k, 100k Natural, 20k Test tự nhiên, 45k Balanced Train |
| 10 | Phân phối 3 lớp được thống kê từ thực tế | **PASS** | Positive (73,7%), Neutral (9,1%), Negative (17,2%) |
| 11 | Số dòng sau ingestion đối chiếu khớp nguồn | **PASS** | 100.000 / 100.000 (Natural), 20.000 / 20.000 (Test), 45.000 / 45.000 (Train) |
| 12 | Toàn bộ 5 báo cáo và README cập nhật | **PASS** | Đầy đủ tại thư mục `reports/environment/` và `reports/dataset/` |

---

## 7. Phase 3 — Huấn luyện và Đánh giá Mô hình Baseline (PySpark MLlib)

### 7.1. Chạy Pre-flight Audit & Kiểm tra Data Leakage
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/audit_phase2_datasets.py
```
*Báo cáo xuất tại:* `reports/phase2_preflight_audit.md` (Xác nhận 0% rò rỉ dữ liệu giữa Train và Test).

### 7.2. Chạy Huấn luyện Pipeline Baseline (TF-IDF + Logistic Regression)
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/train_baseline_lr_tfidf.py
```

### 7.3. Kết quả Baseline Benchmark trên Natural Test Set (20.000 mẫu độc lập)

| Chỉ số đánh giá | Giá trị thực nghiệm | Ghi chú & Ý nghĩa |
|---|:---:|---|
| **Accuracy** | **`73.55%`** (`0.7355`) | Độ chính xác tổng quan trên 20.000 mẫu |
| **Macro F1-score** | **`60.88%`** (`0.6088`) | **Thước đo cốt lõi:** Trung bình cộng không trọng số của 3 lớp |
| **Weighted F1-score** | **`77.07%`** (`0.7707`) | F1 tính theo phân phối tự nhiên (ưu tiên Positive) |
| **Positive F1 (Lớp 0)** | **`85.06%`** (`0.8506`) | Precision: 95.51% \| Recall: 76.67% (Support: 14.920) |
| **Neutral F1 (Lớp 1)** | **`33.33%`** (`0.3333`) | Precision: 23.38% \| Recall: 58.02% (Support: 1.746) |
| **Negative F1 (Lớp 2)** | **`64.26%`** (`0.6426`) | Precision: 61.15% \| Recall: 67.70% (Support: 3.334) |

### 7.4. Vị trí lưu trữ Model và Báo cáo Phase 3
- **Model Pipeline trên HDFS:** `hdfs://localhost:9000/bigdata/models/baseline_lr_tfidf/`
- **Báo cáo chi tiết Markdown:** `reports/models/baseline_model_report.md`
- **Tệp máy đọc Metric JSON:** `reports/models/baseline_metrics.json`
- **Ma trận nhầm lẫn CSV:** `reports/models/baseline_confusion_matrix.csv`
- **Classification Report CSV:** `reports/models/baseline_classification_report.csv`

---

## 8. Kết luận và Kế hoạch Phase 4
Phase 3 đã hoàn tất **PASS** 100%. Hệ thống đã lưu lại baseline benchmark đáng tin cậy trên HDFS.
Kế hoạch tiếp theo:
1. Thử nghiệm các mô hình cải tiến (n-gram, class weights, Naive Bayes, GBT).
2. Xây dựng ứng dụng Web Dashboard trực quan hóa (Streamlit + Plotly).
