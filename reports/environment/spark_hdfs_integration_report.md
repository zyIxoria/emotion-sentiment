# BÁO CÁO KIỂM THỬ TÍCH HỢP SPARK VÀ HDFS (SPARK-HDFS INTEGRATION REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2 — Thiết lập môi trường Big Data và Tích hợp Dataset  
**Thời điểm thực hiện:** 2026-10-16 02:23:08  
**Người thực hiện:** Antigravity (AI Data Engineer)  

---

## 1. THÔNG SỐ CẤU HÌNH KẾT NỐI

- **HDFS NameNode URI:** `hdfs://localhost:9000`
- **Apache Spark Version:** `3.5.5`
- **Hadoop Client Version trong Spark:** `Hadoop 3.3.6 compatible client`
- **Thư mục kiểm thử trên HDFS:** `/bigdata/reviews/test/`
- **Thời gian thực thi toàn trình:** `42.93 giây`

---

## 2. KẾT QUẢ CÁC BƯỚC KIỂM THỬ TÍCH HỢP

| STT | Bước kiểm thử | Thao tác thực tế | Kết quả ghi nhận | Trạng thái |
|:---:|---|---|---|:---:|
| 1 | **Khởi tạo kết nối HDFS** | `spark.hadoop.fs.defaultFS = hdfs://localhost:9000` | Kết nối RPC port 9000 thành công | **PASS** |
| 2 | **Chuẩn bị file nguồn trên HDFS** | Tạo JSONL và nạp vào `hdfs://localhost:9000/bigdata/reviews/test/integration_input.jsonl` | File xuất hiện trên HDFS | **PASS** |
| 3 | **PySpark đọc JSON từ HDFS** | `spark.read.json("hdfs://localhost:9000/...")` | Đọc thành công `5` dòng | **PASS** |
| 4 | **Thực thi tính toán phân tán** | Ghép text, tính độ dài, phân loại 3 lớp sentiment | DataFrame tính toán chính xác | **PASS** |
| 5 | **PySpark ghi Parquet lên HDFS** | `df.write.parquet("hdfs://localhost:9000/...")` | Sinh các file `part-*.snappy.parquet` trên HDFS | **PASS** |
| 6 | **PySpark đọc Parquet từ HDFS** | `spark.read.parquet("hdfs://localhost:9000/...")` | Đọc lại thành công `5` dòng | **PASS** |
| 7 | **Đối soát tính toàn vẹn** | So sánh số dòng (`True`) và schema (`True`) | Dữ liệu chính xác 100% | **PASS** |
| 8 | **Dọn dẹp môi trường test** | Xóa thư mục test trên HDFS qua client RPC | Hoàn tất dọn dẹp | **PASS** |

### Chi tiết các file Parquet được ghi trên HDFS:
```text
Found 2 items
-rw-r--r--   3 tzy supergroup          0 2026-10-16 02:23 hdfs://localhost:9000/bigdata/reviews/test/integration_output.parquet/_SUCCESS
-rw-r--r--   3 tzy supergroup      3.3 K 2026-10-16 02:23 hdfs://localhost:9000/bigdata/reviews/test/integration_output.parquet/part-00000-b4106158-f541-4eb9-a130-d5d2edb77ca4-c000.snappy.parquet
```

---

## 3. KIỂM CHỨNG TÍCH HỢP DATASET CHÍNH THỨC TRÊN HDFS (FULL-SCALE INTEGRATION)

Để đảm bảo pipeline không chỉ hoạt động với 5 dòng kiểm thử mà chịu tải và tương thích hoàn toàn với dữ liệu thực tế của đề tài, toàn bộ các tập dữ liệu chính thức đã được kiểm chứng đối soát trực tiếp bằng PySpark trên HDFS qua script `scripts/verify_official_dataset_integration.py`.

### 3.1. Danh mục các tập dữ liệu thực tế trên HDFS

| STT | Tập dữ liệu | Đường dẫn HDFS | Định dạng | Số dòng ghi nhận | Dung lượng | Trạng thái tích hợp |
|:---:|---|---|:---:|:---:|:---:|:---:|
| 1 | **Master Raw File** | `hdfs://localhost:9000/bigdata/reviews/raw/All_Beauty_full.jsonl` | JSONL | **701.528** | 311.5 MB | **PASS** |
| 2 | **Natural Dataset 100k** | `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_100k_natural.parquet` | Snappy Parquet | **100.000** | ~35 MB | **PASS** |
| 3 | **Natural Test Set 20k** | `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_natural_test.parquet` | Snappy Parquet | **20.000** | ~7 MB | **PASS** |
| 4 | **Balanced Train Set 45k** | `hdfs://localhost:9000/bigdata/reviews/processed/all_beauty_balanced_train.parquet` | Snappy Parquet | **45.000** | ~16 MB | **PASS** |

### 3.2. Đối soát Phân phối nhãn & Rating thực tế qua PySpark

Kết quả chạy tính toán phân tán trực tiếp từ các file Parquet trên HDFS:

#### A. Natural Dataset (100.000 mẫu)
- **Positive (4-5 ⭐):** `73.735` bản ghi (73,74%)
- **Negative (1-2 ⭐):** `17.157` bản ghi (17,16%)
- **Neutral (3 ⭐):** `9.108` bản ghi (9,11%)
- **Chi tiết rating 1-5 sao:**
  - 1 ⭐: 11.028 (11,03%)
  - 2 ⭐: 6.129 (6,13%)
  - 3 ⭐: 9.108 (9,11%)
  - 4 ⭐: 13.163 (13,16%)
  - 5 ⭐: 60.572 (60,57%)

#### B. Natural Test Set (20.000 mẫu độc lập)
- **Positive:** `14.920` bản ghi (74,60%)
- **Negative:** `3.334` bản ghi (16,67%)
- **Neutral:** `1.746` bản ghi (8,73%)

#### C. Balanced Train Set (45.000 mẫu huấn luyện)
- **Negative:** `15.000` bản ghi (33,33%)
- **Neutral:** `15.000` bản ghi (33,33%)
- **Positive:** `15.000` bản ghi (33,33%)

### 3.3. Schema Parquet 12 trường dữ liệu chuẩn được PySpark nạp từ HDFS
```text
root
 |-- asin: string (nullable = true)
 |-- full_text: string (nullable = true)
 |-- helpful_vote: long (nullable = true)
 |-- label: long (nullable = true)
 |-- rating: long (nullable = true)
 |-- review_id: string (nullable = true)
 |-- sentiment: string (nullable = true)
 |-- text: string (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- title: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- verified_purchase: boolean (nullable = true)
```

### 3.4. Trạng thái vật lý Block HDFS (FSCK)
```text
Connecting to namenode via http://localhost:9870/fsck?ugi=tzy&path=%2Fbigdata%2Freviews
Total size:    499,132,391 B
Total files:   35
Total blocks:  34 (100% minimally replicated)
Missing blocks: 0
Corrupt blocks: 0
Status: HEALTHY
The filesystem under path '/bigdata/reviews' is HEALTHY
```

---

## 4. XÁC MINH CẤU HÌNH MÔI TRƯỜNG TRONG SHELL THỰC TẾ

Các biến môi trường phục vụ kết nối PySpark và HDFS đã được cấu hình cố định trong `/etc/profile.d/bigdata.sh`, `/etc/environment` và `/home/tzy/.bashrc`.

Kết quả kiểm tra trực tiếp qua lệnh `bash -lc "env | grep -E '^(JAVA|HADOOP|SPARK)'"`:
```text
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
HADOOP_HOME=/opt/hadoop
SPARK_HOME=/opt/spark
```

Đường dẫn thực thi các công cụ:
- `which hadoop`  -> `/opt/hadoop/bin/hadoop`
- `which pyspark` -> `/home/tzy/.local/bin/pyspark`
- `which java`    -> `/usr/lib/jvm/java-11-openjdk-amd64/bin/java`

Tiến trình dịch vụ HDFS đang chạy (`jps`):
- `NameNode` (Active)
- `DataNode` (Active)
- `SecondaryNameNode` (Active)

---

## 5. KẾT LUẬN VÀ NGHIỆM THU

1. **Kiểm thử tích hợp PySpark - HDFS:** Đạt **PASS** trên cả 2 cấp độ:
   - *Cấp độ Smoke test:* Đọc/ghi Parquet 5 dòng mẫu, xác nhận tính năng round-trip thành công.
   - *Cấp độ Dữ liệu lớn:* Nạp và truy vấn thành công 701.528 dòng raw JSONL và 165.000 dòng Parquet (100k Natural + 20k Test + 45k Balanced Train) trên HDFS trong 11,52 giây.
2. **Tính toàn vẹn hệ thống tệp:** Toàn bộ 34 block dữ liệu trên HDFS đạt trạng thái `HEALTHY`, 0 block hỏng, 0 block mất mát.
3. **Cấu hình môi trường:** Biến môi trường `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME` đã được đồng bộ chuẩn xác và hoạt động tốt trên cả shell tương tác, non-interactive shell và tiến trình Python nền.
4. **Đủ điều kiện chuyển giai đoạn:** Môi trường Big Data và Dataset đã sẵn sàng 100% để chuyển sang **Phase 3 — Xây dựng pipeline tiền xử lý và huấn luyện mô hình baseline bằng PySpark MLlib**.

