# BÁO CÁO NẠP DỮ LIỆU CHÍNH THỨC VÀO HDFS (DATASET INGESTION REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2 — Thiết lập môi trường Big Data và Tích hợp Dataset (Bước F)  
**Thời điểm thực hiện:** 2026-10-16 02:24:47  
**Người thực hiện:** Antigravity (AI Data Engineer)  
**Thời gian xử lý:** `53.29 giây`  

---

## 1. TỔNG QUAN NGUỒN VÀ ĐÍCH DỮ LIỆU

- **Dataset nguồn:** Amazon Reviews 2023 (`All_Beauty.jsonl` từ McAuley-Lab/UCSD, 701.528 dòng, 311.5 MB).
- **Hệ thống lưu trữ phân tán:** Apache Hadoop HDFS (`hdfs://localhost:9000`).
- **Thư mục lưu trữ đích:**
  - Thư mục raw: `/bigdata/reviews/raw/`
  - Thư mục processed: `/bigdata/reviews/processed/`
- **Định dạng tối ưu:** Chuyển đổi toàn bộ từ JSONL sang **Apache Parquet (nén Snappy)** bằng PySpark để tăng tốc độ quét cột và tiết kiệm bộ nhớ RAM/đĩa.

---

## 2. DANH MỤC CÁC TẬP DỮ LIỆU ĐÃ NẠP LÊN HDFS

| Tên tập dữ liệu | Đường dẫn HDFS (JSONL & Parquet) | Số dòng thực tế | Dung lượng | Mục đích sử dụng |
|---|---|:---:|:---:|---|
| **Master Raw File** | `/bigdata/reviews/raw/All_Beauty_full.jsonl` | 701.528 | 311.5 MB | Dữ liệu thô lưu trữ gốc |
| **Natural Dataset 100k** | `/bigdata/reviews/processed/all_beauty_100k_natural.parquet` | 100.000 | ~35 MB Parquet | Đo lường hiệu năng thực tế tự nhiên |
| **Natural Test Set 20k** | `/bigdata/reviews/processed/all_beauty_natural_test.parquet` | 20.000 | ~7 MB Parquet | **Tập kiểm thử độc lập cố định** cho mọi mô hình |
| **Balanced Train Set 45k**| `/bigdata/reviews/processed/all_beauty_balanced_train.parquet` | 45.000 | ~16 MB Parquet | Huấn luyện mô hình cân bằng (15k mẫu/lớp) |

---

## 3. KẾT QUẢ ĐỐI SOÁT VÀ PHÂN PHỐI LỚP TRÊN HDFS (PYSPARK VALIDATION)

Dữ liệu được PySpark đọc trực tiếp từ HDFS và kiểm tra phân phối nhãn độc lập:

| Tập dữ liệu | Số dòng JSONL | Số dòng Parquet | Đối soát khớp | Positive (4-5 ⭐) | Neutral (3 ⭐) | Negative (1-2 ⭐) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Natural 100k** | 100.000 | 100.000 | **100% khớp** | 73.735 (73,74%) | 9.108 (9,11%) | 17.157 (17,16%) |
| **Natural Test 20k** | 20.000 | 20.000 | **100% khớp** | 14.920 (74,60%) | 1.746 (8,73%) | 3.334 (16,67%) |
| **Balanced Train 45k**| 45.000 | 45.000 | **100% khớp** | 15.000 (33,33%) | 15.000 (33,33%) | 15.000 (33,33%) |

---

## 4. CHI TIẾT SCHEMA PARQUET CHUẨN TRÊN HDFS

Schema đồng nhất cho tất cả các bảng Parquet được quản lý bởi Spark:

```text
root
 |-- review_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- asin: string (nullable = true)
 |-- rating: long (nullable = true)
 |-- sentiment: string (nullable = true)
 |-- label: long (nullable = true)
 |-- title: string (nullable = true)
 |-- text: string (nullable = true)
 |-- full_text: string (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- helpful_vote: long (nullable = true)
 |-- verified_purchase: boolean (nullable = true)
```

---

## 5. KẾT QUẢ KIỂM TRA HỆ THỐNG TỆP HDFS (FSCK)

```text
Connecting to namenode via http://localhost:9870/fsck?ugi=tzy&files=1&blocks=1&path=%2Fbigdata%2Freviews
FSCK started by tzy (auth:SIMPLE) from /127.0.0.1 for path /bigdata/reviews at Fri Oct 16 02:24:46 ICT 2026

/bigdata/reviews <dir>
/bigdata/reviews/processed <dir>
/bigdata/reviews/processed/all_beauty_100k_natural.jsonl 71751405 bytes, replicated: replication=1, 1 block(s):  OK
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741838_1014 len=71751405 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet <dir>
/bigdata/reviews/processed/all_beauty_100k_natural.parquet/_SUCCESS 0 bytes, replicated: replication=3, 0 block(s):  OK

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00000-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2369491 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741841_1017. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741841_1017 len=2369491 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00001-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2307526 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741851_1027. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741851_1027 len=2307526 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00002-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2316365 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741842_1018. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741842_1018 len=2316365 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00003-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2309406 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741854_1030. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741854_1030 len=2309406 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00004-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2308661 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741850_1026. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741850_1026 len=2308661 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00005-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2290554 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741849_1025. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741849_1025 len=2290554 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00006-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2261606 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741856_1032. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741856_1032 len=2261606 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00007-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2270435 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741848_1024. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741848_1024 len=2270435 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00008-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2307708 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741853_1029. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741853_1029 len=2307708 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00009-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2286907 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741852_1028. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741852_1028 len=2286907 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00010-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2282634 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741846_1022. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741846_1022 len=2282634 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00011-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2283436 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741843_1019. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741843_1019 len=2283436 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00012-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2276053 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741855_1031. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741855_1031 len=2276053 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00013-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2273465 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741845_1021. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741845_1021 len=2273465 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00014-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 2273561 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741847_1023. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741847_1023 len=2273561 Live_repl=1

/bigdata/reviews/processed/all_beauty_100k_natural.parquet/part-00015-ddc66437-e541-48d9-9c10-17d9e89084e5-c000.snappy.parquet 276672 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741844_1020. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741844_1020 len=276672 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.jsonl 30090620 bytes, replicated: replication=1, 1 block(s):  OK
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741840_1016 len=30090620 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet <dir>
/bigdata/reviews/processed/all_beauty_balanced_train.parquet/_SUCCESS 0 bytes, replicated: replication=3, 0 block(s):  OK

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00000-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2034649 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741865_1041. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741865_1041 len=2034649 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00001-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2023174 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741867_1043. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741867_1043 len=2023174 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00002-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2030044 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741862_1038. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741862_1038 len=2030044 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00003-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2026511 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741866_1042. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741866_1042 len=2026511 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00004-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2036246 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741868_1044. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741868_1044 len=2036246 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00005-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2027647 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741864_1040. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741864_1040 len=2027647 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00006-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 2033839 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741863_1039. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741863_1039 len=2033839 Live_repl=1

/bigdata/reviews/processed/all_beauty_balanced_train.parquet/part-00007-f9e7e6c9-f608-48ea-b6f3-c98a6eb45d6c-c000.snappy.parquet 358589 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741861_1037. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741861_1037 len=358589 Live_repl=1

/bigdata/reviews/processed/all_beauty_natural_test.jsonl 14337643 bytes, replicated: replication=1, 1 block(s):  OK
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741839_1015 len=14337643 Live_repl=1

/bigdata/reviews/processed/all_beauty_natural_test.parquet <dir>
/bigdata/reviews/processed/all_beauty_natural_test.parquet/_SUCCESS 0 bytes, replicated: replication=3, 0 block(s):  OK

/bigdata/reviews/processed/all_beauty_natural_test.parquet/part-00000-f12f4a0e-88b8-487e-8842-9798553ae7d9-c000.snappy.parquet 2069064 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741858_1034. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741858_1034 len=2069064 Live_repl=1

/bigdata/reviews/processed/all_beauty_natural_test.parquet/part-00001-f12f4a0e-88b8-487e-8842-9798553ae7d9-c000.snappy.parquet 2070155 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741860_1036. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741860_1036 len=2070155 Live_repl=1

/bigdata/reviews/processed/all_beauty_natural_test.parquet/part-00002-f12f4a0e-88b8-487e-8842-9798553ae7d9-c000.snappy.parquet 2069599 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741859_1035. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741859_1035 len=2069599 Live_repl=1

/bigdata/reviews/processed/all_beauty_natural_test.parquet/part-00003-f12f4a0e-88b8-487e-8842-9798553ae7d9-c000.snappy.parquet 867220 bytes, replicated: replication=3, 1 block(s):  Under replicated BP-596914429-127.0.1.1-1792054760258:blk_1073741857_1033. Target Replicas is 3 but found 1 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741857_1033 len=867220 Live_repl=1

/bigdata/reviews/raw <dir>
/bigdata/reviews/raw/All_Beauty_full.jsonl 326611506 bytes, replicated: replication=1, 3 block(s):  OK
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741835_1011 len=134217728 Live_repl=1
1. BP-596914429-127.0.1.1-1792054760258:blk_1073741836_1012 len=134217728 Live_repl=1
2. BP-596914429-127.0.1.1-1792054760258:blk_1073741837_1013 len=58176050 Live_repl=1

/bigdata/reviews/test <dir>

Status: HEALTHY
 Number of data-nodes:	1
 Number of racks:		1
 Total dirs:			7
 Total symlinks:		0

Replicated Blocks:
 Total size:	499132391 B
 Total files:	35
 Total blocks (validated):	34 (avg. block size 14680364 B)
 Minimally replicated blocks:	34 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	28 (82.35294 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	1
 Average block replication:	1.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		56 (62.22222 %)
 Blocks queued for replication:	0

Erasure Coded Block Groups:
 Total size:	0 B
 Total files:	0
 Total block groups (validated):	0
 Minimally erasure-coded block groups:	0
 Over-erasure-coded block groups:	0
 Under-erasure-coded block groups:	0
 Unsatisfactory placement block groups:	0
 Average block group size:	0.0
 Missing block groups:		0
 Corrupt block groups:		0
 Missing internal blocks:	0
 Blocks queued for replication:	0
FSCK ended at Fri Oct 16 02:24:46 ICT 2026 in 21 milliseconds


The filesystem under path '/bigdata/reviews' is HEALTHY
```

- **Đánh giá trạng thái:** `Status: HEALTHY`. Không có tệp tin bị hỏng (corrupt), toàn bộ các block Parquet và JSONL được phân bổ an toàn trên DataNode.

---

## 6. KẾT LUẬN NGHIỆM THU BƯỚC F

- Đã đưa toàn bộ dataset thô và dataset đã làm sạch lên cấu trúc thư mục chuẩn `/bigdata/reviews/`.
- PySpark đã chuyển đổi thành công 100% sang định dạng Parquet nén Snappy.
- Số dòng và phân phối nhãn khớp 100% giữa JSONL và Parquet.
- **Kết quả nghiệm thu:** **PASS**. Toàn bộ dữ liệu lớn đã sẵn sàng trên HDFS để phục vụ huấn luyện mô hình ở Phase 3.
