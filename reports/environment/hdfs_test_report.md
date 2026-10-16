# BÁO CÁO KIỂM THỬ HADOOP HDFS (HDFS TEST REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2 — Thiết lập môi trường Big Data và Tích hợp Dataset  
**Thời điểm thực hiện:** 2026-10-16 02:19:32  
**Người thực hiện:** Antigravity (AI Data Engineer)  

---

## 1. THÔNG TIN TIẾN TRÌNH VÀ PHIÊN BẢN HADOOP

- **Phiên bản Hadoop:** `Hadoop 3.3.6`
- **Chế độ triển khai:** Pseudo-Distributed Single-Node (WSL2 Ubuntu 24.04).
- **HDFS Default FS URI:** `hdfs://localhost:9000`
- **Quản lý dịch vụ:** Systemd Service (`systemctl status hadoop-hdfs`).
- **Các tiến trình HDFS đang hoạt động (JPS):**
```text
949 SecondaryNameNode
2024 Jps
459 NameNode
670 DataNode
```

---

## 2. CẤU TRÚC THƯ MỤC DỰ ÁN TRÊN HDFS

Hệ thống đã khởi tạo cấu trúc thư mục phân tầng phục vụ đồ án:

| Đường dẫn HDFS | Mục đích | Quyền & Chủ sở hữu |
|---|---|:---:|
| `/bigdata/reviews/raw/` | Lưu trữ tệp dữ liệu thô gốc từ nguồn | `rwxr-xr-x` (`tzy:supergroup`) |
| `/bigdata/reviews/processed/` | Lưu trữ dữ liệu Parquet sau xử lý / phân tầng | `rwxr-xr-x` (`tzy:supergroup`) |
| `/bigdata/reviews/test/` | Thư mục tạm phục vụ smoke test và kiểm định tích hợp | `rwxr-xr-x` (`tzy:supergroup`) |

---

## 3. KẾT QUẢ THỰC HIỆN KIỂM THỬ (ROUND-TRIP TEST)

| Bước kiểm thử | Lệnh thực thi | Kết quả thực tế | Trạng thái |
|---|---|---|:---:|
| **1. Khởi tạo thư mục** | `hdfs dfs -mkdir -p /bigdata/reviews/test` | Exit code 0 | **PASS** |
| **2. Tạo file mẫu local** | Tạo `/tmp/hdfs_smoke_test.txt` (MD5: `e4f8a8ec31cecce4b9802d0c827a340c`) | Đã tạo thành công | **PASS** |
| **3. Upload file lên HDFS** | `hdfs dfs -put -f /tmp/hdfs_smoke_test.txt /bigdata/reviews/test/hdfs_smoke_test.txt` | Upload hoàn tất | **PASS** |
| **4. Kiểm tra danh sách file** | `hdfs dfs -ls -h /bigdata/reviews/test/hdfs_smoke_test.txt` | `-rw-r--r--   1 tzy supergroup        124 2026-10-16 02:19 /bigdata/reviews/test/hdfs_smoke_test.txt` | **PASS** |
| **5. Đọc lại nội dung** | `hdfs dfs -cat /bigdata/reviews/test/hdfs_smoke_test.txt` | MD5 khớp 100% (`e4f8a8ec31cecce4b9802d0c827a340c`) | **PASS** |
| **6. Kiểm tra toàn vẹn block** | `hdfs fsck /bigdata/reviews/test/hdfs_smoke_test.txt -files -blocks` | `Status: HEALTHY` | **PASS** |
| **7. Dọn dẹp dữ liệu test** | `hdfs dfs -rm /bigdata/reviews/test/hdfs_smoke_test.txt` | Exit code 0, không ảnh hưởng dữ liệu khác | **PASS** |

### Chi tiết log kiểm định `hdfs fsck`:
```text
Connecting to namenode via http://localhost:9870/fsck?ugi=tzy&files=1&blocks=1&path=%2Fbigdata%2Freviews%2Ftest%2Fhdfs_smoke_test.txt
FSCK started by tzy (auth:SIMPLE) from /127.0.0.1 for path /bigdata/reviews/test/hdfs_smoke_test.txt at Fri Oct 16 02:19:30 ICT 2026

/bigdata/reviews/test/hdfs_smoke_test.txt 124 bytes, replicated: replication=1, 1 block(s):  OK
0. BP-596914429-127.0.1.1-1792054760258:blk_1073741832_1008 len=124 Live_repl=1


Status: HEALTHY
 Number of data-nodes:	1
 Number of racks:		1
 Total dirs:			0
 Total symlinks:		0

Replicated Blocks:
 Total size:	124 B
 Total files:	1
 Total blocks (validated):	1 (avg. block size 124 B)
 Minimally replicated blocks:	1 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	1
 Average block replication:	1.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
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
FSCK ended at Fri Oct 16 02:19:30 ICT 2026 in 20 milliseconds


The filesystem under path '/bigdata/reviews/test/hdfs_smoke_test.txt' is HEALTHY
```

---

## 4. KẾT LUẬN NGHIỆM THU BƯỚC C

- Hệ thống Hadoop HDFS hoạt động chính xác 100% theo đúng cơ chế đọc, ghi, xác thực block và phân quyền.
- **Kết quả nghiệm thu:** **PASS**. Đủ điều kiện chuyển sang kiểm thử Spark độc lập và tích hợp Spark-HDFS.
