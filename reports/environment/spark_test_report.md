# BÁO CÁO KIỂM THỬ SPARK ĐỘC LẬP (SPARK TEST REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2 — Thiết lập môi trường Big Data và Tích hợp Dataset  
**Thời điểm thực hiện:** 2026-10-16 02:20:32  
**Người thực hiện:** Antigravity (AI Data Engineer)  

---

## 1. THÔNG SỐ KHỞI TẠO SPARK

- **Phiên bản Apache Spark / PySpark:** `3.5.5`
- **Master:** `local[*]` (Tận dụng toàn bộ vCPUs của WSL2)
- **Bộ nhớ cấu hình Driver:** `2g`
- **Thời gian thực hiện smoke test:** `11.25 giây`

---

## 2. NỘI DUNG VÀ KẾT QUẢ CÁC BƯỚC KIỂM THỬ

| STT | Bước kiểm thử | Thao tác thực hiện | Kết quả thực tế | Trạng thái |
|:---:|---|---|---|:---:|
| 1 | **Khởi tạo SparkSession** | `SparkSession.builder.master("local[*]").getOrCreate()` | Khởi tạo thành công không lỗi | **PASS** |
| 2 | **Tạo DataFrame** | Tạo 5 bản ghi mẫu có schema (`review_id`, `asin`, `rating`, `title`, `text`) | Đếm được `5` dòng | **PASS** |
| 3 | **Biến đổi DataFrame** | Thêm cột `full_text`, `text_length`, gán nhãn `sentiment` proxy | Thực thi tính toán chính xác | **PASS** |
| 4 | **Ghi Parquet cục bộ** | `df.write.mode("overwrite").parquet("/tmp/...")` | Xuất file Parquet nén Snappy | **PASS** |
| 5 | **Đọc Parquet trở lại** | `spark.read.parquet("/tmp/...")` | Đọc lại được `5` dòng | **PASS** |
| 6 | **Đối soát tính nhất quán** | So sánh số dòng (`True`) và schema trường (`True`) | Dữ liệu toàn vẹn 100% | **PASS** |
| 7 | **Giải phóng tài nguyên** | `spark.stop()` và dọn dẹp thư mục tạm | Spark JVM đóng bình thường | **PASS** |

---

## 3. KẾT LUẬN NGHIỆM THU BƯỚC D

- PySpark 3.5.5 trên Python 3.12 và OpenJDK 11 hoạt động hoàn hảo, không có lỗi thư viện C/JNI BLAS, xử lý phân vùng và nén Parquet trơn tru.
- **Kết quả nghiệm thu:** **PASS**. Sẵn sàng cho Bước E (Kiểm thử tích hợp Spark và HDFS).
