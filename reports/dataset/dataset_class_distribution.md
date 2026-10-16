# BÁO CÁO PHÂN PHỐI LỚP DỮ LIỆU (DATASET CLASS DISTRIBUTION)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2.1 — Bổ sung và đánh giá dữ liệu Neutral & Negative  
**Tập dữ liệu:** Amazon Reviews 2023 — All_Beauty  
**Ngày thực hiện:** 2026-10-15  
**Hệ thống lưu trữ:** Hadoop HDFS (WSL2 Ubuntu 24.04)

---

## 1. TỔNG QUAN VÀ NGUỒN DỮ LIỆU

- **Nguồn dữ liệu gốc:** Hugging Face `McAuley-Lab/Amazon-Reviews-2023`, danh mục `All_Beauty`.
- **Đường dẫn tệp gốc:** `raw/review_categories/All_Beauty.jsonl` (Dung lượng 311.5 MB).
- **Phương thức tải:** Tải với xác thực SSL đầy đủ (Strict SSL verification, không tắt chứng chỉ).
- **Quy tắc gán nhãn proxy cảm xúc (Rating-based Sentiment Proxy):**
  - **Negative:** Đánh giá 1 sao và 2 sao (`rating <= 2.0`) $\rightarrow$ Label numeric: `2`
  - **Neutral:** Đánh giá 3 sao (`rating == 3.0`) $\rightarrow$ Label numeric: `1`
  - **Positive:** Đánh giá 4 sao và 5 sao (`rating >= 4.0`) $\rightarrow$ Label numeric: `0`

> [!NOTE]
> **Hạn chế của nhãn proxy:** Nhãn cảm xúc được suy ra từ số sao người dùng chấm, không phải nhãn do chuyên gia ngôn ngữ gán trực tiếp. Đợt kiểm tra thủ công 200 mẫu trước đó ghi nhận tỷ lệ đồng thuận giữa số sao và ngữ nghĩa văn bản đạt 81.0%, trong đó lớp 3 sao có độ nhiễu cao nhất (chứa cả đánh giá hỗn hợp mixed-sentiment).

---

## 2. QUY TRÌNH LỌC VÀ LÀM SẠCH TRƯỚC - SAU

Dữ liệu được xử lý tuần tự qua pipeline kiểm tra chất lượng chặt chẽ:
- **Tạo khóa định danh duy nhất (Stable Deterministic Review ID):** Thay vì chỉ ghép `user_id + asin` (dễ bị trùng khi một khách hàng mua và đánh giá sản phẩm nhiều lần qua các năm), hệ thống sử dụng băm SHA-256 từ bộ 4 thuộc tính: `user_id + asin + timestamp + full_text`.
- **Lọc trùng lặp (Deduplication):** Loại bỏ triệt để các đánh giá bị gửi trùng lặp hoặc bản ghi sao chép.
- **Lọc rác văn bản (Text Content Filter):** Ghép `title + text`, loại bỏ các bản ghi không có nội dung hoặc độ dài dưới 5 ký tự.

### Bảng đối chiếu số lượng Trước và Sau làm sạch:

| Tiêu chí kiểm tra | Số lượng bản ghi | Tỷ lệ % trên tổng dòng | Ghi chú kỹ thuật |
|---|---|---|---|
| **Tổng số dòng quét từ file raw** | **701.528** | 100,00% | File `All_Beauty_full.jsonl` |
| Lỗi cú pháp JSON (JSON Decode Errors) | 0 | 0,00% | 100% dòng là JSON hợp lệ |
| Lỗi rating bất thường (ngoài [1..5]) | 0 | 0,00% | Toàn bộ rating chuẩn từ 1 đến 5 |
| Văn bản rỗng hoặc quá ngắn (< 5 ký tự) | 293 | 0,04% | Đã loại bỏ |
| Bản ghi trùng lặp (Duplicate IDs) | 7.272 | 1,04% | Đã loại bỏ |
| **Tổng bản ghi sạch, hợp lệ & duy nhất** | **693.963** | **98,92%** | Cơ sở dữ liệu sạch toàn diện |

---

## 3. PHÂN PHỐI SỐ SAO VÀ CẢM XÚC TRÊN TOÀN BỘ DATASET (693.963 REVIEWS)

### 3.1. Phân phối chi tiết theo từng mức sao (1 đến 5 sao)

| Mức sao | Nhãn cảm xúc | Số lượng bản ghi | Tỷ lệ phần trăm (%) | Biểu đồ trực quan |
|:---:|:---:|:---:|:---:|---|
| ⭐ (1 sao) | Negative | 100.873 | 14,54% | `██████▌` (14.5%) |
| ⭐⭐ (2 sao) | Negative | 42.592 | 6,14% | `███` (6.1%) |
| ⭐⭐⭐ (3 sao) | **Neutral** | **55.709** | **8,03%** | `████` (8.0%) |
| ⭐⭐⭐⭐ (4 sao) | Positive | 78.579 | 11,32% | `█████` (11.3%) |
| ⭐⭐⭐⭐⭐ (5 sao) | Positive | 416.210 | 59,98% | `████████████████████████████` (60.0%) |

### 3.2. Phân phối tổng hợp 3 lớp cảm xúc

| Lớp cảm xúc | Số lượng mẫu thực tế | Tỷ lệ % | Tỷ lệ mất cân bằng (Imbalance Ratio) |
|---|---|---|---|
| **Positive** (4-5 sao) | 494.789 | **71,30%** | **1,00x** (Lớp chiếm ưu thế) |
| **Negative** (1-2 sao) | 143.465 | **20,67%** | **~3,45x** |
| **Neutral** (3 sao) | **55.709** | **8,03%** | **~8,88x** (Lớp thiểu số sâu) |

> [!IMPORTANT]
> **Phát hiện quan trọng:** Tập dữ liệu `All_Beauty` **KHÔNG HỀ THIẾU DỮ LIỆU** lớp Neutral và Negative!
> Trong toàn bộ file có tới **55.709 review Neutral** và **143.465 review Negative** có thật 100%. Lý do các mẫu khảo sát nhỏ ban đầu (1.000 mẫu) có ít neutral/negative đơn thuần là do tỷ lệ tự nhiên chỉ chiếm ~8% và ~20%. Do đó, **hoàn toàn không cần mở rộng sang category khác** mà vẫn có đủ hàng chục nghìn mẫu neutral/negative thật để huấn luyện.

---

## 4. SO SÁNH PHÂN PHỐI CÁC TẬP DỮ LIỆU ĐƯỢC THIẾT KẾ

Để phục vụ nghiên cứu Big Data học thuật và giải quyết bài toán mất cân bằng lớp, hệ thống đã trích xuất 2 bộ dữ liệu độc lập:

1. **Tập tự nhiên (Natural Dataset - 100.000 reviews):** Giữ nguyên tỷ lệ phân phối gốc để đánh giá khách quan hiệu năng mô hình trong thực tế sản xuất.
   - Trong đó tách riêng **20.000 reviews làm Test Set cố định** (không can thiệp phân phối).
   - **80.000 reviews còn lại** làm Train Set tự nhiên.
2. **Tập huấn luyện cân bằng (Balanced Training Dataset - 45.000 reviews):** Lấy mẫu phân tầng 15.000 mẫu/lớp từ các review có thật trong bể ứng viên huấn luyện (ngoài tập test), tỷ lệ 1:1:1.

### Bảng so sánh phân phối giữa các tập dữ liệu:

| Lớp cảm xúc | Toàn bộ Dataset (694k) | Tập tự nhiên 100k | Tập Test tự nhiên 20k | Tập Train cân bằng 45k |
|---|:---:|:---:|:---:|:---:|
| **Positive** | 494.789 (71,30%) | 73.735 (73,74%) | 14.920 (74,60%) | **15.000 (33,33%)** |
| **Neutral** | 55.709 (8,03%) | 9.108 (9,11%) | 1.746 (8,73%) | **15.000 (33,33%)** |
| **Negative** | 143.465 (20,67%) | 17.157 (17,16%) | 3.334 (16,67%) | **15.000 (33,33%)** |
| **Tổng số** | **693.963 (100%)** | **100.000 (100%)** | **20.000 (100%)** | **45.000 (100%)** |

---

## 5. PHƯƠNG PHÁP LẤY MẪU VÀ ẢNH HƯỞNG ĐẾN TÍNH ĐẠI DIỆN CỦA DỮ LIỆU

### 5.1. Phương pháp lấy mẫu (Sampling Methodology)
- **Tách tập Kiểm thử trước (Test Isolation):** 20.000 mẫu của Test Set được trích xuất ngẫu nhiên từ Tập tự nhiên 100k và được niêm phong hoàn toàn.
- **Bể ứng viên huấn luyện (Training Candidate Pool):** Gom 80.000 mẫu tự nhiên còn lại cùng với 593.963 mẫu quét tiếp theo trong file (loại trừ Test Set).
  - Ứng viên Positive có sẵn: 479.869 mẫu.
  - Ứng viên Neutral có sẵn: 53.963 mẫu.
  - Ứng viên Negative có sẵn: 140.131 mẫu.
- **Lấy mẫu cân bằng ngẫu nhiên có phân tầng (Stratified Random Undersampling):**
  - Rút ngẫu nhiên chính xác 15.000 mẫu Neutral từ 53.963 ứng viên thực tế.
  - Rút ngẫu nhiên chính xác 15.000 mẫu Negative từ 140.131 ứng viên thực tế.
  - Rút ngẫu nhiên chính xác 15.000 mẫu Positive từ 479.869 ứng viên thực tế.
  - Xáo trộn (shuffle với `seed=42`) để tạo tập `all_beauty_balanced_train.jsonl`.

### 5.2. Đánh giá ảnh hưởng đến tính đại diện của dữ liệu (Representativeness Impact)
1. **Ưu điểm:**
   - **Triệt tiêu hiện tượng sụp đổ dự đoán (Prediction Collapse):** Khi huấn luyện trên tập 45k cân bằng, hàm loss không bị chi phối bởi lớp Positive, giúp gradient của lớp Neutral và Negative đủ mạnh để mô hình học các từ khóa cảm xúc đặc thù.
   - **Bảo toàn tính chân thực:** 100% là đánh giá người dùng thật trên Amazon, không dùng kỹ thuật sinh văn bản giả lập (không oversampling trùng lặp).
2. **Đánh đổi (Trade-offs) & Giải pháp:**
   - **Prior Probability Shift:** Tập huấn luyện cân bằng có $P(y)=1/3$, lệch so với phân phối thực tế trong thế giới thực ($P(Pos) \approx 71\%$).
   - **Quy chuẩn đánh giá:** Tuyệt đối **không đánh giá mô hình trên tập cân bằng**. Mọi mô hình huấn luyện (dù dùng tập train tự nhiên hay tập train cân bằng) đều phải được kiểm thử trên cùng một **Tập Test tự nhiên 20k** để đảm bảo tính khách quan và so sánh công bằng.
