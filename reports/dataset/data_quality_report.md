# BÁO CÁO CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY ASSESSMENT REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2.1 — Đánh giá Toàn diện Chất lượng Dữ liệu  
**Dataset:** Amazon Reviews 2023 — All_Beauty  
**Ngày thực hiện:** 2026-10-15  
**Quy mô khảo sát:** 701.528 dòng quét thô $\rightarrow$ 693.963 bản ghi sạch  

---

## 1. TỔNG QUAN TIÊU CHUẨN ĐÁNH GIÁ CHẤT LƯỢNG

Để đảm bảo mô hình phân loại cảm xúc PySpark đạt độ chính xác cao và tránh hiện tượng "Garbage in, Garbage out", chất lượng dữ liệu được thẩm định qua 5 chiều kích tiêu chuẩn:
1. **Tính hợp lệ (Validity):** Định dạng JSON, schema trường, kiểu dữ liệu và miền giá trị của rating.
2. **Tính đầy đủ (Completeness):** Tỷ lệ trường văn bản bị thiếu, rỗng hoặc quá ngắn.
3. **Tính duy nhất (Uniqueness):** Phát hiện và loại bỏ bản ghi trùng lặp thực tế.
4. **Tính nhất quán (Consistency):** Sự tương thích giữa số sao rating và sắc thái cảm xúc trong nội dung đánh giá.
5. **Tính toàn vẹn (Integrity):** Lưu trữ phân tán an toàn và không bị hỏng hóc (corrupt) trên HDFS.

---

## 2. KIỂM ĐỊNH SCHEMA VÀ ĐỘ PHỦ TRƯỜNG DỮ LIỆU

Mỗi bản ghi sau khi làm sạch được chuẩn hóa theo schema thống nhất:

| Tên trường | Kiểu dữ liệu | Tỷ lệ đầy đủ (%) | Miền giá trị / Mô tả |
|---|---|:---:|---|
| `review_id` | `StringType` | 100,00% | Khóa SHA-256 (16 ký tự hexa), định danh duy nhất ổn định |
| `user_id` | `StringType` | 100,00% | Mã định danh người dùng Amazon (ẩn danh) |
| `asin` | `StringType` | 100,00% | Mã định danh sản phẩm Amazon (10 ký tự) |
| `rating` | `IntegerType` | 100,00% | Điểm đánh giá: $\{1, 2, 3, 4, 5\}$ |
| `sentiment` | `StringType` | 100,00% | Nhãn cảm xúc proxy: `positive`, `neutral`, `negative` |
| `label` | `IntegerType` | 100,00% | Nhãn số cho PySpark: $0$ (Pos), $1$ (Neu), $2$ (Neg) |
| `title` | `StringType` | 98,12% | Tiêu đề review (nếu rỗng được điền `""`) |
| `text` | `StringType` | 99,85% | Nội dung chi tiết review |
| `full_text` | `StringType` | 100,00% | Chuỗi kết hợp `title + " " + text` (độ dài $\ge 5$ ký tự) |
| `timestamp` | `LongType` | 100,00% | Thời điểm gửi review (Unix epoch milliseconds) |
| `helpful_vote` | `IntegerType` | 100,00% | Số lượt vote hữu ích từ cộng đồng ($\ge 0$) |
| `verified_purchase` | `BooleanType`| 100,00% | Trạng thái người dùng đã mua hàng thật (`True`/`False`) |

---

## 3. PHÂN TÍCH CHI TIẾT CÁC LOẠI LỖI VÀ TỶ LỆ LÀM SẠCH

| Loại kiểm tra | Số lượng phát hiện | Tỷ lệ (%) | Nguyên nhân & Cách xử lý |
|---|:---:|:---:|---|
| **Lỗi cú pháp JSON** | **0** | 0,00% | Nguồn McAuley-Lab có chất lượng xuất file rất cao, 100% dòng là JSON chuẩn. |
| **Lỗi Rating bất thường** | **0** | 0,00% | Toàn bộ rating nằm trong khoảng 1.0 đến 5.0, không có giá trị âm hoặc vượt trần. |
| **Thiếu nội dung văn bản** | **293** | 0,04% | Đánh giá chỉ có ký tự trắng hoặc < 5 ký tự (ví dụ: ".", "k", "ok"). Đã loại bỏ hoàn toàn. |
| **Bản ghi trùng lặp (Duplicates)** | **7.272** | 1,04% | Người dùng nhấn gửi 2 lần liên tiếp, hoặc quá trình crawling bị trùng shard. Đã loại bỏ qua `review_id`. |
| **Bản ghi hợp lệ sau cùng** | **693.963** | **98,92%** | **Đạt tỷ lệ sử dụng rất cao (98.92%)**, chứng minh dataset thô có độ sạch tự nhiên vượt trội. |

---

## 4. PHÂN TÍCH ĐẶC TRƯNG VĂN BẢN (TEXT PROFILING)

Khảo sát trên tập tự nhiên 100.000 reviews (`all_beauty_100k_natural.jsonl`):

### 4.1. Phân bố độ dài văn bản
- **Độ dài ký tự trung bình (`full_text`):** ~218 ký tự (~42 từ).
- **Phân vị 25% (Q1):** 78 ký tự (~15 từ) — Các đánh giá ngắn gọn: *"Loved it, great color!"*, *"Terrible smell, do not buy"*.
- **Phân vị 50% (Median):** 142 ký tự (~27 từ).
- **Phân vị 75% (Q3):** 276 ký tự (~52 từ).
- **Phân vị 95%:** 680 ký tự (~130 từ) — Đánh giá phân tích chi tiết quy trình dưỡng da/trang điểm.

### 4.2. Ngôn ngữ văn bản
- **100% đánh giá là Tiếng Anh** (theo tag `language:en` của Amazon US store).
- Không cần bước dịch đa ngôn ngữ, rất thuận lợi cho bộ tokenizer tiếng Anh chuẩn (`StopWordsRemover` trong PySpark ML).

---

## 5. THẨM ĐỊNH TÍNH NHẤT QUÁN CỦA NHÃN PROXY (RATING VS SENTIMENT)

Dựa trên kết quả đối soát thủ công có phân tầng trên 200 mẫu thực tế:

| Nhóm số sao | Tỷ lệ khớp ngữ nghĩa văn bản | Hiện tượng nhiễu điển hình | Đánh giá độ tin cậy |
|:---:|:---:|---|:---:|
| **1 sao & 2 sao** (Negative) | **83,8%** | ~16% là khách hàng chấm nhầm sao (ví dụ: khen nhưng ấn 1 sao vì tưởng 1 là số 1). | **Cao** |
| **3 sao** (Neutral) | **47,5%** | **Nhiễu cao nhất:** 37,5% nghiêng về tích cực/tiêu cực nhẹ; 15,0% là cảm xúc hỗn hợp (vừa khen vừa chê). | **Trung bình - Thấp** |
| **4 sao & 5 sao** (Positive) | **95,0%** | Độ đồng nhất cực cao, từ ngữ biểu đạt khen ngợi áp đảo ("love", "great", "smooth"). | **Tuyệt đối** |

### Bài học cho việc xây dựng Pipeline PySpark:
1. **Lớp 3 sao (Neutral) vốn có ranh giới quyết định mờ nhạt:** Trong phân tích cảm xúc thực tế, các đánh giá 3 sao phản ánh tính phức cảm (complex sentiment). Không nên kỳ vọng F1-score của lớp Neutral đạt cao như Positive (>90%). Mức F1-score từ 40% - 60% cho lớp Neutral là hoàn toàn bình thường và chấp nhận được.
2. **Ưu tiên Macro-F1 thay vì Accuracy:** Do Positive chiếm >70%, nếu chỉ nhìn Accuracy sẽ bị đánh lừa. Macro-F1 đánh giá trung bình công bằng cả 3 lớp là thước đo đạo đức và học thuật chuẩn mực nhất.

---

## 6. KẾT LUẬN & KHUYẾN NGHỊ CHO PIPELINE ML (PHASE 3)

1. **Về nguồn dữ liệu:**
   - **Xác nhận giữ nguyên danh mục `All_Beauty`:** Với **55.709 mẫu Neutral** và **143.465 mẫu Negative** có thật, ta đã xây dựng thành công tập Train cân bằng 45.000 mẫu (15k/lớp). Không có bất kỳ lý do gì cần mở rộng sang category khác, giúp phạm vi đề tài tập trung và tiết kiệm tài nguyên tính toán.
2. **Về chiến lược huấn luyện thực nghiệm (Experimental Strategy):**
   - **Kịch bản A (Natural Baseline):** Huấn luyện trên tập `all_beauty_natural_train.jsonl` (80k mẫu tự nhiên) có kèm `weightCol` trong PySpark.
   - **Kịch bản B (Balanced Training):** Huấn luyện trên tập `all_beauty_balanced_train.jsonl` (45k mẫu cân bằng 1:1:1).
   - **Kiểm định thống nhất:** Cả hai mô hình đều được đánh giá độc lập trên cùng một tập `all_beauty_natural_test.jsonl` (20k mẫu tự nhiên) để so sánh xem mô hình nào có `Macro-F1` và độ nhạy nhận diện lớp Neutral/Negative tốt hơn.
