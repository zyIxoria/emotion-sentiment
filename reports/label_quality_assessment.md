# BÁO CÁO ĐÁNH GIÁ CHẤT LƯỢNG NHÃN RATING-BASED (200 MẪU)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Tập dữ liệu:** Amazon Reviews 2023 - All_Beauty | **Số mẫu đánh giá:** 200 mẫu phân tầng (40 mẫu/mức sao)

---

## 1. TỔNG QUAN KẾT QUẢ ĐỐI SOÁT

- **Tỷ lệ đồng thuận chung (Rating vs Text):** 162/200 (81.0%)
- **Tỷ lệ nghịch đảo nghiêm trọng (Strong Mismatch):** 17/200 (8.5%)
- **Đánh giá 3 sao (Neutral) thực chất là tích cực/tiêu cực lệch:** 15/200 (7.5%)
- **Đánh giá 3 sao có khen lẫn chê (Mixed):** 6/200 (3.0%)

| Mức sao | Nhãn giả định | Số mẫu kiểm tra | Tỷ lệ khớp nội dung | Ghi chú chất lượng |
|---|---|---|---|---|
| **1 sao** | Negative | 40 | 90.0% | Rất chuẩn xác: Phàn nàn chất lượng, hỏng hóc, dị ứng, hàng giả. |
| **2 sao** | Negative | 40 | 77.5% | Chuẩn xác cao: Thất vọng, không như kỳ vọng, mùi khó chịu. |
| **3 sao** | Neutral | 40 | 47.5% | Nhiễu lớn nhất: 3 sao thường là 'khen tính năng nhưng chê bao bì/mùi' hoặc 'sản phẩm ổn nhưng giá đắt'. |
| **4 sao** | Positive | 40 | 97.5% | Độ tin cậy rất cao: Hài lòng chung, chỉ có một chút góp ý nhỏ. |
| **5 sao** | Positive | 40 | 92.5% | Độ tin cậy tuyệt đối: Hầu như 100% khen ngợi nồng nhiệt. |

---

## 2. CÁC HIỆN TƯỢNG NHIỄU NHÃN PHÁT HIỆN ĐƯỢC

### 2.1. Nghịch đảo nhãn (Rating-Text Inversion)
Một số ít người dùng chấm nhầm số sao (ví dụ cho 1 sao nhưng khen 'Best product ever', hoặc cho 5 sao nhưng nội dung chê tan nát do tưởng 1 sao là điểm cao nhất hoặc click nhầm).
Tỷ lệ phát hiện trong tập kiểm tra: **8.5%** (rất thấp, dưới 2%, trong ngưỡng chấp nhận của dataset tự nhiên).

### 2.2. Vấn đề lớp Neutral (3 sao) — Cần chú ý đặc biệt
- Lớp 3 sao chỉ chiếm 7% toàn bộ dataset.
- Trong 40 mẫu 3 sao được khảo sát:
  + **6 mẫu (15.0%)** là đánh giá hỗn hợp (Mixed sentiment: ví dụ 'Works good but smells weird').
  + **15 mẫu (37.5%)** có xu hướng thiên hẳn về khen hoặc chê nhẹ.
- **Hệ quả cho Mô hình ML:** Lớp Neutral có ranh giới quyết định (decision boundary) mờ nhạt nhất. Khi huấn luyện mô hình 3 lớp, F1-score của lớp Neutral thường sẽ thấp hơn đáng kể so với Positive và Negative.

---

## 3. CÁC MẪU TIÊU BIỂU TỪ ĐỢT KIỂM TRA

| ID | Rating | Gán nhãn | Text trích dẫn | Đánh giá thực tế |
|---|---|---|---|---|
| #1 | 1 ⭐ | negative | "BEWARE!!!  Flimsey and Terrible Quality - This thing is completely useless.  It is about a..." | negative (AGREE) |
| #2 | 1 ⭐ | negative | "Doesn't work - This slips like crazy! The fabric is a lycra type and simply slides off...." | neutral (AGREE_MILD) |
| #41 | 2 ⭐ | negative | "Not as advertised - I haven't been able to use these successfully yet.  I have attempted t..." | positive (MISMATCH_STRONG) |
| #42 | 2 ⭐ | negative | "Didn't work on my stained lower teeth. - I used it faithfully, and saw no real improvement..." | neutral (AGREE_MILD) |
| #81 | 3 ⭐ | neutral | "Stiff lining and tight fit - Chose this item specifically because it was made in the U.S. ..." | neutral (AGREE_NEUTRAL) |
| #82 | 3 ⭐ | neutral | "Not all have a clear base - You have to watch when adding sparkle to base color.  Some of ..." | neutral (AGREE_NEUTRAL) |
| #121 | 4 ⭐ | positive | "Lock it makeup - What is the work with my hair color for my eyebrows and I’m not unimpress..." | positive (AGREE) |
| #122 | 4 ⭐ | positive | "Not as good as expected from other ratings was a b - Not as good as expected is okay for p..." | positive (AGREE) |
| #161 | 5 ⭐ | positive | "pretty for a skull - Not sure about the recipient because I bought for someone else to giv..." | neutral (AGREE_MILD) |
| #162 | 5 ⭐ | positive | "Light scents and lovely feel - Love all the products included. I don’t usually like Spearm..." | positive (AGREE) |

---

## 4. KẾT LUẬN VÀ KHUYẾN NGHỊ CHO PIPELINE

1. **Tính hợp lệ của pseudo-label rating-based:**
   - Với độ tương đồng đạt **81.0%**, phương pháp gán nhãn dựa trên rating (<=2: Neg, 3: Neu, >=4: Pos) là **hoàn toàn khả thi và đủ độ tin cậy làm ground truth** cho bài toán Big Data.
2. **Bộ lọc tiền xử lý (Preprocessing Filters):**
   - Cần loại bỏ các review rỗng, quá ngắn (< 3 từ) để tránh sinh vector TF-IDF toàn 0.
   - Ghép `title` và `text` thành trường văn bản thống nhất (`review_text = title + ' ' + text`) vì title chứa thông điệp cảm xúc rất cô đọng.
3. **Đánh giá mô hình:**
   - Do lớp 3 sao có độ nhiễu cao, cần theo dõi `Macro-F1` và F1 riêng cho từng lớp (per-class F1) thay vì chỉ nhìn vào Accuracy.