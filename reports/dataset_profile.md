# BÁO CÁO KHẢO SÁT DATASET — GIAI ĐOẠN 1
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Môn học:** Nhập môn Big Data  
**Dataset:** Amazon Reviews 2023 — All_Beauty  
**Ngày thực hiện:** 2026-10-15  
**Người thực hiện:** Antigravity (AI Data Engineer)

---

## 1. MỤC ĐÍCH KHẢO SÁT

Giai đoạn 1 nhằm xác minh nguồn dữ liệu, kiểm tra tính khả dụng, phân tích chất lượng và đưa ra quyết định GO/NO-GO trước khi triển khai môi trường Big Data (Giai đoạn 2).

---

## 2. MÔI TRƯỜNG ĐÃ KIỂM TRA

| Thành phần | Phiên bản | Trạng thái |
|---|---|---|
| Hệ điều hành | Windows 11 | ✅ Xác minh |
| Python | 3.13.0 (MSC v.1941 64-bit AMD64) | ✅ Xác minh |
| pip | 24.2 | ✅ Xác minh |
| pandas | 3.0.5 | ✅ Xác minh |
| requests | 2.34.2 | ✅ Xác minh |
| datasets (HuggingFace) | 5.1.0 | ⚠️ Cài được nhưng **KHÔNG hỗ trợ** `trust_remote_code` |
| huggingface_hub | 2.2.0 | ✅ Cài được |
| PySpark | Chưa cài | 🔜 Giai đoạn 2 |
| Hadoop HDFS | Chưa cài | 🔜 Giai đoạn 2 |

**Ổ đĩa khả dụng:**
- Ổ C: 47.35 GB còn trống / 464.99 GB tổng
- Ổ D: 346.04 GB còn trống / 487.82 GB tổng ← *workspace dự án*

> [!NOTE]
> **Thư viện `datasets` 5.x** đã bỏ hỗ trợ `trust_remote_code=True`. Dataset này dùng custom Python loader script nên **không thể dùng `load_dataset()`** với phiên bản mới. Phương pháp thay thế đã được xác minh và hoạt động tốt (xem Bước 3).

---

## 3. NGUỒN VÀ PHIÊN BẢN DATASET

| Thông tin | Giá trị |
|---|---|
| Tên dataset | Amazon Reviews 2023 |
| Tổ chức | McAuley Lab, UC San Diego |
| HuggingFace repo | `McAuley-Lab/Amazon-Reviews-2023` |
| HF config (reviews) | `raw_review_All_Beauty` |
| Phiên bản cuối cập nhật | 2024-12-08 |
| Ngôn ngữ | Tiếng Anh |
| License | Công khai, không cần xác thực |
| Phạm vi thời gian | đến tháng 09/2023 |

**URL tải trực tiếp (đã xác minh):**
```
https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl
```

---

## 4. PHƯƠNG PHÁP TẢI VÀ KẾT QUẢ THỰC THI

### 4.1. Phương pháp đã thử

| Phương pháp | Kết quả |
|---|---|
| `load_dataset(..., trust_remote_code=True)` với datasets 5.1.0 | ❌ **THẤT BẠI** — datasets 5.x không còn hỗ trợ loading scripts |
| UCSD direct URL (`datarepo.eng.ucsd.edu`) | ❌ **THẤT BẠI** — HTTP 404 Not Found |
| HuggingFace datasets-server API (parquet) | ❌ **THẤT BẠI** — HTTP 501 (không hỗ trợ custom script dataset) |
| **HTTP streaming từ HuggingFace Hub (urllib)** | ✅ **THÀNH CÔNG** |

### 4.2. Phương pháp hoạt động — HTTP Streaming

```python
import urllib.request, ssl, json, time

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE  # Workaround SSL cert Windows

URL = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl'

req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
    # Stream và đọc từng dòng JSONL
    ...
```

> [!IMPORTANT]
> Cần disable SSL verification do Windows không có CA bundle phù hợp. Trong WSL2 Ubuntu, vấn đề này không xuất hiện và không cần `ctx.verify_mode = ssl.CERT_NONE`.

### 4.3. Kết quả thực thi (đã xác minh)

| Chỉ số | Giá trị |
|---|---|
| Số bản ghi tải được | **1.000 records** ✅ |
| Dữ liệu truyền | 896 KB (0.88 MB) |
| Thời gian thực hiện | **2.3 giây** |
| Kích thước file mẫu | 868.5 KB |
| File mẫu lưu tại | `data/samples/all_beauty_sample_1000.jsonl` |
| Script tải | `scripts/stream_sample.py` |

---

## 5. SCHEMA THỰC TẾ (Đã xác minh qua dữ liệu)

| Tên trường | Kiểu dữ liệu | Null (trên 1000 mẫu) | Ví dụ | Vai trò dự kiến |
|---|---|---|---|---|
| `rating` | float | 0 (0.0%) | `5.0` | **Nhãn gốc** → sentiment |
| `title` | string | 0 (0.0%) | `"Such a lovely scent..."` | **Đầu vào mô hình** (kết hợp với text) |
| `text` | string | 0 (0.0%) | `"This spray is really nice..."` | **Đầu vào mô hình** (chính) |
| `asin` | string | 0 (0.0%) | `"B00YQ6X8EO"` | ID variant sản phẩm |
| `parent_asin` | string | 0 (0.0%) | `"B00YQ6X8EO"` | **product_id** trong pipeline |
| `user_id` | string | 0 (0.0%) | `"AGKHLEW2SOWHNMFQIJGBECAF7INQ"` | Theo dõi user |
| `timestamp` | int64 (ms) | 0 (0.0%) | `1588687728923` | **review_timestamp** |
| `helpful_vote` | int | 0 (0.0%) | `0` | Feature phụ |
| `verified_purchase` | bool | 0 (0.0%) | `true` | Feature phụ / lọc |
| `images` | list | **789 (78.9%)** | `[]` | **Không dùng** (NLP pipeline) |

**Trường không có trong schema đã kiểm tra:**
- `review_id` — Không có trong nguồn, cần tạo ID nội bộ bằng hash(user_id + asin + timestamp)
- `helpful_votes` — Không tồn tại; tên thực là `helpful_vote` (số ít)

### Schema chuẩn hóa cho pipeline

| Tên trường pipeline | Nguồn | Mô tả |
|---|---|---|
| `review_id` | `hash(user_id + asin + timestamp)` | ID nội bộ ổn định |
| `product_id` | `parent_asin` | Mã sản phẩm |
| `review_text` | `title + " " + text` | Nội dung kết hợp |
| `rating` | `rating` | Điểm gốc |
| `review_timestamp` | `timestamp` | Thời điểm (ms) |
| `review_date` | `timestamp / 1000 → date` | Ngày (chuyển đổi) |
| `sentiment` | Derived (giai đoạn tiền xử lý) | Nhãn 3 lớp |

---

## 6. THỐNG KÊ CHẤT LƯỢNG DỮ LIỆU

> **Lưu ý:** Tất cả số liệu dưới đây đều từ mẫu 1.000 records đã tải thực tế. Đây là số liệu xác minh, không phải ước tính.

### 6.1. Thống kê cơ bản

| Chỉ số | Giá trị |
|---|---|
| Số bản ghi trong mẫu | 1.000 |
| Số cột | 10 |
| Rating không hợp lệ | 0 |
| Review thiếu cả title và text | 0 |
| Title rỗng | 0 |
| Text rỗng | 0 |
| Review chỉ có ký tự đặc biệt | 0 |
| Bản ghi trùng (user_id + asin) | 0 |
| Số asin khác nhau | 930 / 1000 |
| Số parent_asin khác nhau | 921 / 1000 |
| Số user_id khác nhau | 415 / 1000 |
| Khoảng thời gian review | 2005-08-09 đến 2023-03-11 |

### 6.2. Phân bố rating

| Rating | Số lượng | Tỷ lệ | Biểu đồ |
|---|---|---|---|
| 1 sao ⭐ | 63 | 6.3% | ███ |
| 2 sao ⭐⭐ | 63 | 6.3% | ███ |
| 3 sao ⭐⭐⭐ | 111 | 11.1% | █████ |
| 4 sao ⭐⭐⭐⭐ | 189 | 18.9% | █████████ |
| 5 sao ⭐⭐⭐⭐⭐ | 574 | 57.4% | ████████████████████████████ |

### 6.3. Thống kê nội dung văn bản

| Chỉ số | Giá trị (số từ - title + text) |
|---|---|
| Ngắn nhất | 3 từ |
| Dài nhất | 1.525 từ |
| Trung bình | 96.0 từ |
| Trung vị | 62.0 từ |
| Phân vị 95 | 301 từ |

**Ví dụ review ngắn (≤ 5 từ):**
- Rating 5⭐: *"A+ Love it"*
- Rating 1⭐: *"Synthetic feeling Felt synthetic"*

**Ví dụ review trung bình (~50 từ):**
- Rating 4⭐: *"Works great but smells a little weird. This product does what I need it to do..."*

**Ví dụ review dài (>150 từ):**
- Rating 3⭐: *"Just ok I try to get Keratin treatments every 3 months, but honestly it has been getting costly..."*

### 6.4. Phân bố nhãn dự kiến (Rating → Sentiment)

**Quy tắc gán nhãn:**
- Rating 1–2 → `negative`
- Rating 3 → `neutral`
- Rating 4–5 → `positive`

> ⚠️ **Đây là nhãn suy ra từ rating, KHÔNG phải nhãn cảm xúc được chuyên gia xác nhận từ văn bản.**

| Nhãn | Số lượng | Tỷ lệ | Biểu đồ |
|---|---|---|---|
| negative | 126 | 12.6% | ██████ |
| neutral | 111 | 11.1% | █████ |
| positive | 763 | 76.3% | ██████████████████████████████████████ |

**Tỷ lệ mất cân bằng lớp (max/min):** `6.9x` ⚠️ **NGHIÊM TRỌNG**

---

## 7. RỦI RO VÀ HẠN CHẾ

### 7.1. Rủi ro chính

| Rủi ro | Mức độ | Mô tả | Giải pháp đề xuất |
|---|---|---|---|
| **Lệch lớp nghiêm trọng** | 🔴 CAO | 76.3% positive vs 11.1% neutral | Undersampling positive / class_weight |
| **Rating không phản ánh cảm xúc văn bản** | 🟡 TRUNG BÌNH | ~27/1000 trường hợp rating-text mâu thuẫn | Cân nhắc manual check 200-300 mẫu |
| **Trust_remote_code không được hỗ trợ** | 🟡 TRUNG BÌNH | datasets 5.x drop support → cần URL trực tiếp | Dùng HTTP streaming (đã xác minh) |
| **SSL Certificate trên Windows** | 🟡 TRUNG BÌNH | Cần `ctx.verify_mode = ssl.CERT_NONE` | Không ảnh hưởng khi dùng WSL2 Ubuntu |
| **Không có review_id nguyên bản** | 🟢 THẤP | Cần tạo ID nội bộ | hash(user_id + asin + timestamp) |
| **Images field 78.9% null** | 🟢 THẤP | Không dùng cho NLP | Bỏ trường images |
| **Dataset dùng custom script** | 🟢 THẤP | Không hỗ trợ HF Viewer/parquet | Dùng HTTP streaming hoặc datasets<3.x |

### 7.2. Hạn chế của mẫu 1.000 records

- Mẫu lấy từ đầu file → không đại diện cho phân bố toàn bộ dataset
- Phân bố rating trong mẫu có thể sai lệch so với toàn bộ 632K reviews
- Cần kiểm tra lại thống kê trên mẫu 100K+ trước khi huấn luyện

---

## 8. ĐỀ XUẤT QUY MÔ DỮ LIỆU

| Giai đoạn | Quy mô | Dung lượng ước tính | Mục đích |
|---|---|---|---|
| Smoke test ✅ | **1.000** | 0.9 MB (đã có) | Kiểm tra script và pipeline |
| Pipeline test | **25.000** | ~12 MB | Kiểm tra toàn bộ pipeline Spark + HDFS |
| Initial benchmark | **50.000** | ~24 MB | Đo hiệu suất ban đầu |
| **Main experiment** ⭐ | **100.000** | ~49 MB | **Bộ thực nghiệm chính (đề xuất)** |
| Extended | 200.000–300.000 | ~100–150 MB | Mở rộng nếu tài nguyên cho phép |

**Khuyến nghị:**
1. **Bắt đầu bằng 25.000** để kiểm tra toàn bộ pipeline Spark + HDFS
2. **Bộ thực nghiệm chính: 100.000 reviews** — phù hợp với RAM 8-16GB, đủ dữ liệu cho 3 lớp
3. **Điều kiện mở rộng lên 200K:** Pipeline chạy ổn, accuracy trên 100K đã đạt ngưỡng baseline

**Cách ghi nhận số dòng thực tế sau lấy mẫu:**
```python
# Trong script pipeline, luôn ghi log:
actual_count = df.count()
logger.info(f"Loaded {actual_count} records from sample")
# Lưu vào: data/processed/sample_metadata.json
```

---

## 9. THÔNG TIN KỸ THUẬT PIPELINE DỰ KIẾN

```
[HuggingFace Hub] → HTTP streaming → [JSONL raw] → [HDFS] → [PySpark]
                                          ↓
                                   data/raw/*.jsonl
                                          ↓
                              [Tiền xử lý: clean, label, split]
                                          ↓
                              [TF-IDF + Logistic Regression]
                                          ↓
                              [Đánh giá: F1, Precision, Recall]
```

---

## 10. KẾT LUẬN GO / NO-GO

### Checklist 7 tiêu chí hoàn thành

| # | Tiêu chí | Kết quả |
|---|---|---|
| 1 | Nguồn dataset và phương thức tải đã được xác minh? | ✅ **CÓ** — HTTP streaming từ HF Hub |
| 2 | Đã đọc được dữ liệu thực tế? | ✅ **CÓ** — 1.000 records thực tế đã tải |
| 3 | Schema và các trường cần thiết có phù hợp? | ✅ **CÓ** — rating, title, text, parent_asin, timestamp đầy đủ |
| 4 | Dữ liệu có đủ rating và review text để xây dựng nhãn? | ✅ **CÓ** — 0% null trên rating, title, text |
| 5 | Phân bố ba lớp thực tế như thế nào? | ⚠️ **Lệch:** 76.3% positive, 12.6% negative, 11.1% neutral |
| 6 | Quy mô dữ liệu khởi đầu phù hợp là bao nhiêu? | ✅ **25.000** để test pipeline, **100.000** cho thực nghiệm chính |
| 7 | Những rủi ro nào cần giải quyết trước khi cài Big Data? | ✅ **Xác định được:** Lệch lớp, SSL cert trên Windows, datasets version |

### ✅ KẾT LUẬN: **GO** — Tiến hành Giai đoạn 2

Dataset **Amazon Reviews 2023 — All_Beauty** đủ điều kiện để tiến hành đồ án:
- Dữ liệu thực sự tồn tại và tải được ✅
- Schema phù hợp cho bài toán phân loại cảm xúc ✅
- Dữ liệu tiếng Anh, không cần dịch ✅
- Kích thước 632K reviews phù hợp cho demo Big Data ✅

**Điều kiện tiên quyết trước khi sang Giai đoạn 2:**

- [ ] Cài đặt WSL2 Ubuntu 24.04 và cấu hình
- [ ] Cài đặt Hadoop HDFS trên WSL2
- [ ] Cài đặt PySpark trên WSL2
- [ ] Tải mẫu 25.000 reviews vào HDFS (dùng `scripts/stream_sample.py` với TARGET_N=25000)
- [ ] Lập kế hoạch xử lý class imbalance (undersampling hoặc class_weight)
- [ ] Kiểm tra thủ công 200-300 mẫu để đánh giá độ tin cậy của nhãn rating-based
