# BÁO CÁO QUÁ TRÌNH THU THẬP VÀ ĐỒNG BỘ DỮ LIỆU (DATA COLLECTION REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2.1 — Khai thác, Làm sạch và Đồng bộ HDFS  
**Dataset:** Amazon Reviews 2023 — All_Beauty  
**Ngày thực hiện:** 2026-10-15  
**Hạ tầng:** WSL2 Ubuntu 24.04, Python 3.12, Hadoop 3.3.6 HDFS  

---

## 1. MỤC TIÊU VÀ BỐI CẢNH

Trong Phase 1 và Phase 2 ban đầu, dự án đã thử nghiệm trên mẫu 1.000 và 25.000 reviews, ghi nhận hiện tượng mất cân bằng lớp sâu: lớp Neutral (3 sao) chỉ chiếm ~7-9%, trong khi Positive chiếm >70%.

Mục tiêu của Phase 2.1:
1. **Xác minh nguồn dữ liệu đầy đủ trên Hugging Face:** Khảo sát toàn diện tệp `All_Beauty.jsonl` xem có đủ mẫu neutral/negative tự nhiên hay cần mở rộng sang category khác.
2. **Khai thác dữ liệu an toàn & chuẩn hóa:**
   - Sử dụng kết nối mạng có xác minh chứng chỉ SSL đầy đủ (Strict SSL verification, không dùng `ssl.CERT_NONE`).
   - Xử lý streaming theo từng dòng, không nạp toàn bộ dữ liệu vào RAM.
   - Xây dựng cơ chế sinh mã định danh duy nhất ổn định (Deterministic Stable Review ID).
   - Lọc bỏ bản ghi lỗi, thiếu text và trùng lặp.
3. **Thu thập 2 tập dữ liệu phục vụ nghiên cứu Big Data:**
   - **Tập tự nhiên (Natural Dataset):** 100.000 reviews phản ánh đúng tỷ lệ thế giới thực (chia ra 80.000 Train tự nhiên và 20.000 Test tự nhiên).
   - **Tập cân bằng (Balanced Training Dataset):** 45.000 reviews (15.000 Positive, 15.000 Neutral, 15.000 Negative) lấy 100% từ các review có thật, tuyệt đối không tạo dữ liệu giả lập và không đụng vào tập Test.
4. **Đồng bộ hóa lên cụm lưu trữ phân tán Hadoop HDFS** và kiểm định trạng thái toàn vẹn của cluster.

---

## 2. NGUỒN DỮ LIỆU VÀ PHƯƠNG THỨC TRUY XUẤT

- **Tổ chức chủ quản:** McAuley Lab, University of California, San Diego (UCSD).
- **Hugging Face Repository:** `https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023`
- **Tệp dữ liệu:** `raw/review_categories/All_Beauty.jsonl`
- **Kích thước tệp nguồn:** **326.611.506 bytes (~311.5 MB)**
- **Số dòng văn bản thô:** **701.528 dòng**
- **Phương thức tải:** Tải trực tiếp qua HTTP/HTTPS với TLS 1.3 và xác thực SSL CA đầy đủ trên Ubuntu 24.04 bằng `aria2c --check-certificate=true` (Tốc độ tải đạt ~10 MB/s, hoàn tất trong 31 giây).

---

## 3. THIẾT KẾ KỸ THUẬT VÀ QUY TRÌNH THỰC THI

### 3.1. Thuật toán tạo ID ổn định (Deterministic Review ID)
Trong schema của Amazon Reviews 2023, các trường khóa gồm: `user_id`, `asin`, `timestamp`. Tuy nhiên, thực tế một khách hàng có thể mua lại sản phẩm và viết nhiều đánh giá ở các thời điểm khác nhau, hoặc trường timestamp bị trễ. Để đảm bảo ID ổn định và phát hiện trùng lặp chính xác tuyệt đối:

$$\text{Review\_ID} = \text{SHA256}(\text{user\_id} \parallel \text{"\_"} \parallel \text{asin} \parallel \text{"\_"} \parallel \text{timestamp} \parallel \text{"\_"} \parallel \text{full\_text})[0:16]$$

Khóa băm 16 ký tự hexa vừa tối ưu dung lượng lưu trữ trên HDFS, vừa có không gian mẫu $16^{16} \approx 1.84 \times 10^{19}$, xác suất đụng độ băm gần như bằng 0.

### 3.2. Tiêu chí lọc và làm sạch
- **Cú pháp JSON:** Mỗi dòng phải phân tích cú pháp thành công bằng `json.loads()`.
- **Số sao hợp lệ:** Giá trị trường `rating` phải là số thực thuộc tập $\{1.0, 2.0, 3.0, 4.0, 5.0\}$.
- **Độ dài văn bản:** Ghép `title` và `text` thành `full_text`. Nếu độ dài $< 5$ ký tự (ví dụ đánh giá rỗng hoặc chỉ có dấu cách) thì loại bỏ để tránh tạo vector TF-IDF toàn 0.
- **Trùng lặp:** Dựa trên tập hợp `seen_ids`, nếu `review_id` đã xuất hiện thì loại bỏ bản ghi trùng tiếp theo.

---

## 4. KẾT QUẢ THU THẬP VÀ PHÂN CHIA DỮ LIỆU

### 4.1. Thống kê quá trình làm sạch
- **Tổng số dòng quét:** **701.528 dòng** (Thời gian xử lý: 41,59 giây trên Python 3.12).
- **Lỗi cú pháp JSON:** 0 lỗi.
- **Lỗi rating bất thường:** 0 lỗi.
- **Đánh giá rỗng / quá ngắn:** 293 bản ghi (0,04%).
- **Bản ghi trùng lặp:** 7.272 bản ghi (1,04%).
- **Tổng số bản ghi sạch, hợp lệ & duy nhất:** **693.963 bản ghi (98,92%)**.

### 4.2. Cơ cấu các tập dữ liệu đầu ra

| Tên tập dữ liệu | Đường dẫn tệp cục bộ | Số lượng bản ghi | Dung lượng tệp | Mục đích sử dụng |
|---|---|:---:|:---:|---|
| **Master Raw File** | `data/raw/All_Beauty_full.jsonl` | 701.528 | 311.5 MB | Dữ liệu gốc lưu trữ thô |
| **Natural Dataset 100k** | `data/processed/all_beauty_100k_natural.jsonl` | 100.000 | 68.4 MB | Đo lường hiệu năng thực tế |
| **Natural Test Set 20k** | `data/processed/all_beauty_natural_test.jsonl` | 20.000 | 13.7 MB | Đánh giá chung cho mọi mô hình |
| **Natural Train Set 80k** | `data/processed/all_beauty_natural_train.jsonl` | 80.000 | 54.7 MB | Huấn luyện baseline tự nhiên |
| **Balanced Train Set 45k** | `data/processed/all_beauty_balanced_train.jsonl` | 45.000 | 28.7 MB | Huấn luyện mô hình cân bằng (1:1:1) |

---

## 5. ĐỒNG BỘ VÀ KIỂM ĐỊNH TRÊN HADOOP HDFS

Tất cả các tệp dữ liệu thô và đã xử lý đã được tải lên cụm Hadoop HDFS single-node trên WSL2 Ubuntu 24.04:

### 5.1. Cấu trúc thư mục trên HDFS
```
hdfs://localhost:9000/
└── data/
    └── amazon-reviews/
        ├── raw/
        │   ├── All_Beauty_full.jsonl       (311.5 MB - Master Raw)
        │   └── all_beauty_25k.jsonl        (13.0 MB  - Mẫu Phase 2)
        └── processed/
            ├── all_beauty_100k_natural.jsonl (68.4 MB - 100k mẫu tự nhiên)
            ├── all_beauty_natural_test.jsonl (13.7 MB - 20k mẫu test tự nhiên)
            └── all_beauty_balanced_train.jsonl (28.7 MB - 45k mẫu train cân bằng)
```

### 5.2. Kết quả kiểm tra lệnh thực tế trên HDFS
```bash
$ hdfs dfs -ls -h /data/amazon-reviews/processed/
Found 3 items
-rw-r--r--   1 tzy supergroup     68.4 M 2026-10-15 16:46 /data/amazon-reviews/processed/all_beauty_100k_natural.jsonl
-rw-r--r--   1 tzy supergroup     28.7 M 2026-10-15 16:46 /data/amazon-reviews/processed/all_beauty_balanced_train.jsonl
-rw-r--r--   1 tzy supergroup     13.7 M 2026-10-15 16:46 /data/amazon-reviews/processed/all_beauty_natural_test.jsonl

$ hdfs fsck /data/amazon-reviews -files -blocks
Status: HEALTHY
Total size:	456425772 B (456.4 MB)
Total blocks (validated):	7 (avg. block size 65203681 B)
The filesystem under path '/data/amazon-reviews' is HEALTHY
```

---

## 6. HƯỚNG DẪN TÁI HIỆN TOÀN BỘ (REPRODUCTION INSTRUCTIONS)

### Bước 1: Khởi động WSL2 và kiểm tra HDFS
```bash
wsl -d Ubuntu-24.04
hdfs dfsadmin -report
```

### Bước 2: Chạy script thu thập và làm sạch dữ liệu
```bash
python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/collect_balanced_reviews.py
```

### Bước 3: Đưa dữ liệu đã xử lý vào HDFS
```bash
hdfs dfs -mkdir -p /data/amazon-reviews/processed
hdfs dfs -put -f /mnt/d/Filehoc/big-data/emotion-sentiment/data/processed/*.jsonl /data/amazon-reviews/processed/
hdfs fsck /data/amazon-reviews/processed -files -blocks
```
