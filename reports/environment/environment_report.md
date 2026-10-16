# BÁO CÁO HIỆN TRẠNG MÔI TRƯỜNG BIG DATA (ENVIRONMENT REPORT)
## Đồ án: Phân tích cảm xúc từ đánh giá sản phẩm trực tuyến
**Giai đoạn:** Phase 2 — Thiết lập môi trường Big Data và Tích hợp Dataset  
**Thời điểm kiểm tra:** 2026-10-16 03:04:52  
**Người thực hiện:** Antigravity (AI Data Engineer)  

---

## 1. THÔNG SỐ PHẦN CỨNG VÀ HỆ ĐIỀU HÀNH

| Thành phần | Thông số thực tế | Trạng thái |
|---|---|:---:|
| **Hệ điều hành máy chủ** | Windows 11 64-bit | ✅ Sẵn sàng |
| **Môi trường ảo hóa** | WSL2 (Windows Subsystem for Linux 2) | ✅ Active |
| **Bản phân phối Linux** | Ubuntu 24.04.4 LTS | ✅ Đã cấu hình |
| **Kernel Linux** | `6.18.33.1-microsoft-standard-WSL2` | ✅ Chuẩn LTS |
| **CPU** | 16 vCPUs (AMD Ryzen AI 7 350 w/ Radeon 860M) | ✅ Đủ năng lực |
| **RAM hệ thống** | 15.18 GB (Khả dụng: 13.62 GB) | ✅ Đủ cho local Spark 4G |
| **Dung lượng ổ gốc (WSL /)** | 939.13 GB trống / 1006.85 GB | ✅ Thoải mái |
| **Dung lượng ổ dự án (D:)** | 345.56 GB trống / 487.82 GB | ✅ Không gian lưu trữ lớn |

---

## 2. PHIÊN BẢN CÔNG NGHỆ VÀ TÍNH TƯƠNG THÍCH

| Công nghệ | Phiên bản thực tế | Đường dẫn cài đặt | Tương thích |
|---|---|---|:---:|
| **Python** | 3.12.3 | `/usr/bin/python3` | ✅ Chuẩn |
| **pip** | 24.0 | `/usr/bin/pip3` | ✅ |
| **Java JDK** | openjdk version "11.0.32.1" 2026-08-18 | `/usr/lib/jvm/java-11-openjdk-amd64` | ✅ Chuẩn Hadoop 3.3 & Spark 3.5 |
| **Apache Hadoop** | Hadoop 3.3.6 | `/opt/hadoop` | ✅ Single-node HDFS |
| **Apache Spark / PySpark** | 3.5.5 | Cài qua pip (`/opt/spark` bin) | ✅ Tương thích Java 11 & Python 3.12 |
| **scikit-learn** | 1.9.1 | Python site-packages | ✅ Cho các metric đánh giá |
| **pandas** | 3.0.6 | Python site-packages | ✅ |
| **numpy** | 2.5.4 | Python site-packages | ✅ |
| **aria2** | aria2 version 1.37.0 | `/usr/bin/aria2c` | ✅ Hỗ trợ tải dữ liệu đa luồng |

---

## 3. CẤU HÌNH BIẾN MÔI TRƯỜNG

Các biến môi trường đã được cấu hình cố định tại `/etc/profile.d/bigdata.sh` và `/etc/environment`:

```bash
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export HADOOP_HOME="/opt/hadoop"
export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
export SPARK_HOME="/opt/spark"
export PYSPARK_PYTHON="python3"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/lib/wsl/lib:/mnt/c/Users/ADMIN/.gemini/antigravity-ide/bin:/mnt/c/xampp/php:/mnt/d/Flutter/bin:/mnt/c/Windows/System32:/mnt/c/Windows/System32/WindowsPowerShell/v1.0/:/mnt/c/Windows:/mnt/c/laragon/bin/php/php-8.3.30-Win32-vs16-x64:/mnt/c/laragon/bin/composer:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/:/mnt/c/WINDOWS/System32/OpenSSH/:/mnt/c/laragon/bin/mysql/mysql-8.4.3-winx64/bin:/mnt/c/Users/jdk-25.0.1/bin:/mnt/c/Users/jdk-25.0.1/bin:/mnt/c/Program Files/dotnet/:/mnt/c/Program Files/Microsoft SQL Server/170/Tools/Binn/:/mnt/c/Program Files (x86)/Windows Kits/10/Windows Performance Toolkit/:/mnt/c/Program Files/Microsoft SQL Server/Client SDK/ODBC/180/Tools/Binn/:/mnt/c/Program Files/Microsoft SQL Server/170/DTS/Binn/:/mnt/c/Program Files/Microsoft SQL Server/150/Tools/Binn/:/mnt/c/Program Files/Git/cmd:/mnt/c/Users/spark-3.5.3-bin-hadoop3/bin:/mnt/c/laragon/bin/composer:/mnt/c/laragon/bin/git/bin:/mnt/c/laragon/bin/git/cmd:/mnt/c/laragon/bin/git/mingw64/bin:/mnt/c/laragon/bin/git/usr/bin:/mnt/c/laragon/bin/mysql/mysql-8.4.3-winx64/bin:/mnt/c/laragon/bin/ngrok:/mnt/c/laragon/bin/nodejs/node-v22:/mnt/c/laragon/bin/php/php-8.3.30-Win32-vs16-x64:/mnt/c/laragon/bin/python/python-3.13:/mnt/c/laragon/bin/python/python-3.13/Scripts:/mnt/c/laragon/usr/bin:/mnt/c/Users/ADMIN/AppData/Local/Yarn/config/global/node_modules/.bin:/mnt/c/Users/ADMIN/AppData/Roaming/Composer/vendor/bin:/mnt/c/Users/ADMIN/AppData/Roaming/npm:/mnt/d/Flutter/bin:/mnt/c/Users/ADMIN/AppData/Local/Programs/Microsoft VS Code/bin:/mnt/c/Users/ADMIN/AppData/Local/Microsoft/WindowsApps:/mnt/c/Program Files/JetBrains/IntelliJ IDEA 2026.1.1/bin:/mnt/c/Users/ADMIN/AppData/Local/Microsoft/WindowsApps:/mnt/c/Users/ADMIN/AppData/Local/Programs/Ollama:/mnt/c/Users/ADMIN/.dotnet/tools:/mnt/c/Users/ADMIN/AppData/Local/Programs/Antigravity IDE/bin:/mnt/c/Users/ADMIN/AppData/Local/Programs/DockerDesktop/resources/bin:/mnt/c/Python314"
```

---

## 4. TRẠNG THÁI TIẾN TRÌNH HADOOP HDFS (JPS)

Tiến trình Java đang hoạt động trên hệ thống:
```text
454 NameNode
952 SecondaryNameNode
5082 Jps
667 DataNode
```

Kiểm tra báo cáo NameNode (`hdfs dfsadmin -report`):
```text
Configured Capacity: 1081101176832 (1006.85 GB)
Present Capacity: 1009345851392 (940.03 GB)
DFS Remaining: 1008382615552 (939.13 GB)
DFS Used: 963235840 (918.61 MB)
DFS Used%: 0.10%
Replicated Blocks:
	Under replicated blocks: 28
	Blocks with corrupt replicas: 0
	Missing blocks: 0
	Missing blocks (with replication factor 1): 0
	Low redundancy blocks with highest priority to recover: 28
	Pending deletion blocks: 0
```

---

## 5. KẾT LUẬN HIỆN TRẠNG

1. Môi trường WSL2 Ubuntu 24.04 đã được thiết lập đầy đủ, ổn định và sẵn sàng cho các bài kiểm thử HDFS, Spark độc lập và tích hợp Spark-HDFS.
2. Không có xung đột phiên bản giữa Java 11, Hadoop 3.3.6 và PySpark 3.5.5.
3. NameNode, DataNode và SecondaryNameNode đều đang chạy bình thường, tài nguyên CPU và RAM đủ lớn để xử lý tập dữ liệu 100.000 mẫu.
