#!/usr/bin/env bash
# =============================================================
# load_to_hdfs.sh
# Tải 25.000 reviews vào HDFS directory /data/amazon-reviews/raw/
# Kiểm tra tính toàn vẹn và dung lượng block trên HDFS
# =============================================================
set -e

# Load environment
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

LOCAL_DATA="/mnt/d/Filehoc/big-data/emotion-sentiment/data/raw/all_beauty_25k.jsonl"
HDFS_DIR="/data/amazon-reviews/raw"
HDFS_TARGET="$HDFS_DIR/all_beauty_25k.jsonl"

echo "=============================================="
echo " TẢI DỮ LIỆU VÀO HADOOP HDFS"
echo "=============================================="
echo "Nguồn cục bộ: $LOCAL_DATA"
echo "Đích HDFS   : $HDFS_TARGET"
echo ""

# 1. Kiểm tra file nguồn
if [ ! -f "$LOCAL_DATA" ]; then
    echo "ERROR: Không tìm thấy file $LOCAL_DATA!"
    exit 1
fi
LOCAL_SIZE=$(ls -lh "$LOCAL_DATA" | awk '{print $5}')
LOCAL_LINES=$(wc -l < "$LOCAL_DATA")
echo "✓ File nguồn sẵn sàng: $LOCAL_LINES dòng ($LOCAL_SIZE)"
echo ""

# 2. Đảm bảo Hadoop HDFS đang chạy
echo "Kiểm tra HDFS cluster..."
if ! hdfs dfs -ls / >/dev/null 2>&1; then
    echo "HDFS chưa chạy, đang khởi động HDFS..."
    sudo service ssh restart 2>/dev/null || true
    start-dfs.sh
    sleep 5
fi

# 3. Tạo thư mục đích trên HDFS
echo "Đảm bảo thư mục HDFS tồn tại: $HDFS_DIR"
hdfs dfs -mkdir -p "$HDFS_DIR"

# 4. Upload file vào HDFS
echo "Đang upload vào HDFS (hdfs dfs -put -f)..."
hdfs dfs -put -f "$LOCAL_DATA" "$HDFS_TARGET"
echo "✓ Upload thành công!"
echo ""

# 5. Kiểm tra tính toàn vẹn và thông tin block trên HDFS
echo "=== THÔNG TIN FILE TRÊN HDFS ==="
hdfs dfs -ls -h "$HDFS_TARGET"
echo ""
echo "=== KIỂM TRA HDFS HEALTH & REPLICATION (fsck) ==="
hdfs fsck "$HDFS_TARGET" -files -blocks -locations 2>&1 | grep -E "Status:|Total blocks|Total size|CORRUPT" || true
echo ""
echo "=== XEM THỬ 2 DÒNG ĐẦU TRỰC TIẾP TỪ HDFS ==="
hdfs dfs -cat "$HDFS_TARGET" | head -n 2 | python3 -m json.tool --compact || hdfs dfs -cat "$HDFS_TARGET" | head -n 2
echo ""
echo "=============================================="
echo " HOÀN TẤT NẠP DỮ LIỆU VÀO HDFS"
echo "=============================================="
