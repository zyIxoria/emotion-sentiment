#!/usr/bin/env bash
# =============================================================
# setup_bigdata_stack.sh
# Cài đặt: Java 11, Hadoop 3.3.6 (HDFS), PySpark 3.5.x
# Môi trường: WSL2 Ubuntu 24.04
# Đồ án: Phân tích cảm xúc Amazon Reviews 2023
# =============================================================
set -e  # Dừng ngay khi có lỗi

LOG_FILE="$HOME/setup_bigdata.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=============================================="
echo " BIG DATA STACK SETUP - $(date)"
echo "=============================================="
echo ""

# -------------------------------------------------------
# BƯỚC 0: Cập nhật apt
# -------------------------------------------------------
echo "[0/6] Cập nhật apt package list..."
sudo apt-get update -qq
echo "    ✓ apt updated"
echo ""

# -------------------------------------------------------
# BƯỚC 1: Cài Java 11
# -------------------------------------------------------
echo "[1/6] Cài đặt OpenJDK 11..."
if java -version 2>/dev/null | grep -q "11\|17\|21"; then
    echo "    ✓ Java đã được cài (bỏ qua)"
else
    sudo apt-get install -y openjdk-11-jdk python3-pip python3-venv openssh-server openssh-client -qq
    echo "    ✓ Java 11 & Python tools đã cài xong"
fi

JAVA_PATH=$(readlink -f /usr/bin/java | sed 's|/bin/java||')
echo "    Java home: $JAVA_PATH"
java -version
echo ""

# -------------------------------------------------------
# BƯỚC 2: Đặt biến môi trường Java
# -------------------------------------------------------
echo "[2/6] Cấu hình JAVA_HOME và PATH..."

BASHRC="$HOME/.bashrc"

# Xóa cấu hình cũ nếu có để tránh duplicate
grep -v "JAVA_HOME\|HADOOP_HOME\|SPARK_HOME\|hadoop\|spark" "$BASHRC" > /tmp/bashrc_clean 2>/dev/null || true
cp /tmp/bashrc_clean "$BASHRC"

cat >> "$BASHRC" << 'ENVEOF'

# ============ BIG DATA STACK (auto-added by setup_bigdata_stack.sh) ============
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH
# ==============================================================================
ENVEOF

source "$BASHRC" 2>/dev/null || true
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
echo "    ✓ JAVA_HOME=$JAVA_HOME"
echo ""

# -------------------------------------------------------
# BƯỚC 3: Cài Hadoop 3.3.6
# -------------------------------------------------------
HADOOP_VERSION="3.3.6"
HADOOP_DIR="/opt/hadoop"

echo "[3/6] Cài đặt Hadoop $HADOOP_VERSION..."

if [ -d "$HADOOP_DIR" ] && [ -f "$HADOOP_DIR/bin/hadoop" ]; then
    echo "    ✓ Hadoop đã cài tại $HADOOP_DIR (bỏ qua download)"
else
    echo "    Downloading Hadoop $HADOOP_VERSION (~650MB)..."
    HADOOP_TGZ="/tmp/hadoop-${HADOOP_VERSION}.tar.gz"
    
    if [ ! -f "$HADOOP_TGZ" ]; then
        wget -q --show-progress \
            "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
            -O "$HADOOP_TGZ"
    fi
    
    echo "    Extracting..."
    sudo tar -xzf "$HADOOP_TGZ" -C /opt/
    sudo mv /opt/hadoop-${HADOOP_VERSION} "$HADOOP_DIR"
    sudo chown -R $USER:$USER "$HADOOP_DIR"
    echo "    ✓ Hadoop extracted to $HADOOP_DIR"
fi

export HADOOP_HOME="$HADOOP_DIR"
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
$HADOOP_HOME/bin/hadoop version | head -1
echo ""

# -------------------------------------------------------
# BƯỚC 4: Cấu hình Hadoop (HDFS Pseudo-distributed mode)
# -------------------------------------------------------
echo "[4/6] Cấu hình Hadoop HDFS (pseudo-distributed / single-node)..."

HADOOP_CONF="$HADOOP_HOME/etc/hadoop"

# 4a. Đặt JAVA_HOME trong hadoop-env.sh
sudo bash -c "sed -i 's|# export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64|' $HADOOP_CONF/hadoop-env.sh"

# 4b. core-site.xml
sudo bash -c "cat > $HADOOP_CONF/core-site.xml" << 'COREEOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-${user.name}</value>
  </property>
</configuration>
COREEOF

# 4c. hdfs-site.xml
sudo bash -c "cat > $HADOOP_CONF/hdfs-site.xml" << 'HDFSEOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/${user.name}/hadoopdata/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/${user.name}/hadoopdata/datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
HDFSEOF

# 4d. mapred-site.xml
sudo bash -c "cat > $HADOOP_CONF/mapred-site.xml" << 'MAPEOF'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
MAPEOF

# 4e. yarn-site.xml
sudo bash -c "cat > $HADOOP_CONF/yarn-site.xml" << 'YARNEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
YARNEOF

echo "    ✓ Hadoop config files written"

# 4f. SSH setup (cần cho Hadoop start scripts)
echo "    Cấu hình SSH localhost..."
sudo apt-get install -y openssh-server openssh-client -qq
sudo ssh-keygen -A 2>/dev/null || true
sudo service ssh restart 2>/dev/null || sudo service ssh start 2>/dev/null || true

# Tạo SSH key nếu chưa có
if [ ! -f "$HOME/.ssh/id_rsa" ]; then
    ssh-keygen -t rsa -P '' -f "$HOME/.ssh/id_rsa" -q
fi
cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys" 2>/dev/null
chmod 600 "$HOME/.ssh/authorized_keys"
# Trust localhost
ssh-keyscan -H localhost >> "$HOME/.ssh/known_hosts" 2>/dev/null
ssh-keyscan -H 0.0.0.0 >> "$HOME/.ssh/known_hosts" 2>/dev/null
echo "    ✓ SSH configured"

# 4g. Format namenode (chỉ khi chưa format)
NAMENODE_DIR="$HOME/hadoopdata/namenode"
if [ ! -d "$NAMENODE_DIR" ]; then
    mkdir -p "$NAMENODE_DIR"
    mkdir -p "$HOME/hadoopdata/datanode"
    echo "Y" | $HADOOP_HOME/bin/hdfs namenode -format -force 2>&1 | tail -5
    echo "    ✓ HDFS NameNode formatted"
else
    echo "    ✓ HDFS NameNode đã format trước đó (bỏ qua)"
fi
echo ""

# -------------------------------------------------------
# BƯỚC 5: Khởi động HDFS
# -------------------------------------------------------
echo "[5/6] Khởi động HDFS..."

# Dừng nếu đang chạy
$HADOOP_HOME/sbin/stop-dfs.sh 2>/dev/null || true
sleep 2

# Khởi động lại
$HADOOP_HOME/sbin/start-dfs.sh

sleep 5  # Đợi HDFS ổn định

echo "    Kiểm tra HDFS..."
$HADOOP_HOME/bin/hdfs dfs -ls / 2>&1 | head -5 || echo "    ⚠ HDFS ls failed - kiểm tra sau"

echo "    Tạo thư mục project trên HDFS..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/$USER || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /bigdata/reviews/raw /bigdata/reviews/processed /bigdata/reviews/test /bigdata/models || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/amazon-reviews/raw /data/amazon-reviews/samples /data/amazon-reviews/processed || true

echo "    ✓ HDFS directories created"
$HADOOP_HOME/bin/hdfs dfs -ls /bigdata/reviews/ 2>/dev/null || true
echo ""

# -------------------------------------------------------
# BƯỚC 6: Cài PySpark 3.5.x
# -------------------------------------------------------
echo "[6/6] Cài đặt PySpark và thư viện ML..."

sudo apt-get install -y python3-pip python3-venv -qq
python3 -m pip install --quiet --break-system-packages pyspark==3.5.5 scikit-learn pandas numpy pyyaml

# Cấu hình Systemd Service hadoop-hdfs
echo "    Đăng ký systemd service hadoop-hdfs..."
sudo bash -c "cat > /etc/systemd/system/hadoop-hdfs.service" << SVCEOF
[Unit]
Description=Hadoop HDFS Daemons (NameNode, DataNode, SecondaryNameNode)
After=network.target ssh.service
Wants=ssh.service

[Service]
Type=forking
User=$USER
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
Environment="HADOOP_HOME=/opt/hadoop"
Environment="HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop"
ExecStart=/opt/hadoop/sbin/start-dfs.sh
ExecStop=/opt/hadoop/sbin/stop-dfs.sh
Restart=on-failure
RestartSec=5
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SVCEOF

sudo systemctl daemon-reload
sudo systemctl enable hadoop-hdfs
sudo systemctl restart hadoop-hdfs || true
echo "    ✓ hadoop-hdfs service enabled & started"

echo "    Kiểm tra PySpark..."
python3 -c "
import pyspark
print('    ✓ PySpark version:', pyspark.__version__)
import sklearn
print('    ✓ scikit-learn version:', sklearn.__version__)
import pandas as pd
print('    ✓ pandas version:', pd.__version__)
import numpy as np
print('    ✓ numpy version:', np.__version__)
"

echo ""
echo "=============================================="
echo " SETUP HOÀN TẤT - $(date)"
echo "=============================================="
echo ""
echo "Tóm tắt:"
java -version 2>&1 | head -1
$HADOOP_HOME/bin/hadoop version | head -1
python3 -c "import pyspark; print('PySpark', pyspark.__version__)"
echo ""
echo "Log đầy đủ tại: $LOG_FILE"
echo ""
echo "Bước tiếp theo:"
echo "  1. Tải 25K reviews vào HDFS: bash /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/load_to_hdfs.sh"
echo "  2. Chạy PySpark pipeline: python3 /mnt/d/Filehoc/big-data/emotion-sentiment/scripts/spark_pipeline.py"
