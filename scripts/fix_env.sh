#!/usr/bin/env bash
set -e

# Create symlink for SPARK_HOME
ln -sf /home/tzy/.local/lib/python3.12/site-packages/pyspark /opt/spark

# /etc/profile.d/bigdata.sh for login shells
cat << 'EOF' > /etc/profile.d/bigdata.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PATH="/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$JAVA_HOME/bin:$HOME/.local/bin:$PATH"
EOF
chmod +x /etc/profile.d/bigdata.sh

# /etc/environment for PAM & systemd
cat << 'EOF' > /etc/environment
PATH="/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:/usr/lib/jvm/java-11-openjdk-amd64/bin:/home/tzy/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_HOME="/opt/hadoop"
HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
SPARK_HOME="/opt/spark"
PYSPARK_PYTHON="python3"
EOF

# Ensure environment is injected before interactive check in ~/.bashrc
sed -i '/BIG DATA STACK/d' /home/tzy/.bashrc
sed -i '/JAVA_HOME/d' /home/tzy/.bashrc
sed -i '/HADOOP_HOME/d' /home/tzy/.bashrc
sed -i '/SPARK_HOME/d' /home/tzy/.bashrc
sed -i '/PYSPARK_PYTHON/d' /home/tzy/.bashrc

TMP_FILE=$(mktemp)
cat << 'EOF' > "$TMP_FILE"
# Big Data Environment (Exported at top for non-interactive shells)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PATH="/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$JAVA_HOME/bin:$HOME/.local/bin:$PATH"

EOF
cat /home/tzy/.bashrc >> "$TMP_FILE"
mv "$TMP_FILE" /home/tzy/.bashrc
chown tzy:tzy /home/tzy/.bashrc

echo "Environment successfully configured."

