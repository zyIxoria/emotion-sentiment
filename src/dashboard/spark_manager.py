"""
Module: src.dashboard.spark_manager
Quản lý vòng đời SparkSession, kết nối HDFS và xử lý tài nguyên phân tán.
"""
import os
import socket
import logging
from typing import Optional, Tuple

logger = logging.getLogger("spark_manager")

class SparkManagerError(Exception):
    """Lỗi cơ sở cho các vấn đề liên quan đến SparkManager."""
    pass

class PySparkUnavailableError(SparkManagerError):
    """Ngoại lệ khi PySpark chưa được cài đặt trong môi trường hiện tại."""
    pass

class HDFSConnectionError(SparkManagerError):
    """Ngoại lệ khi không thể kết nối hoặc giao tiếp với Hadoop HDFS daemon."""
    pass

class SparkSessionError(SparkManagerError):
    """Ngoại lệ khi không thể khởi tạo hoặc cấu hình SparkSession."""
    pass

# Biến toàn cục lưu trữ singleton SparkSession
_GLOBAL_SPARK_SESSION = None

def check_hdfs_connectivity(hdfs_uri: str = "hdfs://localhost:9000", timeout_sec: float = 2.0) -> Tuple[bool, str]:
    """
    Kiểm tra nhanh xem RPC port của HDFS có đang lắng nghe hay không.
    Tránh trường hợp Spark bị treo vô hạn khi HDFS NameNode chưa khởi động.
    """
    try:
        # Tách host và port từ URI
        clean_uri = hdfs_uri.replace("hdfs://", "").replace("http://", "")
        if ":" in clean_uri:
            host, port_str = clean_uri.split(":", 1)
            port = int(port_str.split("/")[0])
        else:
            host = clean_uri.split("/")[0]
            port = 9000

        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, f"HDFS RPC sẵn sàng tại {host}:{port}"
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        return False, f"Không thể kết nối HDFS ({host}:{port}): {str(e)}"

def get_spark_session(
    app_name: str = "AmazonReviewsSentimentDashboard",
    master: str = "local[*]",
    driver_memory: str = "4g",
    hdfs_uri: str = "hdfs://localhost:9000",
    check_hdfs: bool = True
):
    """
    Tạo hoặc trả về SparkSession singleton đang hoạt động.
    
    Yêu cầu:
    - Sử dụng singleton pattern để tránh khởi tạo nhiều SparkContext gây lãng phí RAM.
    - Cấu hình driver memory 4GB và local[*] phù hợp môi trường Big Data.
    - Phân loại rõ ràng các ngoại lệ: thiếu PySpark, HDFS offline, lỗi khởi tạo Java/Spark.
    """
    global _GLOBAL_SPARK_SESSION

    # 1. Kiểm tra singleton hiện tại
    if _GLOBAL_SPARK_SESSION is not None:
        try:
            if not _GLOBAL_SPARK_SESSION.sparkContext._jsc.sc().isStopped():
                return _GLOBAL_SPARK_SESSION
        except Exception:
            _GLOBAL_SPARK_SESSION = None

    # 2. Kiểm tra sự tồn tại của thư viện PySpark
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise PySparkUnavailableError(
            "Thư viện 'pyspark' chưa được cài đặt trong môi trường Python hiện tại. "
            "Vui lòng chạy: pip install pyspark==3.5.5 hoặc kích hoạt đúng môi trường WSL/venv."
        ) from e

    # 3. Kiểm tra kết nối HDFS trước khi gán defaultFS nếu được yêu cầu
    if check_hdfs and hdfs_uri.startswith("hdfs://"):
        is_ok, msg = check_hdfs_connectivity(hdfs_uri, timeout_sec=1.5)
        if not is_ok:
            raise HDFSConnectionError(
                f"Lỗi kết nối HDFS: {msg}. "
                "Vui lòng khởi động HDFS bằng lệnh: sudo systemctl start hadoop-hdfs (hoặc start-dfs.sh)."
            )

    # 4. Khởi tạo SparkSession
    try:
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.ui.showConsoleProgress", "false")

        if hdfs_uri:
            builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)

        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        _GLOBAL_SPARK_SESSION = session
        return _GLOBAL_SPARK_SESSION
    except Exception as e:
        raise SparkSessionError(
            f"Không thể khởi động SparkSession: {str(e)}. "
            "Kiểm tra lại cấu hình JAVA_HOME (yêu cầu Java 8/11/17) và tài nguyên RAM khả dụng."
        ) from e

def stop_spark_session():
    """
    Dừng an toàn SparkSession singleton và giải phóng tài nguyên CPU/RAM.
    """
    global _GLOBAL_SPARK_SESSION
    if _GLOBAL_SPARK_SESSION is not None:
        try:
            _GLOBAL_SPARK_SESSION.stop()
        except Exception as e:
            logger.warning("Lỗi khi dừng SparkSession: %s", e)
        finally:
            _GLOBAL_SPARK_SESSION = None
