"""
Ứng dụng: Streamlit Dashboard Cao Cấp - Phân tích Cảm xúc Đánh giá Trực tuyến (Amazon Reviews 2023)
Hệ sinh thái: Hadoop HDFS 3.3.6, Apache Spark 3.5.5, Streamlit, Plotly
Thiết kế: Modern Glassmorphism / Enterprise FinTech AI Theme
"""
import os
import sys
import yaml
import pandas as pd
import streamlit as st

# Thêm thư mục gốc vào PYTHONPATH
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.dashboard.spark_manager import (
    get_spark_session,
    stop_spark_session,
    check_hdfs_connectivity
)
from src.dashboard.model_service import (
    rating_to_sentiment,
    load_phase4_best_model,
    predict_single_review,
    validate_review_input,
    LABEL_NAMES
)
from src.dashboard.data_service import (
    load_offline_reports,
    get_hdfs_dataset_overview,
    get_data_explorer_sample
)
from src.dashboard.visualization import (
    plot_sentiment_distribution,
    plot_rating_distribution,
    plot_prediction_probabilities,
    plot_confusion_matrix_heatmap,
    plot_models_comparison_chart,
    COLOR_PALETTE
)

# -------------------------------------------------------------
# 1. CẤU HÌNH TRANG & HỆ THỐNG GIAO DIỆN CAO CẤP
# -------------------------------------------------------------
st.set_page_config(
    page_title="Amazon Reviews — Big Data Sentiment Intelligence",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS Enterprise Theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Main Container Padding */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* Glassmorphic Hero Banner */
    .hero-banner {
        background: linear-gradient(135deg, #0F172A 0%, #1E1B4B 50%, #312E81 100%);
        border-radius: 16px;
        padding: 32px 36px;
        color: #FFFFFF;
        box-shadow: 0 10px 25px -5px rgba(15, 23, 42, 0.3), 0 8px 10px -6px rgba(15, 23, 42, 0.2);
        margin-bottom: 24px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        position: relative;
        overflow: hidden;
    }
    .hero-title {
        font-size: 2.1rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin-bottom: 10px;
        background: linear-gradient(90deg, #FFFFFF 0%, #E2E8F0 50%, #93C5FD 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .hero-desc {
        font-size: 1.05rem;
        color: #CBD5E1;
        max-width: 900px;
        line-height: 1.6;
        margin-bottom: 18px;
    }
    .pill-tag {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(255, 255, 255, 0.12);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 9999px;
        padding: 5px 14px;
        font-size: 0.85rem;
        font-weight: 600;
        color: #F8FAFC;
        margin-right: 8px;
        margin-bottom: 6px;
    }

    /* Metric & KPI Cards */
    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 14px;
        padding: 20px 22px;
        box-shadow: 0 2px 8px -2px rgba(15, 23, 42, 0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 20px -5px rgba(15, 23, 42, 0.1);
        border-color: #CBD5E1;
    }
    .kpi-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 10px;
    }
    .kpi-title {
        font-size: 0.82rem;
        font-weight: 700;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kpi-icon {
        width: 36px;
        height: 36px;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.2rem;
    }
    .kpi-value {
        font-size: 1.95rem;
        font-weight: 800;
        color: #0F172A;
        letter-spacing: -0.02em;
        line-height: 1.2;
        margin-bottom: 6px;
    }
    .kpi-sub {
        font-size: 0.83rem;
        font-weight: 600;
    }
    .sub-pos { color: #059669; }
    .sub-muted { color: #64748B; }

    /* Prediction Result Cards */
    .pred-card-pos {
        background: linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%);
        border: 2px solid #10B981;
        border-radius: 14px;
        padding: 24px;
        color: #065F46;
    }
    .pred-card-neu {
        background: linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%);
        border: 2px solid #F59E0B;
        border-radius: 14px;
        padding: 24px;
        color: #92400E;
    }
    .pred-card-neg {
        background: linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%);
        border: 2px solid #EF4444;
        border-radius: 14px;
        padding: 24px;
        color: #991B1B;
    }

    /* Pulse Status Dot */
    .pulse-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
        animation: pulse-animation 2s infinite;
    }
    .dot-green { background-color: #10B981; box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
    .dot-amber { background-color: #F59E0B; box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.7); }
    @keyframes pulse-animation {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Architecture Stepper */
    .step-box {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        border-top: 4px solid #4F46E5;
        transition: transform 0.2s ease;
    }
    .step-box:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 12px -2px rgba(0,0,0,0.06);
    }
    .step-num {
        font-size: 0.75rem;
        font-weight: 800;
        color: #4F46E5;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .step-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: #0F172A;
        margin-bottom: 4px;
    }
    .step-desc {
        font-size: 0.8rem;
        color: #64748B;
        line-height: 1.4;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# 2. CẤU HÌNH DỰ ÁN
# -------------------------------------------------------------
@st.cache_data
def load_app_config():
    config_path = os.path.join(BASE_DIR, "configs", "dataset_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}

config = load_app_config()
dash_cfg = config.get("dashboard", {})
model_cfg = dash_cfg.get("model", {})
dataset_cfg = dash_cfg.get("datasets", {})
spark_cfg = dash_cfg.get("spark", {})

HDFS_URI = dash_cfg.get("hdfs_uri", "hdfs://localhost:9000")
DEFAULT_MODEL_HDFS_PATH = model_cfg.get("hdfs_path", f"{HDFS_URI}/bigdata/models/phase4_best_model")
DEFAULT_PARQUET_PATH = dataset_cfg.get("natural_100k_parquet", f"{HDFS_URI}/bigdata/reviews/processed/all_beauty_100k_natural.parquet")

# -------------------------------------------------------------
# 3. QUẢN LÝ TÀI NGUYÊN SPARK VÀ CACHE MÔ HÌNH
# -------------------------------------------------------------
@st.cache_resource(show_spinner="⚡ Đang khởi tạo SparkSession phân tán...")
def get_cached_spark():
    try:
        spark = get_spark_session(
            app_name=spark_cfg.get("app_name", "AmazonReviewsSentimentDashboard"),
            master=spark_cfg.get("master", "local[*]"),
            driver_memory=spark_cfg.get("driver_memory", "4g"),
            hdfs_uri=HDFS_URI,
            check_hdfs=True
        )
        return spark, None
    except Exception as e:
        return None, str(e)

@st.cache_resource(show_spinner="📦 Đang tải PipelineModel từ Hadoop HDFS...")
def get_cached_model(_spark, model_path: str):
    if _spark is None:
        return None, "SparkSession chưa khởi động."
    try:
        model, resolved_path = load_phase4_best_model(_spark, model_path)
        return model, None
    except Exception as e:
        return None, str(e)

# -------------------------------------------------------------
# 4. SIDEBAR ĐIỀU HÀNH & GIÁM SÁT TRẠNG THÁI
# -------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:16px;">
        <span style="font-size:2.2rem;">🛍️</span>
        <div>
            <h3 style="margin:0; font-size:1.15rem; font-weight:800; color:#0F172A;">Amazon Analytics</h3>
            <span style="font-size:0.75rem; color:#64748B; font-weight:600;">Big Data Emotion Engine</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("#### 📡 Trạng thái Hệ thống (Live Status)")

    # Kiểm tra kết nối HDFS daemon
    hdfs_ok, hdfs_msg = check_hdfs_connectivity(HDFS_URI, timeout_sec=1.0)
    if hdfs_ok:
        st.markdown('<p><span class="pulse-dot dot-green"></span><b>Hadoop HDFS RPC:</b> <span style="color:#059669; font-weight:600;">Online (Port 9000)</span></p>', unsafe_allow_html=True)
    else:
        st.markdown('<p><span class="pulse-dot dot-amber"></span><b>Hadoop HDFS RPC:</b> <span style="color:#D97706; font-weight:600;">Offline</span></p>', unsafe_allow_html=True)

    spark_inst = None
    spark_err = None
    if hdfs_ok:
        spark_inst, spark_err = get_cached_spark()
        if spark_inst is not None:
            st.markdown('<p><span class="pulse-dot dot-green"></span><b>Apache Spark:</b> <span style="color:#059669; font-weight:600;">Active (local[*])</span></p>', unsafe_allow_html=True)
        else:
            st.error(f"Spark Error: {spark_err}")
    else:
        st.info("ℹ️ Khởi động HDFS daemon để mở kết nối Spark.")

    model_inst = None
    model_err = None
    if spark_inst is not None:
        model_inst, model_err = get_cached_model(spark_inst, DEFAULT_MODEL_HDFS_PATH)
        if model_inst is not None:
            st.markdown('<p><span class="pulse-dot dot-green"></span><b>PipelineModel:</b> <span style="color:#059669; font-weight:600;">M2c_LR_elastic (Loaded)</span></p>', unsafe_allow_html=True)
        else:
            st.error(f"Model Error: {model_err}")

    st.markdown("---")
    st.markdown("#### ⚙️ Thông số Pipeline")
    st.markdown(f"""
    - **Engine:** PySpark `3.5.5`
    - **Driver RAM:** `4 GB`
    - **Features:** TF-IDF (10k n-grams)
    - **Classifier:** ElasticNet (`reg=0.01, l1=0.5`)
    - **Ground-truth:** Amazon 1–5 ⭐
    """)

    st.markdown("---")
    if st.button("🔄 Giải phóng & Khởi động lại Spark", use_container_width=True):
        stop_spark_session()
        st.cache_resource.clear()
        st.rerun()

    st.caption("Đồ án Nhập môn Big Data • 2026")

# -------------------------------------------------------------
# 5. HERO BANNER CHÍNH
# -------------------------------------------------------------
st.markdown("""
<div class="hero-banner">
    <div class="hero-title">Amazon Reviews 2023 — Sentiment Intelligence</div>
    <div class="hero-desc">
        Hệ thống phân tích và suy luận cảm xúc phân tán từ <b>701.528 đánh giá sản phẩm làm đẹp trực tuyến</b>. 
        Tích hợp toàn trình luồng dữ liệu lớn với <b>Hadoop HDFS 3.3.6</b>, <b>Apache Spark 3.5.5 MLlib</b> và mô hình tối ưu <b>ElasticNet Logistic Regression</b>.
    </div>
    <div>
        <span class="pill-tag">⚡ Apache Spark 3.5.5</span>
        <span class="pill-tag">🐘 Hadoop HDFS 3.3.6</span>
        <span class="pill-tag">🎯 Best Model: 75.40% Accuracy</span>
        <span class="pill-tag">📊 701K Amazon Reviews</span>
    </div>
</div>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# 6. TAB NAVIGATION CHÍNH
# -------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Tổng quan (Overview)",
    "🔮 Dự đoán Cảm xúc (Live Predictor)",
    "📈 Đánh giá Mô hình (Model Benchmark)",
    "🔍 Khám phá Dữ liệu (Data Explorer)"
])

# -------------------------------------------------------------
# TAB 1: TỔNG QUAN KIẾN TRÚC & DỮ LIỆU
# -------------------------------------------------------------
with tab1:
    st.markdown("### 1. Kiến trúc Luồng Dữ liệu Lớn Toàn trình (End-to-End Pipeline)")
    
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    with col_s1:
        st.markdown("""
        <div class="step-box" style="border-top-color:#3B82F6;">
            <div class="step-num">BƯỚC 1 • INGESTION</div>
            <div style="font-size:1.8rem; margin:6px 0;">📥</div>
            <div class="step-title">Thu thập Dữ liệu</div>
            <div class="step-desc">701.528 reviews thô từ McAuley-Lab (311.5 MB JSONL). Khảo sát schema và tiền xử lý.</div>
        </div>
        """, unsafe_allow_html=True)
    with col_s2:
        st.markdown("""
        <div class="step-box" style="border-top-color:#06B6D4;">
            <div class="step-num">BƯỚC 2 • STORAGE</div>
            <div style="font-size:1.8rem; margin:6px 0;">🐘</div>
            <div class="step-title">Hadoop HDFS</div>
            <div class="step-desc">Lưu trữ phân tán tại RPC port 9000. Chuyển đổi và nén tối ưu sang Snappy Parquet.</div>
        </div>
        """, unsafe_allow_html=True)
    with col_s3:
        st.markdown("""
        <div class="step-box" style="border-top-color:#F59E0B;">
            <div class="step-num">BƯỚC 3 • PROCESSING</div>
            <div style="font-size:1.8rem; margin:6px 0;">⚡</div>
            <div class="step-title">PySpark MLlib</div>
            <div class="step-desc">Trích xuất đặc trưng TF-IDF 10k từ vựng, n-grams và huấn luyện 8 mô hình học máy.</div>
        </div>
        """, unsafe_allow_html=True)
    with col_s4:
        st.markdown("""
        <div class="step-box" style="border-top-color:#10B981;">
            <div class="step-num">BƯỚC 4 • SERVING</div>
            <div style="font-size:1.8rem; margin:6px 0;">🎯</div>
            <div class="step-title">Interactive UI</div>
            <div class="step-desc">Streamlit Dashboard & Plotly phục vụ trực quan hóa và suy luận cảm xúc thời gian thực.</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 2. Thống kê Phân phối Dữ liệu Tự nhiên (Natural Dataset 100k)")

    # Đọc trực tiếp từ HDFS nếu online
    if spark_inst is not None:
        with st.spinner("Đang truy vấn thống kê phân tán từ HDFS..."):
            overview = get_hdfs_dataset_overview(spark_inst, DEFAULT_PARQUET_PATH)

        if overview["success"]:
            df_sent = overview["sentiment_distribution"]
            df_rate = overview["rating_distribution"]

            c_o1, c_o2 = st.columns([1, 1])
            with c_o1:
                st.plotly_chart(plot_sentiment_distribution(df_sent), use_container_width=True)
            with c_o2:
                st.plotly_chart(plot_rating_distribution(df_rate), use_container_width=True)
        else:
            st.warning(f"⚠️ {overview['error']}")
    else:
        # Số liệu chuẩn xác minh từ Phase 2
        df_sent_static = pd.DataFrame([
            {"sentiment": "Positive", "count": 73735},
            {"sentiment": "Neutral", "count": 9108},
            {"sentiment": "Negative", "count": 17157}
        ])
        df_rate_static = pd.DataFrame([
            {"rating": 1, "count": 10822},
            {"rating": 2, "count": 6335},
            {"rating": 3, "count": 9108},
            {"rating": 4, "count": 13912},
            {"rating": 5, "count": 59823}
        ])
        c_o1, c_o2 = st.columns([1, 1])
        with c_o1:
            st.plotly_chart(plot_sentiment_distribution(df_sent_static), use_container_width=True)
        with c_o2:
            st.plotly_chart(plot_rating_distribution(df_rate_static), use_container_width=True)

# -------------------------------------------------------------
# TAB 2: LIVE SENTIMENT PREDICTOR
# -------------------------------------------------------------
with tab2:
    st.markdown("### 🔮 Suy luận Cảm xúc Thời gian thực (Real-time Inference)")
    st.markdown("Nhập đánh giá bằng tiếng Anh để kiểm tra khả năng dự đoán xác suất qua PipelineModel `M2c_LR_elastic`:")

    # Quick Samples
    st.markdown("**💡 Thử nghiệm nhanh với các mẫu đánh giá thực tế:**")
    s_col1, s_col2, s_col3 = st.columns(3)
    with s_col1:
        if st.button("😊 Mẫu Tích cực (5 ⭐)", use_container_width=True):
            st.session_state["review_input"] = "This moisturizer is incredible! It leaves my skin glowing, soft, and silky smooth all day without feeling greasy."
            st.session_state["rating_select"] = 5
    with s_col2:
        if st.button("😐 Mẫu Trung tính (3 ⭐)", use_container_width=True):
            st.session_state["review_input"] = "The product is okay, works as described but the scent is rather strong and it took nearly two weeks to arrive."
            st.session_state["rating_select"] = 3
    with s_col3:
        if st.button("😞 Mẫu Tiêu cực (1 ⭐)", use_container_width=True):
            st.session_state["review_input"] = "Terrible quality, completely broke out my face and smelled like harsh cheap chemicals. Total waste of money, never again!"
            st.session_state["rating_select"] = 1

    user_text = st.text_area(
        "Nội dung nhận xét (Review Content):",
        value=st.session_state.get("review_input", ""),
        height=140,
        placeholder="Type or paste customer review text here..."
    )

    c_r1, c_r2 = st.columns([1, 2])
    with c_r1:
        user_rating = st.selectbox(
            "Số sao đánh giá (Ground-truth đối chiếu):",
            options=[5, 4, 3, 2, 1],
            index=[5, 4, 3, 2, 1].index(st.session_state.get("rating_select", 5)),
            format_func=lambda x: f"{x} ⭐"
        )
    with c_r2:
        st.write("")
        st.write("")
        predict_clicked = st.button("🚀 Thực hiện Phân tích Cảm xúc", type="primary", use_container_width=True)

    if predict_clicked:
        if not user_text.strip():
            st.error("⚠️ Vui lòng nhập nội dung đánh giá trước khi thực hiện dự đoán.")
        elif model_inst is None or spark_inst is None:
            st.error(
                "❌ PipelineModel hoặc SparkSession chưa sẵn sàng. "
                "Vui lòng đảm bảo Hadoop HDFS NameNode đang chạy (`sudo systemctl start hadoop-hdfs`) "
                "để nạp mô hình từ HDFS."
            )
        else:
            with st.spinner("⚡ Đang token hóa, tạo vector TF-IDF và suy luận qua ElasticNet Logistic Regression..."):
                pred_res = predict_single_review(
                    spark_session=spark_inst,
                    model=model_inst,
                    review_text=user_text,
                    selected_rating=user_rating
                )

            if not pred_res["success"]:
                st.error(f"❌ {pred_res['error']}")
            else:
                if pred_res.get("warning"):
                    st.warning(f"ℹ️ {pred_res['warning']}")

                st.markdown("---")
                st.markdown("#### 🎯 Kết quả Phân tích Cảm xúc Toàn diện")
                res_col1, res_col2 = st.columns([1, 1])

                lbl = pred_res["predicted_label"]
                card_class = "pred-card-pos" if lbl == "Positive" else ("pred-card-neu" if lbl == "Neutral" else "pred-card-neg")
                badge_bg = "#10B981" if lbl == "Positive" else ("#F59E0B" if lbl == "Neutral" else "#EF4444")

                with res_col1:
                    st.markdown(f"""
                    <div class="{card_class}">
                        <div style="font-size:0.8rem; font-weight:800; letter-spacing:0.05em; text-transform:uppercase; margin-bottom:6px;">KẾT QUẢ DỰ ĐOÁN</div>
                        <div style="font-size:1.8rem; font-weight:800; margin-bottom:12px;">{pred_res['predicted_display']}</div>
                        <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-weight:600; font-size:0.9rem;">
                            <span>Độ tin cậy của Mô hình (Confidence):</span>
                            <span>{pred_res['confidence']*100:.2f}%</span>
                        </div>
                        <div style="width:100%; height:8px; background:rgba(0,0,0,0.1); border-radius:4px; overflow:hidden;">
                            <div style="width:{pred_res['confidence']*100}%; height:100%; background:{badge_bg}; border-radius:4px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    cmp = pred_res.get("rating_comparison")
                    if cmp:
                        match_color = "#059669" if cmp["matches_prediction"] else "#D97706"
                        match_icon = "✅" if cmp["matches_prediction"] else "⚠️"
                        match_verdict = "Trùng khớp hoàn toàn" if cmp["matches_prediction"] else "Có sự khác biệt ngữ nghĩa"
                        st.markdown(f"""
                        <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:12px; padding:16px; margin-top:14px;">
                            <div style="font-size:0.8rem; font-weight:700; color:#64748B;">ĐỐI CHIẾU SỐ SAO (GROUND-TRUTH)</div>
                            <div style="font-size:1.05rem; font-weight:700; color:#0F172A; margin:4px 0;">
                                {cmp['selected_rating']} ⭐ tương ứng cảm xúc <b>{cmp['rating_derived_label']}</b>
                            </div>
                            <div style="color:{match_color}; font-weight:700; font-size:0.9rem;">
                                {match_icon} Kết luận đối chiếu: {match_verdict}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                with res_col2:
                    st.plotly_chart(plot_prediction_probabilities(pred_res["probabilities"]), use_container_width=True)

# -------------------------------------------------------------
# TAB 3: MODEL EVALUATION & BENCHMARKS
# -------------------------------------------------------------
with tab3:
    st.markdown("### 📈 Báo cáo Đánh giá và So sánh Thực nghiệm (Phase 3 & 4)")
    st.markdown(
        "Toàn bộ kết quả dưới đây được ghi nhận từ **20.000 mẫu kiểm thử độc lập (Natural Test Set)** "
        "được cách ly 100% trong quá trình huấn luyện."
    )

    reports_data = load_offline_reports()

    # KPI Head-to-Head
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    with kpi_col1:
        st.markdown("""
        <div class="kpi-card">
            <div>
                <div class="kpi-header">
                    <span class="kpi-title">Accuracy Tổng quan</span>
                    <span class="kpi-icon" style="background:#EFF6FF; color:#3B82F6;">🎯</span>
                </div>
                <div class="kpi-value">75.40%</div>
            </div>
            <div class="kpi-sub sub-pos">▲ +1.85% vs Baseline (73.55%)</div>
        </div>
        """, unsafe_allow_html=True)
    with kpi_col2:
        st.markdown("""
        <div class="kpi-card">
            <div>
                <div class="kpi-header">
                    <span class="kpi-title">Macro F1-Score</span>
                    <span class="kpi-icon" style="background:#EEF2FF; color:#6366F1;">⚖️</span>
                </div>
                <div class="kpi-value">63.02%</div>
            </div>
            <div class="kpi-sub sub-pos">▲ +2.14% vs Baseline (60.88%)</div>
        </div>
        """, unsafe_allow_html=True)
    with kpi_col3:
        st.markdown("""
        <div class="kpi-card">
            <div>
                <div class="kpi-header">
                    <span class="kpi-title">Neutral F1 (Lớp khó 3 ⭐)</span>
                    <span class="kpi-icon" style="background:#FFFBEB; color:#F59E0B;">😐</span>
                </div>
                <div class="kpi-value">35.91%</div>
            </div>
            <div class="kpi-sub sub-pos">▲ +2.58% vs Baseline (33.33%)</div>
        </div>
        """, unsafe_allow_html=True)
    with kpi_col4:
        st.markdown("""
        <div class="kpi-card">
            <div>
                <div class="kpi-header">
                    <span class="kpi-title">Weighted F1-Score</span>
                    <span class="kpi-icon" style="background:#ECFDF5; color:#10B981;">📊</span>
                </div>
                <div class="kpi-value">78.33%</div>
            </div>
            <div class="kpi-sub sub-pos">▲ +1.26% vs Baseline (77.07%)</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("#### 1. Ma trận Nhầm lẫn (Confusion Matrix) trên 20.000 mẫu Natural Test")

    cm_col1, cm_col2 = st.columns(2)
    with cm_col1:
        if reports_data["baseline_cm"] is not None:
            st.plotly_chart(
                plot_confusion_matrix_heatmap(reports_data["baseline_cm"], title="Baseline (Phase 3) — Test Matrix"),
                use_container_width=True
            )
    with cm_col2:
        if reports_data["final_test_cm"] is not None:
            st.plotly_chart(
                plot_confusion_matrix_heatmap(reports_data["final_test_cm"], title="Best Model M2c_LR_elastic — Test Matrix"),
                use_container_width=True
            )

    st.markdown("#### 2. Bảng Xếp hạng và So sánh 8 Thí nghiệm Cải tiến (Phase 4)")
    if reports_data["phase4_metrics_df"] is not None:
        st.plotly_chart(plot_models_comparison_chart(reports_data["phase4_metrics_df"]), use_container_width=True)

        with st.expander("📋 Xem toàn bộ bảng số liệu chi tiết các mô hình"):
            st.dataframe(reports_data["phase4_metrics_df"], use_container_width=True)

    st.markdown("""
    <div style="background:#F8FAFC; border:1px solid #E2E8F0; border-radius:12px; padding:20px; margin-top:16px;">
        <h4 style="margin-top:0; color:#0F172A;">💡 Phân tích Giới hạn & Nghịch lý Tỷ lệ nền (Base Rate Fallacy)</h4>
        <p style="color:#475569; font-size:0.9rem; line-height:1.6; margin-bottom:8px;">
            • <b>Tại sao Neutral F1 thấp hơn hai lớp còn lại?</b> Trong tập kiểm thử tự nhiên, Positive chiếm áp đảo (74.6%), trong khi Neutral chỉ chiếm 8.7%. Dù tỷ lệ đoán nhầm từ Positive sang Neutral khá nhỏ (~14.6%), số lượng tuyệt đối 2.191 mẫu nhầm lẫn này vẫn áp đảo 995 mẫu True Neutral, kéo Precision xuống 26.2%.
        </p>
        <p style="color:#475569; font-size:0.9rem; line-height:1.6; margin:0;">
            • <b>Độ bao phủ thực tế (Recall):</b> Mô hình cải tiến <code>M2c_LR_elastic</code> đạt độ bao phủ lớp Neutral là <b>56.99%</b>, chứng minh mô hình đã học tốt đặc trưng của các khách hàng phân vân 3 sao.
        </p>
    </div>
    """, unsafe_allow_html=True)

# -------------------------------------------------------------
# TAB 4: DATA EXPLORER
# -------------------------------------------------------------
with tab4:
    st.markdown("### 🔍 Khám phá Dữ liệu Lớn (Big Data Explorer)")
    st.markdown("Trích xuất và duyệt mẫu dữ liệu trực tiếp từ tập Parquet phân tán trên HDFS:")

    if spark_inst is not None:
        c_flt1, c_flt2, c_flt3 = st.columns([1, 1, 1])
        with c_flt1:
            sent_flt = st.selectbox("Lọc cảm xúc (Sentiment):", ["Tất cả", "Positive", "Neutral", "Negative"])
        with c_flt2:
            rate_flt = st.selectbox("Lọc điểm đánh giá (Rating):", [0, 5, 4, 3, 2, 1], format_func=lambda x: "Tất cả" if x == 0 else f"{x} ⭐")
        with c_flt3:
            sample_size = st.slider("Số dòng trích xuất an toàn:", min_value=5, max_value=50, value=15, step=5)

        with st.spinner("Đang quét phân tán trên HDFS..."):
            sample_res = get_data_explorer_sample(
                spark_session=spark_inst,
                parquet_path=DEFAULT_PARQUET_PATH,
                n_samples=sample_size,
                sentiment_filter=sent_flt,
                rating_filter=rate_flt
            )

        if sample_res["success"]:
            st.success(f"✓ Đã trích xuất {sample_res['returned_count']} mẫu đánh giá (Đã tự động che giấu mã user_id).")
            st.dataframe(sample_res["sample_df"], use_container_width=True)
        else:
            st.error(f"❌ {sample_res['error']}")
    else:
        st.info("ℹ️ Đang ở chế độ Offline. Dưới đây là cấu trúc Schema chuẩn hóa của bảng Parquet trên HDFS:")
        schema_sample = pd.DataFrame([
            {"Tên cột": "review_id", "Kiểu dữ liệu": "string", "Mô tả": "Mã hash sha256 duy nhất của review"},
            {"Tên cột": "rating", "Kiểu dữ liệu": "long", "Mô tả": "Số sao đánh giá (1.0 đến 5.0)"},
            {"Tên cột": "sentiment", "Kiểu dữ liệu": "string", "Mô tả": "Positive / Neutral / Negative"},
            {"Tên cột": "label", "Kiểu dữ liệu": "long", "Mô tả": "0: Positive, 1: Neutral, 2: Negative"},
            {"Tên cột": "title", "Kiểu dữ liệu": "string", "Mô tả": "Tiêu đề đánh giá"},
            {"Tên cột": "text", "Kiểu dữ liệu": "string", "Mô tả": "Nội dung nhận xét chi tiết"},
            {"Tên cột": "full_text", "Kiểu dữ liệu": "string", "Mô tả": "Đầu vào cho RegexTokenizer (title + text)"},
            {"Tên cột": "user_id", "Kiểu dữ liệu": "string", "Mô tả": "Mã người dùng (ẩn danh)"},
            {"Tên cột": "asin", "Kiểu dữ liệu": "string", "Mô tả": "Mã định danh sản phẩm Amazon"}
        ])
        st.table(schema_sample)
