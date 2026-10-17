"""
Module: src.dashboard.visualization
Tạo các biểu đồ tương tác cao cấp bằng Plotly cho Streamlit Dashboard.
"""
from typing import Dict, Any, List, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Bảng màu chuẩn mực đồng nhất
COLOR_PALETTE = {
    "Positive": "#10B981",  # Emerald Green
    "Neutral": "#F59E0B",   # Amber Gold
    "Negative": "#EF4444",  # Crimson Red
    "Primary": "#4F46E5",   # Indigo
    "Secondary": "#06B6D4", # Cyan
    "DarkBg": "#0F172A",    # Slate 900
    "CardBg": "#FFFFFF",
    "Muted": "#64748B"
}

COLOR_MAP = {
    "Positive": COLOR_PALETTE["Positive"],
    "Neutral": COLOR_PALETTE["Neutral"],
    "Negative": COLOR_PALETTE["Negative"],
    "0": COLOR_PALETTE["Positive"],
    "1": COLOR_PALETTE["Neutral"],
    "2": COLOR_PALETTE["Negative"]
}

GLOBAL_FONT = dict(family="Plus Jakarta Sans, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif", size=13, color="#1E293B")

def plot_sentiment_distribution(df_sentiment: pd.DataFrame) -> go.Figure:
    """
    Vẽ biểu đồ Donut Chart cao cấp thể hiện phân phối 3 lớp cảm xúc.
    """
    total = int(df_sentiment["count"].sum())
    df_plot = df_sentiment.copy()
    df_plot["percentage"] = (df_plot["count"] / total * 100).round(2)

    fig = px.pie(
        df_plot,
        names="sentiment",
        values="count",
        color="sentiment",
        color_discrete_map=COLOR_MAP,
        hole=0.62
    )
    fig.update_traces(
        textposition="outside",
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Số lượng: <b>%{value:,}</b> mẫu<br>Tỷ lệ: <b>%{percent}</b><extra></extra>",
        marker=dict(line=dict(color="#FFFFFF", width=3))
    )
    fig.update_layout(
        font=GLOBAL_FONT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.18, xanchor="center", x=0.5),
        annotations=[
            dict(
                text=f"<span style='font-size:12px;color:#64748B;'>Tổng mẫu</span><br><b style='font-size:22px;color:#0F172A;'>{total:,}</b>",
                x=0.5, y=0.5, font=dict(family="Plus Jakarta Sans", size=16),
                showarrow=False
            )
        ],
        margin=dict(t=30, b=30, l=20, r=20),
        height=370
    )
    return fig

def plot_rating_distribution(df_rating: pd.DataFrame) -> go.Figure:
    """
    Vẽ biểu đồ cột phân phối số lượng đánh giá theo số sao (1 đến 5 sao).
    """
    df_plot = df_rating.copy().sort_values("rating")
    colors = [
        "#EF4444",  # 1 sao (Red)
        "#F87171",  # 2 sao (Soft Red)
        "#F59E0B",  # 3 sao (Amber)
        "#34D399",  # 4 sao (Soft Green)
        "#10B981"   # 5 sao (Emerald Green)
    ]

    fig = go.Figure(data=[
        go.Bar(
            x=[f"{int(r)} ⭐" for r in df_plot["rating"]],
            y=df_plot["count"],
            text=[f"{c:,}" for c in df_plot["count"]],
            textposition="outside",
            marker=dict(
                color=colors[:len(df_plot)],
                line=dict(color="rgba(255,255,255,0.8)", width=1.5),
                cornerradius=6
            ),
            hovertemplate="Đánh giá: <b>%{x}</b><br>Số lượng: <b>%{y:,}</b> reviews<extra></extra>"
        )
    ])
    fig.update_layout(
        font=GLOBAL_FONT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="", showgrid=False, linecolor="#E2E8F0"),
        yaxis=dict(title="Số lượng đánh giá", showgrid=True, gridcolor="#F1F5F9", linecolor="#E2E8F0"),
        margin=dict(t=30, b=30, l=50, r=20),
        height=370
    )
    return fig

def plot_prediction_probabilities(probabilities: Dict[str, float]) -> go.Figure:
    """
    Vẽ biểu đồ thanh ngang thể hiện phân phối xác suất dự đoán của mô hình.
    """
    classes = ["Negative", "Neutral", "Positive"]
    labels_display = ["😞 Negative", "😐 Neutral", "😊 Positive"]
    values = [probabilities.get(c, 0.0) * 100 for c in classes]
    bar_colors = [COLOR_PALETTE[c] for c in classes]

    fig = go.Figure(data=[
        go.Bar(
            y=labels_display,
            x=values,
            orientation="h",
            text=[f"<b>{v:.1f}%</b>" for v in values],
            textposition="inside",
            insidetextanchor="middle",
            marker=dict(
                color=bar_colors,
                line=dict(color="#FFFFFF", width=2),
                cornerradius=8
            ),
            hovertemplate="Lớp: <b>%{y}</b><br>Xác suất: <b>%{x:.2f}%</b><extra></extra>"
        )
    ])
    fig.update_layout(
        font=GLOBAL_FONT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(range=[0, 100], title="Xác suất phân loại (%)", showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="", showgrid=False),
        margin=dict(t=20, b=30, l=100, r=30),
        height=240
    )
    return fig

def plot_confusion_matrix_heatmap(
    cm_df_or_array,
    title: str = "Ma trận nhầm lẫn (Confusion Matrix)",
    labels: Optional[List[str]] = None
) -> go.Figure:
    """
    Vẽ ma trận nhầm lẫn dạng Heatmap với màu sắc trực quan, độ tương phản cao.
    """
    if labels is None:
        labels = ["Positive (0)", "Neutral (1)", "Negative (2)"]

    if isinstance(cm_df_or_array, pd.DataFrame):
        numeric_cols = [c for c in cm_df_or_array.columns if c != "True_Label"]
        z_data = cm_df_or_array[numeric_cols].values.tolist()
    else:
        z_data = cm_df_or_array

    # Tạo text annotation với độ tương phản sắc nét
    annotations = []
    max_val = max(map(max, z_data))
    for i in range(len(labels)):
        for j in range(len(labels)):
            val = z_data[i][j]
            annotations.append(dict(
                x=labels[j],
                y=labels[i],
                text=f"<b style='font-size:15px;'>{val:,}</b>",
                font=dict(
                    color="#FFFFFF" if val > (max_val * 0.40) else "#0F172A",
                    family="Plus Jakarta Sans"
                ),
                showarrow=False
            ))

    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=labels,
        y=labels,
        colorscale=[
            [0.0, "#EFF6FF"],
            [0.3, "#93C5FD"],
            [0.7, "#3B82F6"],
            [1.0, "#1E40AF"]
        ],
        showscale=True,
        colorbar=dict(title=dict(text="Số lượng", font=dict(size=12)), thickness=14),
        hovertemplate="Thực tế: <b>%{y}</b><br>Dự đoán: <b>%{x}</b><br>Số lượng: <b>%{z:,}</b> mẫu<extra></extra>"
    ))

    fig.update_layout(
        font=GLOBAL_FONT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=dict(text=f"<b>{title}</b>", font=dict(size=15, color="#0F172A")),
        xaxis=dict(title="<b>Nhãn Dự đoán (Predicted Label)</b>", side="bottom"),
        yaxis=dict(title="<b>Nhãn Thực tế (True Label)</b>", autorange="reversed"),
        annotations=annotations,
        margin=dict(t=50, b=50, l=90, r=40),
        height=380
    )
    return fig

def plot_models_comparison_chart(phase4_metrics_df: pd.DataFrame) -> go.Figure:
    """
    Vẽ biểu đồ cột nhóm hiện đại so sánh Accuracy, Macro F1 và Neutral F1 của 8 thí nghiệm Phase 4.
    """
    df_val = phase4_metrics_df[phase4_metrics_df["Dataset"] == "Validation"].copy()
    if df_val.empty:
        df_val = phase4_metrics_df.copy()

    df_val = df_val.sort_values("Macro_F1", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_val["Experiment_ID"],
        x=df_val["Macro_F1"] * 100,
        name="Macro F1 (%)",
        orientation="h",
        marker=dict(color="#4F46E5", cornerradius=4),
        text=[f"{v*100:.1f}%" for v in df_val["Macro_F1"]],
        textposition="inside",
        insidetextanchor="middle"
    ))
    fig.add_trace(go.Bar(
        y=df_val["Experiment_ID"],
        x=df_val["Neu_F1"] * 100,
        name="Neutral F1 (%)",
        orientation="h",
        marker=dict(color="#F59E0B", cornerradius=4),
        text=[f"{v*100:.1f}%" for v in df_val["Neu_F1"]],
        textposition="inside",
        insidetextanchor="middle"
    ))
    fig.add_trace(go.Bar(
        y=df_val["Experiment_ID"],
        x=df_val["Accuracy"] * 100,
        name="Accuracy (%)",
        orientation="h",
        marker=dict(color="#10B981", cornerradius=4),
        text=[f"{v*100:.1f}%" for v in df_val["Accuracy"]],
        textposition="inside",
        insidetextanchor="middle"
    ))

    fig.update_layout(
        font=GLOBAL_FONT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        barmode="group",
        xaxis=dict(title="Tỷ lệ (%)", range=[0, 100], showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=-0.18, xanchor="center", x=0.5),
        margin=dict(t=30, b=60, l=120, r=30),
        height=480
    )
    return fig
