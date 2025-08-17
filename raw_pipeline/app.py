# app.py
# -------------------------------------------
# Real-time TikTok Harmful-Content Dashboard
# Author: OpenAI o3
# -------------------------------------------
import datetime as dt
import json
import random
import string

import pandas as pd
import pytz
import streamlit as st
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh
from wordcloud import WordCloud

# ---------- CONFIG ---------- #
st.set_page_config(page_title="TikTok Safety Monitor",
                   page_icon="📹",
                   layout="wide")

LABELS = ["Safe", "Harmful Content", "Adult Content", "Suicide"]
LABEL_COLORS = {  # Chart.js colour defaults ― dễ nhìn & nhất quán
    "Safe": "rgba(75, 192, 192, 0.6)",
    "Harmful Content": "rgba(255, 159, 64, 0.6)",
    "Adult Content": "rgba(255, 99, 132, 0.6)",
    "Suicide": "rgba(153, 102, 255, 0.6)",
}

# ---------- MONGODB ---------- #
@st.cache_resource(show_spinner=False)
def get_collection():
    client = MongoClient(st.secrets["MONGO_URI"])
    db = client[st.secrets["DATABASE"]]
    return db[st.secrets["COLLECTION"]]

col = get_collection()

# ---------- LOAD DATA ---------- #
@st.cache_data(ttl=30, show_spinner=False)
def load_data(hours: int = 168):
    """Fetch last <hours> of data for the dashboard (cached 30 s)."""
    since = dt.datetime.utcnow() - dt.timedelta(hours=hours)
    cursor = col.find({"processed_at": {"$gte": since}})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df
    # Ensure correct dtypes
    df["created_time"] = pd.to_datetime(df["created_time"])
    df["processed_at"] = pd.to_datetime(df["processed_at"]["$date"]
                                        if isinstance(df.iloc[0]["processed_at"], dict)
                                        else df["processed_at"])
    df["label"] = df["classification"].apply(lambda x: x.get("label") if isinstance(x, dict) else None)
    return df

df = load_data()

# Auto-refresh mỗi 60 s
st_autorefresh(interval=60_000, key="auto-refresh")

st.title("📹 TikTok Harmful-Content Safety Monitor")

if df.empty:
    st.warning("Chưa có dữ liệu! Hãy để hệ thống streaming gửi dữ liệu vào MongoDB.")
    st.stop()

# ========= 1. Duration distribution ========= #
st.subheader("⏱️ Phân bố thời lượng video theo nhãn")

def chart_bar(x, y, title, y_label="giây"):
    """Generate a unique Chart.js bar chart inside an HTML component."""
    rand = ''.join(random.choices(string.ascii_letters, k=6))
    chart_config = {
        "type": "bar",
        "data": {
            "labels": x,
            "datasets": [{
                "label": y_label,
                "data": y,
                "backgroundColor": [LABEL_COLORS.get(lbl, "rgba(201, 203, 207, 0.6)") for lbl in x],
            }]
        },
        "options": {
            "responsive": True,
            "plugins": {
                "title": {"display": True, "text": title}
            },
            "scales": {"y": {"beginAtZero": True}}
        }
    }
    html = f"""
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <canvas id="c{rand}" height="120"></canvas>
    <script>
      new Chart(document.getElementById('c{rand}').getContext('2d'), {json.dumps(chart_config)});
    </script>"""
    st.components.v1.html(html, height=320, scrolling=False)

avg_dur = df.groupby("label")["duration"].mean().reindex(LABELS).fillna(0).round(1)
chart_bar(avg_dur.index.tolist(),
          avg_dur.tolist(),
          "Thời lượng video trung bình (giây)")

# ========= 2. WordCloud helpers ========= #
def draw_wordcloud(text_series, label, max_words=100):
    wc = WordCloud(width=800, height=400,
                   background_color="white",
                   max_words=max_words,
                   colormap="Dark2").generate(" ".join(text_series.dropna().astype(str)))
    return wc.to_image()

# ========= 3. Wordclouds & Hashtags ========= #
st.subheader("📝 WordCloud theo nhãn")

tab_desc, tab_text, tab_hash = st.tabs(["Description", "OCR + Audio", "Hashtags"])

with tab_desc:
    cols = st.columns(len(LABELS))
    for i, lbl in enumerate(LABELS):
        with cols[i]:
            st.markdown(f"**{lbl}**")
            img = draw_wordcloud(df[df["label"] == lbl]["description"], lbl)
            st.image(img)

with tab_text:
    cols = st.columns(len(LABELS))
    for i, lbl in enumerate(LABELS):
        subset = df[df["label"] == lbl]
        combined_text = subset["ocr_text"].fillna("") + " " + subset["audio_text"].fillna("")
        with cols[i]:
            st.markdown(f"**{lbl}**")
            img = draw_wordcloud(combined_text, lbl)
            st.image(img)

with tab_hash:
    cols = st.columns(len(LABELS))
    for i, lbl in enumerate(LABELS):
        tags = df[df["label"] == lbl]["hashtags"].explode().dropna().astype(str)
        with cols[i]:
            st.markdown(f"**{lbl}**")
            img = draw_wordcloud(tags, lbl)
            st.image(img)

# ========= 4. Engagement metrics ========= #
st.subheader("📈 Chỉ số tương tác trung bình theo nhãn")

metrics = ["playcount", "diggcount", "commentcount", "sharecount"]
metric_tabs = st.tabs(metrics)

for m, tab in zip(metrics, metric_tabs):
    with tab:
        avg = df.groupby("label")[m].mean().reindex(LABELS).fillna(0).round(0)
        chart_bar(avg.index.tolist(),
                  avg.tolist(),
                  f"Trung bình {m} / video",
                  y_label=m)

# ========= 5. Time-series by label ========= #
st.subheader("⏳ Số lượng video theo thời gian (real-time)")

# Resample theo phút
df_time = (df[["processed_at", "label"]]
           .set_index("processed_at")
           .groupby("label")
           .resample("1min")
           .size()
           .unstack(level=0)
           .fillna(0)
           .cumsum())  # cộng dồn để thấy xu hướng

# Prepare data for Chart.js line chart
labels_time = df_time.index.strftime("%H:%M")
datasets = [{
    "label": lbl,
    "data": df_time[lbl].astype(int).tolist(),
    "borderColor": LABEL_COLORS.get(lbl, "rgba(0,0,0,0.5)"),
    "fill": False,
    "tension": 0.1
} for lbl in LABELS]

rand = ''.join(random.choices(string.ascii_letters, k=6))
line_config = {
    "type": "line",
    "data": {"labels": labels_time.tolist(), "datasets": datasets},
    "options": {
        "responsive": True,
        "plugins": {"title": {"display": True, "text": "Video count over time"}},
        "interaction": {"mode": "index", "intersect": False},
        "stacked": False,
        "scales": {"x": {"title": {"display": True, "text": "UTC Time"}},
                   "y": {"beginAtZero": True, "title": {"display": True, "text": "Videos"}}}
    }
}
html_line = f"""
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<canvas id="l{rand}" height="120"></canvas>
<script>
  new Chart(document.getElementById('l{rand}').getContext('2d'), {json.dumps(line_config)});
</script>"""
st.components.v1.html(html_line, height=400, scrolling=False)

# ---------- FOOTER ---------- #
st.caption("Dashboard auto-refresh mỗi 30 s • Dữ liệu hiển thị từ 24 h gần nhất • Bản quyền © 2025")
