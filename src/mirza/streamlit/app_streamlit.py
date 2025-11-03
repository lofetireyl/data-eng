import os
import math
import pandas as pd
import streamlit as st
import psycopg2
import plotly.express as px
from contextlib import contextmanager

# ---------- DB CONFIG ----------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASS = os.getenv("PG_PASSWORD", "spotify")

st.set_page_config(page_title="Spotify Metrics", layout="wide")

# ---------- CONNECTION ----------
@st.cache_resource(show_spinner=False)
def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

@contextmanager
def cursor():
    conn = get_conn()
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()

# ---------- GENERIC HELPERS ----------
def df_from_query(sql, params=None):
    with cursor() as cur:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def ms_to_mmss(ms):
    if ms is None or (isinstance(ms, float) and math.isnan(ms)):
        return "00:00"
    s = int(round(ms / 1000.0))
    m, s = divmod(s, 60)
    return f"{m:02d}:{s:02d}"

# ---------- QUERIES FOR METRICS ----------
SQL_AVG_DURATION = """
    SELECT AVG(duration_ms)::float AS avg_duration_ms
    FROM spotify_track_meta
    WHERE duration_ms IS NOT NULL
"""

SQL_CORR_DURATION_POP = """
    SELECT corr(duration_ms::float, popularity::float) AS corr_dp
    FROM spotify_track_meta
    WHERE duration_ms IS NOT NULL AND popularity IS NOT NULL
"""

SQL_TOP_ARTISTS = """
    SELECT artist_name, popularity
    FROM spotify_artist_meta
    WHERE artist_name IS NOT NULL AND popularity IS NOT NULL
    ORDER BY popularity DESC, artist_name ASC
    LIMIT 10
"""

SQL_BOTTOM_ARTISTS = """
    SELECT artist_name, popularity
    FROM spotify_artist_meta
    WHERE artist_name IS NOT NULL AND popularity IS NOT NULL
    ORDER BY popularity ASC, artist_name ASC
    LIMIT 10
"""

SQL_ALL_ARTISTS = """
    SELECT artist_name, popularity
    FROM spotify_artist_meta
    WHERE artist_name IS NOT NULL AND popularity IS NOT NULL
    ORDER BY popularity DESC
"""

SQL_AVG_TRACKS_PER_ALBUM = """
    SELECT AVG(total_tracks)::float AS avg_tracks
    FROM spotify_album
    WHERE total_tracks IS NOT NULL AND total_tracks > 0
"""

SQL_TRACKS_PER_ALBUM_DIST = """
    SELECT COALESCE(total_tracks, 0) AS total_tracks, COUNT(*) AS albums
    FROM spotify_album
    GROUP BY total_tracks
    ORDER BY total_tracks
"""
SQL_DURATION_VS_POPULARITY = """
    SELECT duration_ms, popularity
    FROM spotify_track_meta
    WHERE duration_ms IS NOT NULL AND popularity IS NOT NULL
"""

# ---------- UI ----------
st.title("Spotify Database")

st.sidebar.markdown("### Connection")
st.sidebar.text(f"{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# =================== METRICS PAGE ===================
st.header("Key Metrics")

# --- KPI row ---
c1, c2, c3 = st.columns(3)
try:
    avg_ms = df_from_query(SQL_AVG_DURATION).iloc[0]["avg_duration_ms"] or 0.0
    with c1:
        st.metric("Avg Track Duration", ms_to_mmss(avg_ms))
except Exception as e:
    with c1:
        st.error(f"Avg duration failed: {e}")

try:
    corr_val = df_from_query(SQL_CORR_DURATION_POP).iloc[0]["corr_dp"]
    corr_str = "n/a" if corr_val is None else f"{corr_val:.3f}"
    with c2:
        st.metric("Correlation (duration ↔ popularity)", corr_str)
except Exception as e:
    with c2:
        st.error(f"Correlation failed: {e}")

try:
    avg_tracks = df_from_query(SQL_AVG_TRACKS_PER_ALBUM).iloc[0]["avg_tracks"]
    with c3:
        st.metric("Avg Tracks per Album", f"{(avg_tracks or 0):.2f}")
except Exception as e:
    with c3:
        st.error(f"Avg tracks/album failed: {e}")

st.markdown("---")

# --- Top/Bottom 10 artists by popularity ---
colA, colB = st.columns(2)
try:
    top10 = df_from_query(SQL_TOP_ARTISTS)
    with colA:
        st.subheader("🏆 Top 10 Artists by Popularity")
        fig = px.bar(top10, x="artist_name", y="popularity", title=None)
        fig.update_layout(xaxis_title=None, yaxis_title="Popularity", xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top10, use_container_width=True, hide_index=True)
except Exception as e:
    with colA:
        st.error(f"Top artists failed: {e}")

try:
    bottom10 = df_from_query(SQL_BOTTOM_ARTISTS)
    with colB:
        st.subheader("⬇ Bottom 10 Artists by Popularity")
        fig2 = px.bar(bottom10, x="artist_name", y="popularity", title=None)
        fig2.update_layout(xaxis_title=None, yaxis_title="Popularity", xaxis_tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(bottom10, use_container_width=True, hide_index=True)
except Exception as e:
    with colB:
        st.error(f"Bottom artists failed: {e}")

st.markdown("---")

# --- All artists popularity plot ---
try:
    df_all = df_from_query(SQL_ALL_ARTISTS)
    st.subheader("📈 Artists by Popularity")
    df_show = df_all.head(300)  # show top 300 artists
    fig3 = px.bar(df_show.sort_values("popularity", ascending=False),
                  x="artist_name", y="popularity", title=None)
    fig3.update_layout(xaxis_title=None, yaxis_title="Popularity", xaxis_tickangle=-60, margin=dict(t=10))
    st.plotly_chart(fig3, use_container_width=True)
    with st.expander("Show data"):
        st.dataframe(df_show, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"All artists plot failed: {e}")

st.markdown("---")

# --- Tracks per album distribution ---
try:
    dist = df_from_query(SQL_TRACKS_PER_ALBUM_DIST)
    st.subheader("🎚️ Distribution: Tracks per Album")
    fig4 = px.bar(dist, x="total_tracks", y="albums", title=None)
    fig4.update_layout(xaxis_title="Total tracks on album", yaxis_title="# albums")
    st.plotly_chart(fig4, use_container_width=True)
    with st.expander("Show data"):
        st.dataframe(dist, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Tracks/album distribution failed: {e}")

# --- Popularity vs Track Length (scatter) ---
try:
    df_scatter = df_from_query(SQL_DURATION_VS_POPULARITY)
    if not df_scatter.empty:
        # minutes for x-axis; keep ms for hover
        df_scatter["duration_min"] = df_scatter["duration_ms"] / 60000.0

        st.markdown("---")
        st.subheader("🎯 Popularity vs Track Length")

        fig5 = px.scatter(
            df_scatter,
            x="duration_min",
            y="popularity",
            render_mode="webgl",     # faster with many points
            opacity=0.35,
            labels={"duration_min": "Duration (min)", "popularity": "Popularity"}
        )
        st.plotly_chart(fig5, use_container_width=True)

        with st.expander("Show data (first 1,000 rows)"):
            st.dataframe(df_scatter.head(1000), use_container_width=True, hide_index=True)
    else:
        st.info("No data available for duration vs popularity.")
except Exception as e:
    st.error(f"Duration vs popularity plot failed: {e}")

