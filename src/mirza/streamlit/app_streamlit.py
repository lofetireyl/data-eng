import os
import math
import textwrap
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

st.set_page_config(page_title="Spotify DB – Explorer & Metrics", layout="wide")

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
def list_tables(schema="public"):
    with cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        return [r[0] for r in cur.fetchall()]

def df_from_query(sql, params=None):
    with cursor() as cur:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def ms_to_mmss(ms):
    if ms is None or math.isnan(ms):
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

# ---------- UI ----------
st.title("🎧 Spotify Database – Explorer & Metrics")

page = st.sidebar.radio("Navigate", ["Metrics", "DB Explorer"], index=0)

st.sidebar.markdown("### Connection")
st.sidebar.text(f"{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# =================== METRICS PAGE ===================
if page == "Metrics":
    st.header("Key Metrics (for presentation)")

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
        st.subheader("📈 All Artists by Popularity")
        max_bars = st.slider("Max artists to display (sorted by popularity)", 50, 2000, min(len(df_all), 300), step=50)
        df_show = df_all.head(max_bars)
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

# =================== DB EXPLORER PAGE ===================
else:
    st.header("Database Explorer")
    with st.sidebar:
        schema = st.text_input("Schema", value="public")
        tables = list_tables(schema) if schema else []
        table = st.selectbox("Table", options=tables) if tables else None
        limit = st.number_input("Preview rows (LIMIT)", 10, 10000, 200, step=10)
        st.markdown("---")
        st.markdown("### Custom SQL (SELECT-only)")
        default_sql = textwrap.dedent("""
            -- Example:
            -- SELECT track_id, popularity, duration_ms
            -- FROM spotify_track_meta
            -- ORDER BY popularity DESC
            -- LIMIT 50;
        """).strip()
        sql = st.text_area("Query", value=default_sql, height=150)
        run_sql = st.button("Run SQL")

    # Table preview
    if table:
        c1, c2 = st.columns([1,4])
        with c1:
            try:
                cnt = df_from_query(f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"').iloc[0]["n"]
                st.metric("Row count", f"{cnt:,}")
            except Exception as e:
                st.error(f"Count failed: {e}")
        with c2:
            try:
                df = df_from_query(f'SELECT * FROM "{schema}"."{table}" LIMIT %s', (limit,))
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Preview failed: {e}")

    # Custom SQL
    if run_sql and sql.strip():
        q = sql.strip().rstrip(";")
        if not q.lower().startswith("select"):
            st.warning("Only SELECT queries are allowed from the UI.")
        else:
            try:
                df = df_from_query(q)
                st.success("Query OK")
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Query failed: {e}")
