import os
import math
import pandas as pd
import streamlit as st
import psycopg2
import plotly.express as px
from contextlib import contextmanager

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASS = os.getenv("PG_PASSWORD", "spotify")

st.set_page_config(page_title="Spotify DB – Explorer & Metrics", layout="wide")

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

SQL_SONGBPM_BASE = """
  SELECT
    query_id,
    spotify_artist_id,
    spotify_artist_name,
    item_artist_name,
    item_id,
    title,
    tempo,              -- = BPM (numeric)
    musical_key,
    camelot,
    danceability,       -- 0–100
    acousticness        -- 0–100
  FROM v_artist_bpm_items_flat
"""
SQL_SONGBPM_DISTINCT_ARTISTS = """
  SELECT DISTINCT spotify_artist_name
  FROM v_artist_bpm_items_flat
  WHERE spotify_artist_name IS NOT NULL
  ORDER BY 1
"""
SQL_SONGBPM_DISTINCT_ITEM_ARTISTS = """
  SELECT DISTINCT item_artist_name
  FROM v_artist_bpm_items_flat
  WHERE item_artist_name IS NOT NULL
  ORDER BY 1
"""

# ---------- UI SHELL ----------
st.sidebar.markdown("### Connection")
st.sidebar.text(f"{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")

page = st.sidebar.radio("Navigate", ["Metrics", "DB Explorer", "SongBPM"], index=0)

if page == "Metrics":
    st.title("🎧 Spotify – Key Metrics")

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

    try:
        df_all = df_from_query(SQL_ALL_ARTISTS)
        st.subheader("📈 All Artists by Popularity")
        df_show = df_all.head(300)
        fig3 = px.bar(df_show.sort_values("popularity", ascending=False),
                      x="artist_name", y="popularity", title=None)
        fig3.update_layout(xaxis_title=None, yaxis_title="Popularity",
                           xaxis_tickangle=-60, margin=dict(t=10))
        st.plotly_chart(fig3, use_container_width=True)
        with st.expander("Show data"):
            st.dataframe(df_show, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"All artists plot failed: {e}")

    st.markdown("---")

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

    try:
        df_scatter = df_from_query(SQL_DURATION_VS_POPULARITY)
        if not df_scatter.empty:
            df_scatter["duration_min"] = df_scatter["duration_ms"] / 60000.0
            st.markdown("---")
            st.subheader("🎯 Popularity vs Track Length")
            fig5 = px.scatter(
                df_scatter,
                x="duration_min",
                y="popularity",
                render_mode="webgl",
                opacity=0.35,
                labels={"duration_min": "Duration (min)", "popularity": "Popularity"},
            )
            st.plotly_chart(fig5, use_container_width=True)
            with st.expander("Show data (first 1,000 rows)"):
                st.dataframe(df_scatter.head(1000), use_container_width=True, hide_index=True)
        else:
            st.info("No data available for duration vs popularity.")
    except Exception as e:
        st.error(f"Duration vs popularity plot failed: {e}")

elif page == "DB Explorer":
    st.title("🗄️ Database Explorer")
    schema = st.sidebar.text_input("Schema", value="public")
    try:
        tables = df_from_query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))["table_name"].tolist()
    except Exception:
        tables = []
    table = st.sidebar.selectbox("Table", options=tables) if tables else None
    limit = st.sidebar.number_input("Preview rows (LIMIT)", 10, 10000, 200, step=10)

    if table:
        c1, c2 = st.columns([1, 4])
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

else: 
    st.title("🎵 SongBPM – Analyse und Übersicht")

    try:
        all_artists = df_from_query(SQL_SONGBPM_DISTINCT_ARTISTS)["spotify_artist_name"].tolist()
    except Exception:
        all_artists = []
    try:
        all_item_artists = df_from_query(SQL_SONGBPM_DISTINCT_ITEM_ARTISTS)["item_artist_name"].tolist()
    except Exception:
        all_item_artists = []

    with st.sidebar:
        st.markdown("### Filter – SongBPM")
        sel_artists = st.multiselect("Spotify Artist(en)", options=all_artists, default=[])
        sel_item_artists = st.multiselect("Item Artist(en)", options=all_item_artists, default=[])
        bpm_min, bpm_max = st.slider("BPM Bereich", 40, 220, (60, 180), step=1)

    try:
        base_sql = SQL_SONGBPM_BASE
        where = []
        params = []

        if sel_artists:
            ph = ", ".join(["%s"] * len(sel_artists))
            where.append(f"spotify_artist_name IN ({ph})")
            params.extend(sel_artists)
        if sel_item_artists:
            ph = ", ".join(["%s"] * len(sel_item_artists))
            where.append(f"item_artist_name IN ({ph})")
            params.extend(sel_item_artists)

        where.append("tempo BETWEEN %s AND %s")
        params.extend([bpm_min, bpm_max])

        if where:
            base_sql += "\nWHERE " + " AND ".join(where)

        base_sql += "\nORDER BY spotify_artist_name, item_artist_name, title"

        bpm_df = df_from_query(base_sql, params).rename(columns={"tempo": "bpm"})
        for c in ["bpm", "danceability", "acousticness"]:
            bpm_df[c] = pd.to_numeric(bpm_df[c], errors="coerce")
    except Exception as e:
        bpm_df = pd.DataFrame()
        st.error(f"Konnte SongBPM-Daten nicht laden: {e}")

    if bpm_df.empty:
        st.info("Keine Daten gefunden (Filter anpassen?).")
        st.stop()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Songs", f"{len(bpm_df):,}")
    k2.metric("Ø BPM", f"{bpm_df['bpm'].mean():.1f}" if bpm_df["bpm"].notna().any() else "n/a")
    k3.metric("Ø Acousticness", f"{bpm_df['acousticness'].mean():.1f}" if bpm_df["acousticness"].notna().any() else "n/a")
    k4.metric("Ø Danceability", f"{bpm_df['danceability'].mean():.1f}" if bpm_df["danceability"].notna().any() else "n/a")

    st.caption(f"Filter aktiv – BPM {bpm_min}-{bpm_max}"
               + (f" | Spotify Artists: {len(sel_artists)}" if sel_artists else "")
               + (f" | Item Artists: {len(sel_item_artists)}" if sel_item_artists else ""))

    st.markdown("---")

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Verteilung der BPM")
        fig_hist = px.histogram(bpm_df, x="bpm", nbins=30, labels={"bpm": "BPM"})
        fig_hist.update_layout(yaxis_title="Songs", margin=dict(t=10))
        st.plotly_chart(fig_hist, use_container_width=True)

    with c2:
        st.subheader("BPM vs. Danceability")
        fig_sc = px.scatter(
            bpm_df,
            x="bpm",
            y="danceability",
            color="musical_key",
            hover_data=["spotify_artist_name", "item_artist_name", "title", "camelot"],
            labels={"bpm": "BPM", "danceability": "Danceability (0–100)", "musical_key": "Key"},
        )
        fig_sc.update_layout(margin=dict(t=10))
        st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    st.subheader("Detail-Daten")
    with st.expander("Tabelle anzeigen"):
        st.dataframe(
            bpm_df[[
                "spotify_artist_name", "item_artist_name", "title",
                "bpm", "musical_key", "camelot", "danceability", "acousticness"
            ]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

    csv = bpm_df.to_csv(index=False).encode("utf-8")
    st.download_button("CSV herunterladen", data=csv, file_name="songbpm_export.csv", mime="text/csv")