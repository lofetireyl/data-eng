
import streamlit as st
import pandas as pd
import altair as alt
import os
from io import BytesIO

st.set_page_config(page_title="New Releases Explorer", layout="wide")

st.title("🎧 New Releases Explorer")

@st.cache_data
def load_data(file, sheet_name=None):
    if isinstance(file, (str, bytes)):
        xls = pd.ExcelFile(file)
        sheet = sheet_name or xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet)
    else:
        xls = pd.ExcelFile(file)
        sheet = sheet_name or xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet)

    # Clean/prepare
    if "album_release_date" in df.columns:
        df["album_release_date"] = pd.to_datetime(df["album_release_date"], errors="coerce")
    if "track_duration_ms" in df.columns and "track_duration_min" not in df.columns:
        df["track_duration_min"] = df["track_duration_ms"] / 60000.0
    return df

# --- Sidebar I/O ---
st.sidebar.header("Data")
uploaded = st.sidebar.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
default_path = os.environ.get("DEFAULT_XLSX", "new_releases_2025-10-07.xlsx")

sheet_name = None
if uploaded is not None:
    try:
        xls_tmp = pd.ExcelFile(uploaded)
        sheet_name = st.sidebar.selectbox("Sheet", xls_tmp.sheet_names, index=0)
    except Exception as e:
        st.sidebar.error(f"Error reading sheets: {e}")
        sheet_name = None

# Load data
try:
    if uploaded is not None:
        df = load_data(uploaded, sheet_name=sheet_name)
    else:
        df = load_data(default_path, sheet_name=sheet_name)
except Exception as e:
    st.error(f"Could not load data: {e}")
    st.stop()

# --- Sidebar filters ---
st.sidebar.header("Filters")
text_query = st.sidebar.text_input("Search (track/album/artist)")

artists = sorted(df["album_artists"].dropna().unique().tolist()) if "album_artists" in df.columns else []
sel_artists = st.sidebar.multiselect("Artists", artists, default=[])

# Popularity filter if present
if "track_popularity" in df.columns:
    min_pop, max_pop = int(df["track_popularity"].min()), int(df["track_popularity"].max())
    sel_pop = st.sidebar.slider("Track popularity range", min_pop, max_pop, (min_pop, max_pop))
else:
    sel_pop = None

# Date range filter if present
if "album_release_date" in df.columns:
    min_date = pd.to_datetime(df["album_release_date"]).min()
    max_date = pd.to_datetime(df["album_release_date"]).max()
    sel_dates = st.sidebar.date_input("Album release date range", value=(min_date, max_date))
else:
    sel_dates = None

# Apply filters
flt = df.copy()

if text_query:
    q = text_query.lower()
    cols = [c for c in ["track_name", "album_name", "album_artists"] if c in flt.columns]
    if cols:
        mask = False
        for c in cols:
            mask = mask | flt[c].astype(str).str.lower().str.contains(q, na=False)
        flt = flt[mask]

if sel_artists:
    if "album_artists" in flt.columns:
        flt = flt[flt["album_artists"].isin(sel_artists)]

if sel_pop is not None:
    lo, hi = sel_pop
    flt = flt[(flt["track_popularity"] >= lo) & (flt["track_popularity"] <= hi)]

if sel_dates is not None and isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
    start, end = pd.to_datetime(sel_dates[0]), pd.to_datetime(sel_dates[1])
    flt = flt[(flt["album_release_date"] >= start) & (flt["album_release_date"] <= end)]

# --- KPIs ---
kpi_cols = st.columns(4)
with kpi_cols[0]:
    st.metric("Artists", flt["album_artists"].nunique() if "album_artists" in flt.columns else 0)
with kpi_cols[1]:
    st.metric("Albums", flt["album_name"].nunique() if "album_name" in flt.columns else 0)
with kpi_cols[2]:
    st.metric("Tracks", len(flt))
with kpi_cols[3]:
    if "track_popularity" in flt.columns and len(flt):
        st.metric("Avg Popularity", f"{flt['track_popularity'].mean():.1f}")
    else:
        st.metric("Avg Popularity", "—")

st.markdown("---")

# --- Charts ---
chart_tab, table_tab = st.tabs(["📊 Charts", "📄 Table"])

with chart_tab:
    ccols = st.columns(2)

    # Avg popularity by artist
    if "album_artists" in flt.columns and "track_popularity" in flt.columns:
        by_artist = (
            flt.groupby("album_artists")["track_popularity"]
            .mean()
            .reset_index()
            .sort_values("track_popularity", ascending=False)
        )
        top_n = st.slider("Top N artists (by avg popularity)", 5, min(25, len(by_artist)) if len(by_artist) else 5, min(10, len(by_artist)) if len(by_artist) else 5)
        chart1 = (
            alt.Chart(by_artist.head(top_n))
            .mark_bar()
            .encode(
                x=alt.X("track_popularity:Q", title="Avg popularity"),
                y=alt.Y("album_artists:N", sort="-x", title="Artist"),
                tooltip=["album_artists", alt.Tooltip("track_popularity:Q", format=".1f")]
            )
            .properties(height=400)
        )
        ccols[0].subheader("Avg popularity by artist")
        ccols[0].altair_chart(chart1, use_container_width=True)

    # Top tracks by popularity
    if "track_name" in flt.columns and "track_popularity" in flt.columns:
        top_tracks = flt.sort_values("track_popularity", ascending=False).head(20)
        chart2 = (
            alt.Chart(top_tracks)
            .mark_bar()
            .encode(
                x=alt.X("track_popularity:Q", title="Popularity"),
                y=alt.Y("track_name:N", sort="-x", title="Track"),
                tooltip=["track_name", "album_name", "album_artists", "track_popularity"]
            )
            .properties(height=500)
        )
        ccols[1].subheader("Top tracks by popularity")
        ccols[1].altair_chart(chart2, use_container_width=True)

    # Duration vs popularity scatter
    if "track_duration_min" in flt.columns and "track_popularity" in flt.columns:
        scatter = (
            alt.Chart(flt)
            .mark_circle(size=60, opacity=0.6)
            .encode(
                x=alt.X("track_duration_min:Q", title="Track duration (min)"),
                y=alt.Y("track_popularity:Q", title="Popularity"),
                tooltip=["track_name", "album_name", "album_artists", alt.Tooltip("track_duration_min:Q", format=".2f"), "track_popularity"]
            )
            .interactive()
            .properties(height=450)
        )
        st.subheader("Duration vs popularity")
        st.altair_chart(scatter, use_container_width=True)

with table_tab:
    st.dataframe(flt, use_container_width=True, height=600)

    # Download filtered data
    csv = flt.to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered CSV", csv, file_name="filtered_new_releases.csv", mime="text/csv")

st.caption("Tip: Use the sidebar to upload a different Excel or narrow down by artist, date, and popularity.")
