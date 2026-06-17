import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pipeline:pipeline@localhost:5432/datasync",
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

st.set_page_config(
    page_title="Data Sync Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* ── Global ── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
    }
    section[data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] p { color: #94a3b8 !important; }

    /* ── Header banner ── */
    .hero {
        background: linear-gradient(135deg, #0ea5e9 0%, #6366f1 50%, #8b5cf6 100%);
        border-radius: 16px;
        padding: 28px 36px;
        margin-bottom: 28px;
        display: flex;
        align-items: center;
        gap: 18px;
    }
    .hero h1 { color: white; font-size: 1.9rem; font-weight: 700; margin: 0; }
    .hero p  { color: rgba(255,255,255,.75); font-size: .9rem; margin: 4px 0 0; }

    /* ── KPI cards ── */
    [data-testid="metric-container"] {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 18px 22px !important;
        box-shadow: 0 1px 4px rgba(0,0,0,.05);
    }
    [data-testid="metric-container"] label { font-size: .75rem !important; color: #64748b !important; text-transform: uppercase; letter-spacing: .06em; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 1.9rem !important; font-weight: 700 !important; color: #0f172a !important; }

    /* ── Section cards ── */
    .card {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 1px 4px rgba(0,0,0,.04);
    }
    .card h3 { margin: 0 0 16px; font-size: 1rem; font-weight: 600; color: #1e293b; }

    /* ── Status badges ── */
    .badge-success { background:#dcfce7; color:#166534; padding:3px 10px; border-radius:99px; font-size:.75rem; font-weight:600; }
    .badge-error   { background:#fee2e2; color:#991b1b; padding:3px 10px; border-radius:99px; font-size:.75rem; font-weight:600; }
    .badge-running { background:#fef9c3; color:#854d0e; padding:3px 10px; border-radius:99px; font-size:.75rem; font-weight:600; }

    /* ── Divider ── */
    hr { border-color: #e2e8f0 !important; }

    /* ── Streamlit overrides ── */
    .block-container { padding-top: 1.5rem !important; }
    div[data-testid="stHorizontalBlock"] > div { gap: 12px; }
    .stDataFrame { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


def query(sql: str) -> list:
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Data Sync Pipeline")
    st.markdown("---")
    page = st.selectbox(
        "Navigation",
        ["Overview", "Run History", "Webhook Events", "Raw Records"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    if st.button("↺  Refresh now", use_container_width=True):
        st.rerun()
    st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")
    st.markdown("---")
    st.caption("Built with FastAPI · Prefect · PostgreSQL · Streamlit")

st.markdown('<meta http-equiv="refresh" content="30">', unsafe_allow_html=True)

# ── Hero banner ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div>
        <h1>⚡ Data Sync Pipeline</h1>
        <p>Real-time monitoring · Auto-refresh every 30 s · Multi-source ETL</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as exc:
    st.error(f"Cannot connect to the database: {exc}")
    st.info("Make sure PostgreSQL is running and DATABASE_URL is set correctly.")
    st.stop()

# ── Load KPIs ─────────────────────────────────────────────────────────────────
try:
    total_records = query("SELECT COUNT(*) FROM synced_records")[0][0]
    num_sources   = query("SELECT COUNT(DISTINCT source_name) FROM synced_records")[0][0]
    last_sync_row = query("SELECT MAX(synced_at) FROM synced_records")[0][0]
    errors        = query("SELECT COUNT(*) FROM sync_runs WHERE status = 'error'")[0][0]
    successes     = query("SELECT COUNT(*) FROM sync_runs WHERE status = 'success'")[0][0]
    total_runs    = successes + errors
    success_rate  = round(successes / total_runs * 100) if total_runs else 0
except Exception as exc:
    st.warning(f"Tables not initialised yet — run the pipeline first. ({exc})")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "Overview":

    # KPI row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Records", f"{total_records:,}")
    c2.metric("Active Sources", num_sources)
    c3.metric("Successful Runs", successes)
    c4.metric("Failed Runs", errors, delta=f"-{errors}" if errors else None,
              delta_color="inverse")
    c5.metric("Success Rate", f"{success_rate} %",
              delta="↑ good" if success_rate >= 80 else "↓ check errors")

    st.markdown("<br>", unsafe_allow_html=True)
    col_l, col_r = st.columns(2)

    # Records by source — donut chart
    with col_l:
        st.markdown('<div class="card"><h3>📊 Records by Source</h3>', unsafe_allow_html=True)
        rows = query("""
            SELECT source_name, source_type, COUNT(*) AS cnt
            FROM synced_records
            GROUP BY source_name, source_type
            ORDER BY cnt DESC
        """)
        if rows:
            df = pd.DataFrame(rows, columns=["Source", "Type", "Count"])
            fig = px.pie(
                df, names="Source", values="Count", hole=0.55,
                color_discrete_sequence=px.colors.sequential.Blues_r,
            )
            fig.update_traces(textposition="outside", textinfo="percent+label")
            fig.update_layout(
                margin=dict(t=20, b=20, l=20, r=20),
                showlegend=False,
                height=300,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No records yet — run the pipeline.")
        st.markdown("</div>", unsafe_allow_html=True)

    # Success vs Error — bar chart
    with col_r:
        st.markdown('<div class="card"><h3>📈 Runs Over Time</h3>', unsafe_allow_html=True)
        rows = query("""
            SELECT DATE(started_at) AS day, status, COUNT(*) AS cnt
            FROM sync_runs
            GROUP BY day, status
            ORDER BY day
        """)
        if rows:
            df = pd.DataFrame(rows, columns=["Day", "Status", "Count"])
            fig = px.bar(
                df, x="Day", y="Count", color="Status", barmode="group",
                color_discrete_map={"success": "#22c55e", "error": "#ef4444", "running": "#f59e0b"},
            )
            fig.update_layout(
                margin=dict(t=20, b=20, l=20, r=20),
                height=300,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sync runs recorded yet.")
        st.markdown("</div>", unsafe_allow_html=True)

    # Last sync timeline
    st.markdown('<div class="card"><h3>🕐 Last Sync Activity</h3>', unsafe_allow_html=True)
    rows = query("""
        SELECT source_name, source_type, status,
               records_extracted, records_loaded,
               started_at, finished_at
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT 8
    """)
    if rows:
        df = pd.DataFrame(rows, columns=[
            "Source", "Type", "Status", "Extracted", "Loaded", "Started", "Finished",
        ])

        def badge(s):
            cls = {"success": "badge-success", "error": "badge-error"}.get(s, "badge-running")
            return f'<span class="{cls}">{s}</span>'

        df["Status"] = df["Status"].apply(badge)
        st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No sync runs recorded yet.")
    st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: RUN HISTORY
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Run History":
    st.markdown("### Run History")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Runs", total_runs)
    col2.metric("Successes", successes)
    col3.metric("Errors", errors)

    st.markdown("<br>", unsafe_allow_html=True)

    rows = query("""
        SELECT source_name, source_type, status,
               records_extracted, records_loaded,
               started_at, error_message
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT 50
    """)
    if rows:
        df = pd.DataFrame(rows, columns=[
            "Source", "Type", "Status", "Extracted", "Loaded", "Started", "Error",
        ])
        status_filter = st.multiselect(
            "Filter by status", options=df["Status"].unique().tolist(),
            default=df["Status"].unique().tolist(),
        )
        df = df[df["Status"].isin(status_filter)]
        st.dataframe(
            df.style.applymap(
                lambda v: "color: #16a34a; font-weight:600" if v == "success"
                else ("color: #dc2626; font-weight:600" if v == "error" else ""),
                subset=["Status"],
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No sync runs recorded yet.")

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: WEBHOOK EVENTS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Webhook Events":
    st.markdown("### Webhook Events")

    rows = query("""
        SELECT id, source_name, status, received_at, processed_at
        FROM webhook_events
        ORDER BY received_at DESC
        LIMIT 50
    """)
    if rows:
        df = pd.DataFrame(rows, columns=["ID", "Source", "Status", "Received At", "Processed At"])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No webhook events received yet.")
        st.markdown("**Send a test event:**")
        st.code(
            'curl -X POST http://<your-host>:8000/webhook/my-source \\\n'
            '  -H "Content-Type: application/json" \\\n'
            '  -d \'{"title": "test event", "value": 42}\'',
            language="bash",
        )

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: RAW RECORDS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Raw Records":
    st.markdown("### Synced Records")

    col1, col2 = st.columns([3, 1])
    with col2:
        limit = st.selectbox("Show last", [50, 100, 250, 500], index=1)

    rows = query(f"""
        SELECT id, source_type, source_name, external_id,
               title, author, category, value, synced_at
        FROM synced_records
        ORDER BY synced_at DESC
        LIMIT {limit}
    """)
    if rows:
        df = pd.DataFrame(rows, columns=[
            "ID", "Type", "Source", "Ext ID", "Title", "Author", "Category", "Value", "Synced At",
        ])
        source_filter = st.multiselect(
            "Filter by source", options=df["Source"].unique().tolist(),
            default=df["Source"].unique().tolist(),
        )
        df = df[df["Source"].isin(source_filter)]
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"{len(df):,} records displayed")
    else:
        st.info("No records synced yet.")
