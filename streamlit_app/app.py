#Movement & Turnover

#Databricks App - HR Analytics
#Source views: v_fact_movement_hranalytics, v_dim_org_structure_hranalytics

#Deploy in Databricks Apps. Requires:
#  - databricks-sdk
#  - streamlit
#  - plotly
#  - pandas

#Connection uses the workspaces built-in OAuth - no API key needed.
#Set DATABRICKS_WAREHOUSE_ID in app.yaml or Databricks App environment variables.

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from databricks import sql as dbsql
from databricks.sdk.config import Config

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Movement & Turnover",
    page_icon="👥",
    layout="wide",
)

# ── Theme colours (aligned to your dashboard palette) ─────────────────────────
COLOR_HIRE        = "#22c55e"   # green
COLOR_TERMINATION = "#ef4444"   # red
COLOR_INTERNAL    = "#3b82f6"   # blue
COLOR_OTHER       = "#94a3b8"   # slate

MOVEMENT_COLORS = {
    "hire":            COLOR_HIRE,
    "termination":     COLOR_TERMINATION,
    "job_change":      "#8b5cf6",
    "grade_change":    "#f59e0b",
    "org_change":      COLOR_INTERNAL,
    "location_change": "#06b6d4",
    "other":           COLOR_OTHER,
}

# ── Catalog / schema - edit here or set as env vars ───────────────────────────
CATALOG = os.getenv("HRA_CATALOG", "dd_hra1")
SCHEMA  = os.getenv("HRA_SCHEMA",  "datalake_curated2_hranalytics")
WH_ID   = os.getenv("DATABRICKS_WAREHOUSE_ID", "")  # set in Databricks App config

FACT_VIEW    = f"{CATALOG}.{SCHEMA}.v_fact_movement_hranalytics"
ORG_VIEW     = f"{CATALOG}.{SCHEMA}.v_dim_org_structure_hranalytics"


# ── Connection helper (cached for the session) ────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_connection():
    cfg = Config()
    return dbsql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WH_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=columns)


# ── SQL QUERIES ────────────────────────────────────────────────────────────────

SQL_DATE_RANGE = f"""
SELECT
    MIN(movement_date) AS min_date,
    MAX(movement_date) AS max_date
FROM {FACT_VIEW}
"""

SQL_L2_OPTIONS = f"""
SELECT DISTINCT l2_org_unit
FROM {ORG_VIEW}
WHERE l2_org_unit IS NOT NULL
ORDER BY l2_org_unit
"""

SQL_TREND = """
SELECT
    DATE_TRUNC('{period}', f.movement_date)            AS period_start,
    f.movement_type,
    COUNT(*)                                            AS events
FROM {fact} f
{org_join}
WHERE f.movement_date BETWEEN '{date_from}' AND '{date_to}'
  {l2_filter}
  {type_filter}
GROUP BY 1, 2
ORDER BY 1, 2
"""

SQL_KPI = """
SELECT
    SUM(CASE WHEN f.is_hire        = true THEN 1 ELSE 0 END) AS hires,
    SUM(CASE WHEN f.is_termination = true THEN 1 ELSE 0 END) AS terminations,
    SUM(CASE WHEN f.is_hire = false
              AND f.is_termination = false THEN 1 ELSE 0 END) AS internal_moves,
    COUNT(*)                                                   AS total_events
FROM {fact} f
{org_join}
WHERE f.movement_date BETWEEN '{date_from}' AND '{date_to}'
  {l2_filter}
"""

SQL_BY_CATEGORY = """
SELECT
    f.movement_category,
    COUNT(*) AS events
FROM {fact} f
{org_join}
WHERE f.movement_date BETWEEN '{date_from}' AND '{date_to}'
  {l2_filter}
  {type_filter}
GROUP BY 1
ORDER BY 2 DESC
"""

SQL_BY_ORG = """
SELECT
    os.l3_org_unit                                       AS org_unit,
    SUM(CASE WHEN f.is_hire        = true THEN 1 ELSE 0 END) AS hires,
    SUM(CASE WHEN f.is_termination = true THEN 1 ELSE 0 END) AS terminations,
    COUNT(*)                                             AS total
FROM {fact} f
JOIN {org} os
    ON os.hierarchy_sk = f.current_hierarchy_sk
WHERE f.movement_date BETWEEN '{date_from}' AND '{date_to}'
  {l2_filter}
GROUP BY 1
ORDER BY total DESC
LIMIT 20
"""

SQL_DETAIL = """
SELECT
    f.movement_date,
    f.movement_category,
    f.movement_direction,
    f.internal_move_type,
    f.movement_type,
    os.l2_org_unit,
    os.l3_org_unit,
    os.department_name,
    f.is_hire,
    f.is_termination,
    f.is_grade_change,
    f.is_org_change,
    f.promotion_velocity_days
FROM {fact} f
LEFT JOIN {org} os
    ON os.hierarchy_sk = f.current_hierarchy_sk
WHERE f.movement_date BETWEEN '{date_from}' AND '{date_to}'
  {l2_filter}
  {type_filter}
ORDER BY f.movement_date DESC
LIMIT 2000
"""


# ── SQL builder helpers ────────────────────────────────────────────────────────

def build_org_join(use_org: bool) -> str:
    if not use_org:
        return ""
    return f"JOIN {ORG_VIEW} os ON os.hierarchy_sk = f.current_hierarchy_sk"


def build_l2_filter(l2_values: list, use_org: bool) -> str:
    if not l2_values or not use_org:
        return ""
    escaped = ", ".join(f"'{v}'" for v in l2_values)
    return f"AND os.l2_org_unit IN ({escaped})"


def build_type_filter(movement_types: list) -> str:
    if not movement_types:
        return ""
    escaped = ", ".join(f"'{v}'" for v in movement_types)
    return f"AND f.movement_type IN ({escaped})"


def fmt_sql(template: str, **kwargs) -> str:
    return template.format(
        fact=FACT_VIEW,
        org=ORG_VIEW,
        **kwargs,
    )

# ── Sidebar - filters ─────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## Filters")

    # Date range - load min/max once
    @st.cache_data(ttl=3600, show_spinner=False)
    def load_date_range():
        return query(SQL_DATE_RANGE)

    with st.spinner("Loading date range…"):
        dr = load_date_range()
    min_date = pd.to_datetime(dr["min_date"].iloc[0]).date()
    max_date = pd.to_datetime(dr["max_date"].iloc[0]).date()

    default_from = max(min_date, pd.Timestamp(max_date) - pd.DateOffset(years=2))
    date_from, date_to = st.date_input(
        "Date range",
        value=(default_from, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Granularity
    period = st.selectbox("Trend granularity", ["month", "quarter", "year"], index=0)

    # Business area / L2 filter
    @st.cache_data(ttl=3600, show_spinner=False)
    def load_l2():
        return query(SQL_L2_OPTIONS)

    with st.spinner("Loading org levels…"):
        l2_df = load_l2()
    l2_options = l2_df["l2_org_unit"].dropna().tolist()

    selected_l2 = st.multiselect(
        "Business area (L2)",
        options=l2_options,
        default=[],
        placeholder="All business areas",
    )

    # Movement type filter
    all_types = ["hire", "termination", "job_change", "grade_change", "org_change", "location_change", "other"]
    selected_types = st.multiselect(
        "Movement type",
        options=all_types,
        default=[],
        placeholder="All movement types",
    )

    st.divider()
    st.caption(f"Data: `{CATALOG}.{SCHEMA}`")
    refresh = st.button("Refresh data", use_container_width=True)

# ── Resolve SQL parts ─────────────────────────────────────────────────────────

use_org      = bool(selected_l2)
org_join     = build_org_join(use_org)
l2_filter    = build_l2_filter(selected_l2, use_org)
type_filter  = build_type_filter(selected_types)
date_from_s  = str(date_from)
date_to_s    = str(date_to)

common = dict(
    org_join=org_join,
    l2_filter=l2_filter,
    type_filter=type_filter,
    date_from=date_from_s,
    date_to=date_to_s,
)

# ── Load all data ─────────────────────────────────────────────────────────────

cache_key = (date_from_s, date_to_s, period, tuple(selected_l2), tuple(selected_types), refresh)

@st.cache_data(ttl=1800, show_spinner=False)
def load_kpi(_key, **kw):
    return query(fmt_sql(SQL_KPI, **kw))

@st.cache_data(ttl=1800, show_spinner=False)
def load_trend(_key, **kw):
    return query(fmt_sql(SQL_TREND, period=period, **kw))

@st.cache_data(ttl=1800, show_spinner=False)
def load_category(_key, **kw):
    return query(fmt_sql(SQL_BY_CATEGORY, **kw))

@st.cache_data(ttl=1800, show_spinner=False)
def load_by_org(_key, **kw):
    use_org_local = bool(selected_l2)
    lf = build_l2_filter(selected_l2, use_org_local)
    oj = build_org_join(True)   # always join for this query
    return query(fmt_sql(SQL_BY_ORG, org_join=oj, l2_filter=lf,
                         date_from=kw["date_from"], date_to=kw["date_to"],
                         type_filter=kw["type_filter"]))

@st.cache_data(ttl=1800, show_spinner=False)
def load_detail(_key, **kw):
    return query(fmt_sql(SQL_DETAIL, **kw))

with st.spinner("Loading data…"):
    kpi_df      = load_kpi(cache_key, **common)
    trend_df    = load_trend(cache_key, **common)
    cat_df      = load_category(cache_key, **common)
    org_df      = load_by_org(cache_key, **common)
    detail_df   = load_detail(cache_key, **common)


# ── Header ────────────────────────────────────────────────────────────────────

st.title("Movement & Turnover Monitor")
st.caption(
    f"Period: **{date_from}** → **{date_to}**"
    + (f"  ·  Business area: **{', '.join(selected_l2)}**" if selected_l2 else "")
    + (f"  ·  Type: **{', '.join(selected_types)}**" if selected_types else "")
)

# ── KPI row ───────────────────────────────────────────────────────────────────

kpi = kpi_df.iloc[0]
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total events",     f"{int(kpi['total_events']):,}")
col2.metric("Hires",            f"{int(kpi['hires']):,}")
col3.metric("Terminations",     f"{int(kpi['terminations']):,}")
col4.metric("Internal moves",   f"{int(kpi['internal_moves']):,}")

st.divider()

# ── Row 1: trend chart + category donut ───────────────────────────────────────

left, right = st.columns([3, 2])

with left:
    st.subheader("Events over time")

    if trend_df.empty:
        st.info("No data for the selected filters.")
    else:
        trend_df["period_start"] = pd.to_datetime(trend_df["period_start"])

        # Pivot to stacked bar
        pivot = (
            trend_df
            .pivot_table(index="period_start", columns="movement_type", values="events", aggfunc="sum", fill_value=0)
            .reset_index()
        )

        fig_trend = go.Figure()
        for mtype in trend_df["movement_type"].unique():
            if mtype in pivot.columns:
                fig_trend.add_trace(go.Bar(
                    x=pivot["period_start"],
                    y=pivot[mtype],
                    name=mtype.replace("_", " ").title(),
                    marker_color=MOVEMENT_COLORS.get(mtype, COLOR_OTHER),
                ))

        fig_trend.update_layout(
            barmode="stack",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(t=30, b=10, l=0, r=0),
            height=340,
            xaxis_title=None,
            yaxis_title="Events",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        fig_trend.update_xaxes(showgrid=False)
        fig_trend.update_yaxes(gridcolor="rgba(128,128,128,0.15)")
        st.plotly_chart(fig_trend, use_container_width=True)

with right:
    st.subheader("By movement category")

    if cat_df.empty:
        st.info("No data.")
    else:
        fig_donut = px.pie(
            cat_df,
            names="movement_category",
            values="events",
            hole=0.55,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_donut.update_traces(textposition="outside", textinfo="percent+label")
        fig_donut.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20, l=20, r=20),
            height=340,
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_donut, use_container_width=True)

# ── Row 2: hires vs terminations by org ───────────────────────────────────────

st.subheader("Hires vs terminations by organisation (top 20 L3)")

if org_df.empty:
    st.info("No data for the selected filters.")
else:
    org_df = org_df.sort_values("total", ascending=True).tail(20)

    fig_org = go.Figure()
    fig_org.add_trace(go.Bar(
        y=org_df["org_unit"],
        x=org_df["hires"],
        name="Hires",
        orientation="h",
        marker_color=COLOR_HIRE,
    ))
    fig_org.add_trace(go.Bar(
        y=org_df["org_unit"],
        x=-org_df["terminations"],   # negative for butterfly chart
        name="Terminations",
        orientation="h",
        marker_color=COLOR_TERMINATION,
    ))

    max_val = max(org_df["hires"].max(), org_df["terminations"].max()) + 2

    fig_org.update_layout(
        barmode="overlay",
        height=max(300, len(org_df) * 28),
        margin=dict(t=10, b=10, l=0, r=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        xaxis=dict(
            range=[-max_val, max_val],
            tickvals=[-max_val, -max_val//2, 0, max_val//2, max_val],
            ticktext=[str(abs(v)) for v in [-max_val, -max_val//2, 0, max_val//2, max_val]],
            title="← Terminations    |    Hires →",
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig_org.update_yaxes(showgrid=False)
    fig_org.update_xaxes(gridcolor="rgba(128,128,128,0.15)")
    st.plotly_chart(fig_org, use_container_width=True)

# ── Row 3: detail table ───────────────────────────────────────────────────────

st.subheader("Event detail (latest 2 000 rows)")

if detail_df.empty:
    st.info("No events match the selected filters.")
else:
    # Friendly column names for display
    display_df = detail_df.rename(columns={
        "movement_date":           "Date",
        "movement_category":       "Category",
        "movement_direction":      "Direction",
        "internal_move_type":      "Move type",
        "movement_type":           "Type",
        "l2_org_unit":             "L2 org",
        "l3_org_unit":             "L3 org",
        "department_name":         "Department",
        "is_hire":                 "Hire",
        "is_termination":          "Termination",
        "is_grade_change":         "Grade change",
        "is_org_change":           "Org change",
        "promotion_velocity_days": "Promo velocity (days)",
    })

    display_df["Date"] = pd.to_datetime(display_df["Date"]).dt.date

    # Colour rows by type
    def row_color(row):
        if row.get("Hire"):
            return [f"background-color: {COLOR_HIRE}18"] * len(row)
        if row.get("Termination"):
            return [f"background-color: {COLOR_TERMINATION}18"] * len(row)
        return [""] * len(row)

    styled = display_df.style.apply(row_color, axis=1)

    st.dataframe(styled, use_container_width=True, height=400)

    csv = detail_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv,
        file_name=f"movements_{date_from_s}_{date_to_s}.csv",
        mime="text/csv",
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Source: `v_fact_movement_hranalytics` · `v_dim_org_structure_hranalytics` · HR Analytics DWH")
