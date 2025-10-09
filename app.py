import os
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
from pyathena import connect

# -----------------------------
# App config & env
# -----------------------------
st.set_page_config(page_title="Healthcare Staffing Analytics", layout="wide")

AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
ATHENA_S3_OUTPUT = os.getenv("ATHENA_S3_OUTPUT")  # REQUIRED
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "kerok-healthcare-bronze")
ATHENA_CATALOG  = os.getenv("ATHENA_CATALOG",  "AwsDataCatalog")

if not ATHENA_S3_OUTPUT:
    st.error("Environment variable ATHENA_S3_OUTPUT is required (e.g., s3://kerok-athena-query-output-storage-v1/).")
    st.stop()

# -----------------------------
# Athena connection
# -----------------------------
def _connect():
    return connect(
        region_name=AWS_REGION,
        s3_staging_dir=ATHENA_S3_OUTPUT,
        work_group=ATHENA_WORKGROUP,
        schema_name=ATHENA_DATABASE,
        catalog_name=ATHENA_CATALOG,
    )

def _quote_str(x: str) -> str:
    return "'" + str(x).replace("'", "''") + "'"

def _in_clause(col: str, values: list[str] | None):
    if not values:
        return "TRUE"
    qs = ", ".join(_quote_str(v) for v in values)
    return f"{col} IN ({qs})"

@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    with _connect() as conn:
        return pd.read_sql(sql, conn)

# -----------------------------
# Lookups
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_states() -> list[str]:
    # Use any view guaranteed to have state
    sql = "SELECT DISTINCT state FROM gold_vw_hprd_by_state WHERE state IS NOT NULL ORDER BY state"
    df = run_query(sql)
    return df["state"].dropna().astype(str).tolist()

@st.cache_data(ttl=600, show_spinner=False)
def get_facilities(states: list[str]) -> pd.DataFrame:
    where_states = _in_clause("state", states) if states else "TRUE"
    # Use facility HPRD view for a reliable directory
    sql = f"""
      SELECT DISTINCT ccn, provider_name, state
      FROM gold_vw_hprd_by_facility
      WHERE {where_states}
      ORDER BY provider_name
    """
    return run_query(sql)

@st.cache_data(ttl=600, show_spinner=False)
def get_month_bounds() -> tuple[pd.Timestamp, pd.Timestamp]:
    # Use a monthly view to establish range
    sql = """
      SELECT
        CAST(MIN(CAST(month AS DATE)) AS DATE) AS min_m,
        CAST(MAX(CAST(month AS DATE)) AS DATE) AS max_m
      FROM gold_vw_total_nurse_hours_facility_monthly
    """
    df = run_query(sql)
    if df.empty or pd.isna(df.iloc[0]["min_m"]):
        today = pd.Timestamp.today(tz="UTC").normalize()
        mstart = (today - pd.offsets.MonthBegin(3)).date()
        return (pd.to_datetime(mstart), today)
    return (pd.to_datetime(df.iloc[0]["min_m"]), pd.to_datetime(df.iloc[0]["max_m"]))

def paginate_df(df: pd.DataFrame, page_size: int = 25, key: str = "pager") -> pd.DataFrame:
    total = len(df)
    if total <= page_size:
        return df
    pages = (total + page_size - 1) // page_size
    col1, col2 = st.columns([1, 5])
    with col1:
        page = st.number_input("Page", min_value=1, max_value=pages, step=1, value=1, key=key)
    with col2:
        st.caption(f"{total} rows • {pages} pages • {page_size} per page")
    start = (page - 1) * page_size
    end = start + page_size
    return df.iloc[start:end]

def download_csv(df: pd.DataFrame, label: str, key: str):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{key}.csv",
        mime="text/csv",
        key=f"dl_{key}",
    )

def kpi_row(df: pd.DataFrame, value_col: str, fmt="{:,.2f}", extra: dict | None = None):
    s = pd.to_numeric(df[value_col], errors="coerce").dropna()
    stats = {
        "Count": len(s),
        "Mean": s.mean(),
        "Median": s.median(),
        "Min": s.min(),
        "Max": s.max(),
        "P90": s.quantile(0.90)
    }
    if extra:
        stats.update(extra)
    cols = st.columns(min(6, len(stats)))
    for col, (k, v) in zip(cols, stats.items()):
        if isinstance(v, (int, float, np.floating)):
            col.metric(k, fmt.format(v))
        else:
            col.metric(k, str(v))

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def coerce_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

# -----------------------------
# Sidebar filters (global)
# -----------------------------
st.sidebar.header("Global Filters")

states_all = get_states()
default_states = states_all if len(states_all) <= 6 else states_all[:6]
selected_states = st.sidebar.multiselect("States", options=states_all, default=default_states)

fac_df = get_facilities(selected_states or states_all)
facility_options = [f"{row.provider_name} ({row.ccn}) – {row.state}" for _, row in fac_df.iterrows()]
facility_lookup = {f"{row.provider_name} ({row.ccn}) – {row.state}": row.ccn for _, row in fac_df.iterrows()}
selected_facilities_ui = st.sidebar.multiselect("Facilities", options=facility_options, default=[])
selected_ccns = [facility_lookup[x] for x in selected_facilities_ui]

min_m, max_m = get_month_bounds()
default_start = max(min_m, max_m - pd.offsets.MonthBegin(3))  # ~last 3 months by default
month_range = st.sidebar.date_input(
    "Month range (applies to monthly views)",
    value=(default_start.date(), max_m.date()),
    min_value=min_m.date(),
    max_value=max_m.date(),
)

if isinstance(month_range, tuple):
    start_date, end_date = [pd.to_datetime(x) for x in month_range]
else:
    start_date = pd.to_datetime(month_range)
    end_date = start_date

def where_monthly(alias: str, month_col: str = "month") -> str:
    states_clause = _in_clause(f"{alias}.state", selected_states) if selected_states else "TRUE"
    ccns_clause = _in_clause(f"{alias}.ccn", selected_ccns) if selected_ccns else "TRUE"
    date_clause = f"CAST({alias}.{month_col} AS DATE) BETWEEN DATE '{start_date:%Y-%m-%d}' AND DATE '{end_date:%Y-%m-%d}'"
    return f"{states_clause} AND {ccns_clause} AND {date_clause}"

def where_state_ccn_only(alias: str) -> str:
    states_clause = _in_clause(f"{alias}.state", selected_states) if selected_states else "TRUE"
    ccns_clause = _in_clause(f"{alias}.ccn", selected_ccns) if selected_ccns else "TRUE"
    return f"{states_clause} AND {ccns_clause}"

# -----------------------------
# Tabs
# -----------------------------
st.title("Healthcare Staffing Analytics (Athena Gold Views)")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Facility HPRD",
    "State HPRD",
    "Total Nurse Hours",
    "Perm vs Contract",
    "Bed Utilization (+ Map)",
    "Staffing vs Occupancy"
])

# 1) Facility HPRD (all-time aggregation per your view)
with tab1:
    st.subheader("Facility HPRD (Nurse-to-patient ratio, resident-weighted, overall)")

    # Query (unchanged)
    sql = f"""
      SELECT ccn, provider_name, state,
             days_with_residents, start_date, end_date,
             hprd_weighted, rn_hprd, lpn_hprd, cna_hprd
      FROM gold_vw_hprd_by_facility
      WHERE {where_state_ccn_only('gold_vw_hprd_by_facility')}
    """
    df = run_query(sql)

    if not df.empty:
        # --- KPIs (coerce to numeric first)
        for col in ["hprd_weighted", "rn_hprd", "lpn_hprd", "cna_hprd"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df_valid = df.dropna(subset=["hprd_weighted"]).copy()

        kpi_row(df_valid, "hprd_weighted")

        # --- Top-N control
        topN = st.slider(
            "Show Top-N facilities by HPRD (overview)",
            10, min(300, len(df_valid)), 50, 5, key="hprd_topn"
        )

        # Stable sort: highest HPRD first; tie-break by provider name
        df_sorted = df_valid.sort_values(
            ["hprd_weighted", "provider_name"],
            ascending=[False, True],
            kind="mergesort"  # stable sort
        )

        df_top = df_sorted.head(topN)

        # --- Overview bar (y sorted by numeric field)
        bar = alt.Chart(df_top).mark_bar().encode(
            x=alt.X("hprd_weighted:Q", title="HPRD (Weighted)"),
            y=alt.Y(
                "provider_name:N",
                sort=alt.SortField(field="hprd_weighted", order="descending"),
                title="Facility"
            ),
            color=alt.Color("state:N"),
            tooltip=[
                "provider_name", "state", "ccn",
                alt.Tooltip("hprd_weighted:Q", format=".2f"),
                alt.Tooltip("rn_hprd:Q", format=".2f"),
                alt.Tooltip("lpn_hprd:Q", format=".2f"),
                alt.Tooltip("cna_hprd:Q", format=".2f"),
                "days_with_residents", "start_date", "end_date"
            ]
        ).properties(height=400)

        st.altair_chart(bar, use_container_width=True)

    table = paginate_df(df.sort_values("hprd_weighted", ascending=False), page_size=25, key="t1")
    st.dataframe(table, use_container_width=True)
    download_csv(df, "Download CSV", "facility_hprd_overall")


# 2) State HPRD (overall per your view)
with tab2:
    st.subheader("State HPRD (Nurse-to-patient ratio, resident-weighted, overall)")

    # Local override: default to all states, optional filter
    local_states = st.multiselect("Filter states (optional)", options=states_all, default=states_all)
    where_local = _in_clause("state", local_states) if local_states else "TRUE"

    sql = f"""
      SELECT state, start_date, end_date, hprd_weighted
      FROM gold_vw_hprd_by_state
      WHERE {where_local}
      ORDER BY hprd_weighted DESC
    """
    df = run_query(sql)

    df = coerce_numeric(df, ["hprd_weighted"])

    # ...
    if not df.empty:
        kpi_row(df, "hprd_weighted")
        bar = alt.Chart(df).mark_bar().encode(
            x=alt.X("hprd_weighted:Q", title="HPRD (Weighted)"),
            y=alt.Y("state:N",
                    sort=alt.SortField(field="hprd_weighted", order="descending"),
                    title="State"),
            tooltip=["state", alt.Tooltip("hprd_weighted:Q", format=".2f"), "start_date", "end_date"]
        ).properties(height=400)
        st.altair_chart(bar, use_container_width=True)

    table = paginate_df(df, page_size=50, key="t2")
    st.dataframe(table, use_container_width=True)
    download_csv(df, "Download CSV", "state_hprd_overall")


# 3) Total nurse hours by facility/month
with tab3:
    st.subheader("Total Nurse Hours")

    view_mode = st.radio(
        "View",
        ["Facility Summary (Avg + Range)", "State Comparison"],
        horizontal=False,
        key="hours_view_mode"
    )

    if view_mode.startswith("Facility"):
        # pull facility-month hours within filters
        sql = f"""
          SELECT state, provider_name, ccn, month, total_hours_direct
          FROM gold_vw_total_nurse_hours_facility_monthly
          WHERE {where_monthly('gold_vw_total_nurse_hours_facility_monthly', 'month')}
        """
        df = run_query(sql)
        if not df.empty:
            df = coerce_numeric(df, ["total_hours_direct"])
            df = coerce_datetime(df, ["month"])

            # aggregate per facility over the selected months
            agg = (df.groupby(["ccn","provider_name","state"], as_index=False)
                     .agg(avg_hours=("total_hours_direct","mean"),
                          min_hours=("total_hours_direct","min"),
                          max_hours=("total_hours_direct","max"),
                          p10=("total_hours_direct", lambda s: s.quantile(0.10)),
                          p90=("total_hours_direct", lambda s: s.quantile(0.90)),
                          months=("total_hours_direct","size")))

            # Top-N by average hours
            topN = st.slider("Top-N facilities by average monthly hours", 10, min(200, len(agg)), 50, 5, key="hours_fac_topn")
            agg_sorted = agg.sort_values(["avg_hours","provider_name"], ascending=[False, True], kind="mergesort")
            keep = agg_sorted.head(topN)

            kpi_row(keep, "avg_hours", fmt="{:,.0f}", extra={"Facilities": len(keep)})

            # range (p10→p90) + dot at mean; y ordered by mean desc
            y_order = keep.sort_values("avg_hours", ascending=False)["provider_name"].tolist()

            rng = alt.Chart(keep).mark_rule(opacity=0.6).encode(
                x=alt.X("p10:Q", title="Monthly hours"),
                x2="p90:Q",
                y=alt.Y("provider_name:N", sort=y_order, title="Facility"),
                tooltip=["provider_name","state",
                         alt.Tooltip("min_hours:Q", format=",.0f"),
                         alt.Tooltip("avg_hours:Q", format=",.0f"),
                         alt.Tooltip("max_hours:Q", format=",.0f"),
                         "months"]
            )

            dot = alt.Chart(keep).mark_point(filled=True).encode(
                x=alt.X("avg_hours:Q"),
                y=alt.Y("provider_name:N", sort=y_order),
                color=alt.Color("state:N", legend=alt.Legend(title="State")),
                size=alt.value(60)
            )

            st.altair_chart((rng + dot).properties(height=max(240, min(30*len(keep), 700))),
                            use_container_width=True)

            # detail table (Top-N)
            table = paginate_df(keep, 50, key="t3_fac_summary")
            st.dataframe(table, use_container_width=True)
            download_csv(keep, "Download CSV", "total_hours_facility_summary")


    else:

        # --- State Comparison (Monthly): Ranked Bars or Dumbbell ---

        st.subheader("Total Nurse Hours — State Comparison")

        # Local state filter (ignores global)

        local_states = st.multiselect(

            "Filter states (optional)",

            options=states_all,

            default=states_all,

            key="state_hours_local_states"

        )

        # Predicates (avoid global state filter)

        month_pred = f"CAST(month AS DATE) BETWEEN DATE '{start_date:%Y-%m-%d}' AND DATE '{end_date:%Y-%m-%d}'"

        where_local_s = _in_clause("s.state", local_states) if local_states else "TRUE"

        where_local_fc = _in_clause("state", local_states) if local_states else "TRUE"

        # Pull state-month totals + MoM% and # facilities to enable multiple comparisons

        sql = f"""

          WITH fac_counts AS (

            SELECT state, month, COUNT(DISTINCT ccn) AS n_facilities

            FROM gold_vw_total_nurse_hours_facility_monthly

            WHERE {where_local_fc}

              AND {month_pred}

            GROUP BY 1,2

          ),

          state_month AS (

            SELECT s.state,

                   s.month,

                   s.total_hours_direct,

                   date_format(s.month, '%%Y-%%m') AS month_label

            FROM gold_vw_total_nurse_hours_state_monthly s

            WHERE {where_local_s}

              AND {month_pred}

          ),

          joined AS (

            SELECT sm.state,

                   sm.month,

                   sm.month_label,

                   sm.total_hours_direct,

                   COALESCE(fc.n_facilities, 0) AS n_facilities

            FROM state_month sm

            LEFT JOIN fac_counts fc

              ON fc.state = sm.state AND fc.month = sm.month

          ),

          with_change AS (

            SELECT j.*,

                   (j.total_hours_direct

                     - LAG(j.total_hours_direct) OVER (PARTITION BY j.state ORDER BY j.month))

                   / NULLIF(LAG(j.total_hours_direct) OVER (PARTITION BY j.state ORDER BY j.month), 0) AS mom_change

            FROM joined j

          )

          SELECT *

          FROM with_change

          ORDER BY month

        """

        df = run_query(sql)

        if df.empty:

            st.info("No data for the selected window.")

        else:

            df = coerce_numeric(df, ["total_hours_direct", "n_facilities", "mom_change"])
            df = coerce_datetime(df, ["month"])
            df["avg_per_fac"] = np.where(df["n_facilities"] > 0,
                                         df["total_hours_direct"] / df["n_facilities"], np.nan)
            view_mode = st.radio(
                "View",
                ["Ranked Bars (pick month)", "Start vs End (Dumbbell)"],
                horizontal=True,
                key="state_hours_view"
            )
            # Common month lists/labels
            month_choices = df.sort_values("month")["month"].unique().tolist()
            month_labels = df.sort_values("month")["month_label"].unique().tolist()
            if view_mode.startswith("Ranked"):
                # choose a single month to rank states
                chosen = st.selectbox(
                    "Month",
                    options=month_choices,
                    index=len(month_choices) - 1,
                    format_func=lambda x: pd.to_datetime(x).strftime("%Y-%m"),
                    key="state_hours_month_select"
                )
                dfm = df[df["month"] == chosen].copy()
                # KPIs
                kpi_row(dfm, "total_hours_direct", fmt="{:,.0f}",
                        extra={"States": dfm["state"].nunique(),
                               "Avg / facility": f"{np.nanmean(dfm['avg_per_fac']):,.0f}"})
                # Sort states by value desc
                dfm = dfm.sort_values(["total_hours_direct", "state"], ascending=[False, True], kind="mergesort")
                # Optional color by MoM change
                color_mode = st.radio("Bar color", ["Uniform", "MoM change"], horizontal=True, key="state_rank_color")
                if color_mode == "MoM change":
                    color_enc = alt.Color("mom_change:Q", title="MoM change",
                                          scale=alt.Scale(scheme="redblue", domainMid=0),
                                          legend=alt.Legend(format=".0%"))
                else:
                    color_enc = alt.value("#4c78a8")
                bar = alt.Chart(dfm).mark_bar().encode(
                    x=alt.X("total_hours_direct:Q", title="Total hours"),
                    y=alt.Y("state:N", sort="-x", title="State"),
                    color=color_enc,
                    tooltip=[
                        "state",
                        alt.Tooltip("total_hours_direct:Q", title="Total hours", format=",.0f"),
                        alt.Tooltip("n_facilities:Q", title="# facilities", format=",.0f"),
                        alt.Tooltip("avg_per_fac:Q", title="Avg / facility", format=",.0f"),
                        alt.Tooltip("mom_change:Q", title="MoM change", format=".0%")
                    ]
                ).properties(height=max(240, 22 * dfm["state"].nunique()))
                st.altair_chart(bar, use_container_width=True)
                table = paginate_df(dfm, 100, key="t3_state_ranked")
                st.dataframe(table, use_container_width=True)
                download_csv(dfm, "Download CSV", "total_hours_state_ranked")

            else:
                # Dumbbell: first vs last month in the selection
                first_m = min(month_choices)
                last_m = max(month_choices)
                base = df[df["month"].isin([first_m, last_m])].copy()
                piv = (base.pivot_table(index="state",
                                        columns="month",
                                        values="total_hours_direct",
                                        aggfunc="sum")
                       .reset_index()
                       .rename(columns={first_m: "first_hours", last_m: "last_hours"}))
                # bring facility counts for tooltip (optional)
                fac_first = (base[base["month"] == first_m][["state", "n_facilities"]]
                             .rename(columns={"n_facilities": "n_fac_first"}))
                fac_last = (base[base["month"] == last_m][["state", "n_facilities"]]
                            .rename(columns={"n_facilities": "n_fac_last"}))
                piv = piv.merge(fac_first, on="state", how="left").merge(fac_last, on="state", how="left")
                piv = coerce_numeric(piv, ["first_hours", "last_hours", "n_fac_first", "n_fac_last"])
                piv["delta"] = piv["last_hours"] - piv["first_hours"]
                piv["pct"] = np.where(piv["first_hours"] > 0, piv["delta"] / piv["first_hours"], np.nan)
                # KPIs
                kpi_row(piv, "last_hours", fmt="{:,.0f}",
                        extra={"Δ total": f"{piv['delta'].sum():,.0f}",
                               "Median %Δ": f"{np.nanmedian(piv['pct']):.1%}"})
                # Top-N by max(first,last) to keep chart readable
                topN = st.slider("Top-N states by size (ma of first/last)", 5, min(50, len(piv)), min(20, len(piv)), 1,
                                 key="state_dumbbell_topn")
                piv["rank_key"] = piv[["first_hours", "last_hours"]].max(axis=1)
                keep = (piv.sort_values(["rank_key", "state"], ascending=[False, True], kind="mergesort")
                        .head(topN))
                y_order = keep.sort_values("last_hours", ascending=False)["state"].tolist()
                # Dumbbell: line from first→last, points colored by direction
                line = alt.Chart(keep).mark_rule().encode(
                    y=alt.Y("state:N", sort=y_order, title="State"),
                    x=alt.X("first_hours:Q", title=f"{pd.to_datetime(first_m).strftime('%Y-%m')} hours"),
                    x2=alt.X2("last_hours:Q")
                )
                updown = alt.Chart(keep).mark_point(filled=True, size=80).encode(
                    y=alt.Y("state:N", sort=y_order),
                    x=alt.X("first_hours:Q"),
                    color=alt.value("#999999"),
                    tooltip=["state", alt.Tooltip("first_hours:Q", format=",.0f")]
                ) + alt.Chart(keep).mark_point(filled=True, size=80).encode(
                    y=alt.Y("state:N", sort=y_order),
                    x=alt.X("last_hours:Q"),
                    color=alt.Color("pct:Q", title="%Δ", scale=alt.Scale(scheme="redblue", domainMid=0),
                                    legend=alt.Legend(format=".0%")),
                    tooltip=["state", alt.Tooltip("last_hours:Q", format=",.0f"),
                             alt.Tooltip("delta:Q", title="Δ", format=",.0f"),
                             alt.Tooltip("pct:Q", title="%Δ", format=".0%")]
                )
                st.altair_chart((line + updown).properties(height=max(260, 22 * len(keep))), use_container_width=True)
                table = paginate_df(keep.drop(columns=["rank_key"]), 100, key="t3_state_dumbbell")
                st.dataframe(table, use_container_width=True)
                download_csv(keep.drop(columns=["rank_key"]), "Download CSV", "total_hours_state_dumbbell")

# 4) Permanent vs Contract (monthly)
with tab4:
    st.subheader("Permanent vs Contract")

    # pick a single month for clarity
    m_sql = f"""
      SELECT DISTINCT month
      FROM gold_vw_perm_vs_contract_facility_monthly
      WHERE {_in_clause("state", selected_states) if selected_states else "TRUE"}
        AND CAST(month AS DATE) BETWEEN DATE '{start_date:%Y-%m-%d}' AND DATE '{end_date:%Y-%m-%d}'
      ORDER BY month
    """
    months = run_query(m_sql)
    if months.empty:
        st.info("No monthly data for the selected filters.")
    else:
        chosen = st.selectbox("Month", months["month"].tolist(), index=len(months)-1,
                              format_func=lambda x: str(pd.to_datetime(x).date()), key="perm_contract_month_select")
        sql = f"""
          SELECT state, ccn, month, emp_hours, ctr_hours
          FROM gold_vw_perm_vs_contract_facility_monthly
          WHERE {_in_clause("state", selected_states) if selected_states else "TRUE"}
            AND CAST(month AS DATE) = DATE '{pd.to_datetime(chosen):%Y-%m-%d}'
            AND {_in_clause("ccn", selected_ccns) if selected_ccns else "TRUE"}
        """
        df = run_query(sql)


        if not df.empty:
            fac_names = get_facilities(selected_states or states_all)[["ccn","provider_name","state"]]
            df = df.merge(fac_names, on=["ccn","state"], how="left")
            df = coerce_numeric(df, ["emp_hours", "ctr_hours"])
            df["total_hours"] = df["emp_hours"].fillna(0) + df["ctr_hours"].fillna(0)
            df["pct_contract"] = np.where(df["total_hours"] > 0,
                                          df["ctr_hours"].fillna(0) / df["total_hours"], np.nan)

            kpi_row(df, "pct_contract", fmt="{:.1%}", extra={"Total hours": df["total_hours"].sum()})

            # Scatter: contract share vs size (total hours)

            swap = st.checkbox("Plot hours on X-axis (log scale)", value=True, key="pc_swap_axes")
            log_hours = True  # always log when hours on X; if you want a toggle, expose another checkbox

            if swap:
                x_enc = alt.X("total_hours:Q", title="Total hours (log)", scale=alt.Scale(type="log", nice=True))
                y_enc = alt.Y("pct_contract:Q", title="Contract share")
            else:
                x_enc = alt.X("pct_contract:Q", title="Contract share")
                y_enc = alt.Y("total_hours:Q", title="Total hours", scale=alt.Scale(type="log", nice=True))

            sc = alt.Chart(df).mark_circle(opacity=0.85).encode(
                x=x_enc, y=y_enc,
                size=alt.Size("total_hours:Q", title="Total hours"),
                color=alt.Color("state:N"),
                tooltip=["provider_name", "state",
                         alt.Tooltip("pct_contract:Q", format=".1%"),
                         alt.Tooltip("total_hours:Q", format=",.0f")]
            ).properties(height=420)
            st.altair_chart(sc, use_container_width=True)

            # Optional: 100% bars for Top-N facilities
            with st.expander("Top-N facilities (ranked by contract share, bubble sized by hours)", expanded=True):
                # Rank settings
                rank_by = st.radio(
                    "Order by",
                    ["Contract share (desc)", "Total hours (desc)"],
                    horizontal=True,
                    key="pc_rank_by2"
                )
                topN = st.slider("Top-N facilities", 10, min(200, len(df)), 50, 5, key="pc_topn_bubble")

                # Ensure numerics exist (safe if already coerced earlier)
                df = coerce_numeric(df, ["pct_contract", "total_hours", "emp_hours", "ctr_hours"])

                # Stable ranking
                if rank_by.startswith("Contract"):
                    df_sorted = df.sort_values(
                        ["pct_contract", "total_hours", "provider_name"],
                        ascending=[False, False, True],
                        kind="mergesort"
                    )
                else:
                    df_sorted = df.sort_values(
                        ["total_hours", "pct_contract", "provider_name"],
                        ascending=[False, False, True],
                        kind="mergesort"
                    )

                keep = df_sorted.head(topN).copy()
                # Y order by the ranking we just made (so previously visible items stay on top as N grows)
                y_order = keep["provider_name"].tolist()

                # Helpful KPIs for the selection
                kpi_row(
                    keep, "pct_contract", fmt="{:.1%}",
                    extra={
                        "Facilities": len(keep),
                        "Total hours": f"{keep['total_hours'].sum():,.0f}",
                        "Median % contract": f"{keep['pct_contract'].median():.1%}"
                    }
                )

                # Bubble chart: x = contract share, y = facility, size = total hours
                bubble = alt.Chart(keep).mark_circle(opacity=0.9).encode(
                    x=alt.X("pct_contract:Q", title="Contract share"),
                    y=alt.Y("provider_name:N", sort=y_order, title="Facility"),
                    size=alt.Size("total_hours:Q", title="Total hours", scale=alt.Scale(type="sqrt", nice=True)),
                    color=alt.Color("state:N", title="State"),
                    tooltip=[
                        "provider_name", "state",
                        alt.Tooltip("pct_contract:Q", title="Contract share", format=".1%"),
                        alt.Tooltip("total_hours:Q", title="Total hours", format=",.0f"),
                        alt.Tooltip("emp_hours:Q", title="Perm hours", format=",.0f"),
                        alt.Tooltip("ctr_hours:Q", title="Contract hours", format=",.0f")
                    ]
                ).properties(height=max(240, min(30 * len(keep), 800)))

                # Optional guide lines at 25/50/75% share to help scan
                guides = alt.Chart(
                    pd.DataFrame({"x": [0.25, 0.50, 0.75]})
                ).mark_rule(strokeDash=[3, 3]).encode(x="x:Q")

                st.altair_chart(bubble + guides, use_container_width=True)

                # Detail table (matches what's shown)
                table = paginate_df(
                    keep[["provider_name", "state", "pct_contract", "total_hours", "emp_hours", "ctr_hours"]],
                    50, key="t4_bubble_table")
                st.dataframe(table, use_container_width=True)
                download_csv(keep, "Download CSV", "perm_contract_topn_bubbles")

# 5) Bed Utilization (reworked)
with tab5:
    st.subheader("Bed Utilization by Facility / Month")

    sql = f"""
      SELECT v.state, v.provider_name, v.ccn, v.month,
             v.bed_utilization_rate_monthly AS utilization,
             v.resident_days, v.observed_days, v.certified_beds_reported,
             d.latitude AS lat, d.longitude AS lon
      FROM gold_vw_bed_utilization_facility_monthly v
      LEFT JOIN gold_facility_dim d ON d.ccn = v.ccn
      WHERE {where_monthly('v', 'month')}
      ORDER BY v.month
    """
    df = run_query(sql)

    if not df.empty:
        df = coerce_numeric(df, ["utilization","resident_days","observed_days","certified_beds_reported","lat","lon"])
        df = coerce_datetime(df, ["month"])

        # KPIs across selection
        kpi_row(df, "utilization", fmt="{:.2f}",
                extra={"Facilities": df["ccn"].nunique(), "Months": df["month"].nunique()})

        view_mode = st.radio(
            "View",
            ["Ranked (pick month)", "Level vs Variability (scatter)", "Start vs End (dumbbell)"],
            horizontal=True,
            key="bed_view_mode"
        )

        # Common helpers
        months_sorted = sorted(df["month"].dropna().unique())
        month_labels  = [pd.to_datetime(m).strftime("%Y-%m") for m in months_sorted]

        if view_mode.startswith("Ranked"):
            # Single-month ranked bars
            chosen = st.selectbox(
                "Month",
                options=months_sorted,
                index=len(months_sorted)-1,
                format_func=lambda x: pd.to_datetime(x).strftime("%Y-%m"),
                key="bed_rank_month_select"
            )
            dfm = df[df["month"] == chosen].dropna(subset=["utilization"]).copy()

            # Top-N by utilization
            topN = st.slider("Top-N facilities by utilization", 10, min(200, len(dfm)), min(50, len(dfm)), 5, key="bed_rank_topn")
            dfm = dfm.sort_values(["utilization","provider_name"], ascending=[False, True], kind="mergesort").head(topN)

            y_order = dfm["provider_name"].tolist()
            bar = alt.Chart(dfm).mark_bar().encode(
                x=alt.X("utilization:Q", title="Bed utilization"),
                y=alt.Y("provider_name:N", sort=y_order, title="Facility"),
                color=alt.Color("state:N", title="State"),
                tooltip=[
                    "provider_name","state",
                    alt.Tooltip("utilization:Q", format=".2f"),
                    alt.Tooltip("resident_days:Q", title="Resident-days", format=",.0f"),
                    "observed_days","certified_beds_reported"
                ]
            ).properties(height=max(240, min(30*len(y_order), 700)))
            st.altair_chart(bar, use_container_width=True)

            table = paginate_df(dfm, 50, key="t5_rank_table")
            st.dataframe(table, use_container_width=True)
            download_csv(dfm, "Download CSV", "bed_util_ranked")

        elif "Variability" in view_mode:
            # Facility scatter: x=avg utilization, y=variability, size=exposure
            agg = (df.groupby(["ccn","provider_name","state"], as_index=False)
                     .agg(avg_util=("utilization","mean"),
                          std_util=("utilization","std"),
                          p10=("utilization", lambda s: s.quantile(0.10)),
                          p90=("utilization", lambda s: s.quantile(0.90)),
                          res_days=("resident_days","sum"),
                          months=("utilization","size")))
            # choose variability metric
            var_metric = st.radio("Variability metric", ["Std dev", "P90 − P10"], horizontal=True, key="bed_scatter_var_metric")
            agg["var_util"] = agg["std_util"] if var_metric=="Std dev" else (agg["p90"] - agg["p10"])

            # Top-N by exposure (resident-days) to keep it readable
            topN = st.slider("Top-N facilities by resident-days", 10, min(300, len(agg)), 100, 10, key="bed_scatter_topn")
            keep = (agg.sort_values(["res_days","provider_name"], ascending=[False, True], kind="mergesort")
                        .head(topN))
            kpi_row(keep, "avg_util", fmt="{:.2f}", extra={"Median variability": f"{keep['var_util'].median():.2f}"})

            sc = alt.Chart(keep).mark_circle(opacity=0.85).encode(
                x=alt.X("avg_util:Q", title="Average utilization"),
                y=alt.Y("var_util:Q", title=f"Variability ({var_metric})"),
                size=alt.Size("res_days:Q", title="Resident-days", scale=alt.Scale(type="sqrt")),
                color=alt.Color("state:N", title="State"),
                tooltip=[
                    "provider_name","state",
                    alt.Tooltip("avg_util:Q", format=".2f"),
                    alt.Tooltip("var_util:Q", title="Variability", format=".2f"),
                    alt.Tooltip("res_days:Q", title="Resident-days", format=",.0f"),
                    "months"
                ]
            ).properties(height=450)
            st.altair_chart(sc, use_container_width=True)

            table = paginate_df(keep.drop(columns=["std_util","p10","p90"]), 50, key="t5_scatter_table")
            st.dataframe(table, use_container_width=True)
            download_csv(keep, "Download CSV", "bed_util_scatter")

        else:
            # Dumbbell: first vs last month change per facility
            if len(months_sorted) < 2:
                st.info("Need at least two months for a dumbbell view.")
            else:
                first_m, last_m = months_sorted[0], months_sorted[-1]
                base = df[df["month"].isin([first_m, last_m])].copy()
                piv = (base.pivot_table(index=["ccn","provider_name","state"],
                                        columns="month", values="utilization", aggfunc="mean")
                              .reset_index()
                              .rename(columns={first_m:"first_util", last_m:"last_util"}))
                # exposure for ranking
                exposure = (df.groupby("ccn", as_index=False)["resident_days"].sum().rename(columns={"resident_days":"res_days"}))
                piv = piv.merge(exposure, on="ccn", how="left")
                piv["delta"] = piv["last_util"] - piv["first_util"]

                topN = st.slider("Top-N facilities by resident-days", 10, min(200, len(piv)), 50, 5, key="bed_dumbbell_topn")
                keep = (piv.sort_values(["res_days","provider_name"], ascending=[False, True], kind="mergesort")
                            .head(topN))
                y_order = keep.sort_values("last_util", ascending=False)["provider_name"].tolist()

                line = alt.Chart(keep).mark_rule().encode(
                    y=alt.Y("provider_name:N", sort=y_order, title="Facility"),
                    x=alt.X("first_util:Q", title=f"{pd.to_datetime(first_m).strftime('%Y-%m')}"),
                    x2="last_util:Q"
                )
                pts = (alt.Chart(keep).mark_point(filled=True, size=70).encode(
                    y=alt.Y("provider_name:N", sort=y_order),
                    x=alt.X("first_util:Q"),
                    color=alt.value("#999"))
                       +
                       alt.Chart(keep).mark_point(filled=True, size=70).encode(
                    y=alt.Y("provider_name:N", sort=y_order),
                    x=alt.X("last_util:Q"),
                    color=alt.Color("delta:Q", title="Δ utilization", scale=alt.Scale(scheme="redblue", domainMid=0)),
                    tooltip=["provider_name","state",
                             alt.Tooltip("first_util:Q", format=".2f"),
                             alt.Tooltip("last_util:Q", format=".2f"),
                             alt.Tooltip("delta:Q", title="Δ", format="+.2f"),
                             alt.Tooltip("res_days:Q", title="Resident-days", format=",.0f")]
                ))
                st.altair_chart((line + pts).properties(height=max(240, min(30*len(keep), 700))),
                                use_container_width=True)

                table = paginate_df(keep.drop(columns=[]), 50, key="t5_dumbbell_table")
                st.dataframe(table, use_container_width=True)
                download_csv(keep, "Download CSV", "bed_util_dumbbell")

        # --- Distribution (always visible)
        with st.expander("Distribution across selected period", expanded=True):
            hist = alt.Chart(df).mark_bar().encode(
                x=alt.X("utilization:Q", bin=alt.Bin(maxbins=40), title="Utilization"),
                y=alt.Y("count():Q", title="Facilities")
            )
            box = alt.Chart(df).mark_boxplot().encode(y=alt.Y("utilization:Q", title="Utilization"))
            st.altair_chart(hist | box, use_container_width=True)

        # --- Map (latest month), no Mapbox token needed (Carto/OSM tiles)
        import pydeck as pdk
        import numpy as np

        latest_m = df["month"].max()
        latest_df = df[(df["month"] == latest_m) & df["lat"].notna() & df["lon"].notna()].copy()
        st.caption(f"Map — {pd.to_datetime(latest_m).strftime('%Y-%m')} (color by utilization, size by resident-days)")

        if not latest_df.empty:
            # Build color array from utilization (blue→red)
            def util_to_color(u):
                if pd.isna(u):
                    return [160, 160, 160, 140]
                v = float(max(0.0, min(1.0, u)))
                r = int(20 + 235 * v)
                g = int(60 + 40 * (1 - v))
                b = int(210 - 190 * v)
                return [r, g, b, 170]


            latest_df["color"] = latest_df["utilization"].apply(util_to_color)

            # PRECOMPUTE radius (no functions in JSON accessors!)
            # radius ~ 20*sqrt(resident_days), clipped to [2000, 12000]
            latest_df["radius"] = (
                latest_df["resident_days"]
                .fillna(0)
                .clip(lower=0)
                .apply(lambda x: int(np.clip(20 * np.sqrt(x), 2000, 12000)))
            )

            view = pdk.ViewState(
                latitude=float(latest_df["lat"].mean()),
                longitude=float(latest_df["lon"].mean()),
                zoom=4
            )

            tile_layer = pdk.Layer(
                "TileLayer",
                data="https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
                min_zoom=0, max_zoom=19, tile_size=256
            )

            points = pdk.Layer(
                "ScatterplotLayer",
                data=latest_df,
                get_position='[lon, lat]',  # OK as a field list
                get_fill_color='color',  # uses the precomputed RGBA list
                get_radius='radius',  # uses the precomputed numeric column
                pickable=True
            )

            deck = pdk.Deck(
                layers=[tile_layer, points],
                initial_view_state=view,
                tooltip={
                    "text": "{provider_name}\nState: {state}\nUtil: {utilization}\nRes-days: {resident_days}"
                }
            )
            st.pydeck_chart(deck, use_container_width=True)
        else:
            st.info("No coordinates available for the selected filters/month.")

# 6) Staffing vs Occupancy (scatter, computed monthly HPRD via two monthly views)
with tab6:
    st.subheader("Staffing vs Occupancy (Monthly HPRD vs Utilization)")
    # Month choices (from bed util view, respects filters)
    month_sql = f"""
      SELECT DISTINCT month
      FROM gold_vw_bed_utilization_facility_monthly
      WHERE {_in_clause("state", selected_states) if selected_states else "TRUE"}
        AND CAST(month AS DATE) BETWEEN DATE '{start_date:%Y-%m-%d}' AND DATE '{end_date:%Y-%m-%d}'
        AND {_in_clause("ccn", selected_ccns) if selected_ccns else "TRUE"}
      ORDER BY month
    """
    months = run_query(month_sql)
    if months.empty:
        st.info("No monthly data for the selected filters.")
    else:
        month_choices = months["month"].tolist()
        chosen_month = st.selectbox(
            "Month",
            options=month_choices,
            index=len(month_choices)-1,
            format_func=lambda x: str(pd.to_datetime(x).date()),
            key="staffing_vs_occupancy_month_select"
        )
        # Join monthly hours with monthly bed util to compute HPRD = total_hours_direct / resident_days
        sql = f"""
          SELECT bu.state,
                 bu.provider_name,
                 bu.ccn,
                 bu.month,
                 bu.bed_utilization_rate_monthly AS utilization,
                 bu.resident_days,
                 bu.observed_days,
                 CAST(bu.resident_days / NULLIF(bu.observed_days, 0) AS DECIMAL(18,4)) AS monthly_avg_residents,
                 th.total_hours_direct,
                 CAST(th.total_hours_direct / NULLIF(CAST(bu.resident_days AS DECIMAL(18,4)), 0) AS DECIMAL(18,4)) AS hprd_monthly
          FROM gold_vw_bed_utilization_facility_monthly bu
          INNER JOIN gold_vw_total_nurse_hours_facility_monthly th
            ON th.ccn = bu.ccn AND th.month = bu.month
          WHERE {_in_clause("bu.state", selected_states) if selected_states else "TRUE"}
            AND CAST(bu.month AS DATE) = DATE '{pd.to_datetime(chosen_month):%Y-%m-%d}'
            AND {_in_clause("bu.ccn", selected_ccns) if selected_ccns else "TRUE"}
        """
        df = run_query(sql)
        if not df.empty:
            df = coerce_numeric(df,
                                ["utilization", "resident_days", "observed_days", "total_hours_direct", "hprd_monthly",
                                 "monthly_avg_residents"])
            df = coerce_datetime(df, ["month"])

            sc = alt.Chart(df).mark_circle().encode(
                x=alt.X("utilization:Q", title="Bed Utilization Rate"),
                y=alt.Y("hprd_monthly:Q", title="HPRD (Monthly, Weighted)"),
                size=alt.Size("monthly_avg_residents:Q", title="Avg Residents"),
                color=alt.Color("state:N"),
                tooltip=[
                    "provider_name","state",
                    alt.Tooltip("hprd_monthly:Q", format=".2f"),
                    alt.Tooltip("utilization:Q", format=".2f"),
                    alt.Tooltip("monthly_avg_residents:Q", format=".1f"),
                    "resident_days","observed_days","total_hours_direct"
                ]
            ).properties(height=450)
            st.altair_chart(sc, use_container_width=True)
        table = paginate_df(df, page_size=100, key="t6")
        st.dataframe(table, use_container_width=True)
        download_csv(df, "Download CSV", "staffing_vs_occupancy_hprd_vs_utilization")

st.caption("Views queried from Athena (Gold) • "
           f"Workgroup: {ATHENA_WORKGROUP} • Database: {ATHENA_DATABASE} • "
           f"Catalog: {ATHENA_CATALOG} • Region: {AWS_REGION}")
