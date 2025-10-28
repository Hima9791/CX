# =============================
# ui_sheet2.py  (UPDATED with Top 20 charts)
# =============================
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

from charts import bar_counts
from analytics import sheet2_pivots, top_k_table, detect_outliers

_DEF_METRICS = ["rows","Partscount (PL-Comp)","Codecount"]

def _build_filters(df: pd.DataFrame):
    all_companies = sorted(df.get("CompanyName", pd.Series([], dtype=str)).dropna().unique().tolist())
    all_products  = sorted(df.get("Product", pd.Series([], dtype=str)).dropna().unique().tolist())
    all_grades    = sorted(df.get("Grade", pd.Series([], dtype=str)).dropna().unique().tolist())

    with st.expander("Filters (apply to all tabs)", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            sel_companies = st.multiselect("Company", options=all_companies, default=[])
        with fc2:
            sel_products = st.multiselect("Product (PL)", options=all_products, default=[])
        with fc3:
            sel_grades = st.multiselect("Grade", options=all_grades, default=[])

        # Optional advanced filters for any other categorical columns
        adv = st.toggle("More column filters", value=False)
        extra_filters = {}
        if adv:
            cat_cols = [c for c in df.columns if df[c].dtype == 'object' and c not in ("CompanyName","Product","Grade")]
            if cat_cols:
                pick = st.multiselect("Pick extra columns", options=cat_cols, default=[])
                for c in pick:
                    vals = sorted(df[c].dropna().astype(str).unique().tolist())[:300]
                    extra_filters[c] = st.multiselect(f"{c}", options=vals, default=[])
        return sel_companies, sel_products, sel_grades, extra_filters

def _apply_filters(df: pd.DataFrame, companies, products, grades, extra_filters: dict):
    out = df.copy()
    if companies and "CompanyName" in out.columns:
        out = out[out["CompanyName"].isin(companies)]
    if products and "Product" in out.columns:
        out = out[out["Product"].isin(products)]
    if grades and "Grade" in out.columns:
        out = out[out["Grade"].isin(grades)]
    for c, vals in (extra_filters or {}).items():
        if vals and c in out.columns:
            out = out[out[c].astype(str).isin([str(v) for v in vals])]
    return out

def _make_label(df: pd.DataFrame, cols):
    cols = [c for c in cols if c in df.columns]
    if not cols:
        df["label"] = df.index.astype(str)
    elif len(cols) == 1:
        df["label"] = df[cols[0]].astype(str)
    else:
        df["label"] = df[cols[0]].astype(str) + " — " + df[cols[1]].astype(str)
    return df

def render_sheet2_tab(s2: pd.DataFrame):
    st.markdown("## Sheet2 Explorer")

    metric = st.selectbox("Metric", _DEF_METRICS, index=0)
    piv = sheet2_pivots(s2, metric=metric)

    sel_companies, sel_products, sel_grades, extra_filters = _build_filters(piv)
    piv_f = _apply_filters(piv, sel_companies, sel_products, sel_grades, extra_filters)

    # Distribution bar
    if {"Grade","rows"}.issubset(piv_f.columns):
        dist = piv_f.groupby("Grade")["rows"].sum().reset_index()
        st.altair_chart(bar_counts(dist, "Grade", "rows", title=f"Grade Distribution by {metric}"), use_container_width=True)

    # Tabs for slices
    t_over, t_pl, t_co, t_top, t_out = st.tabs([
        "Overview Table", "PL Analysis", "Company Analysis", "Top-N", "Outliers"
    ])

    with t_over:
        st.write(f"Rows: {len(piv_f):,}")
        st.dataframe(piv_f, use_container_width=True)

    with t_pl:
        st.write("Breakdown by Product Line (PL) with optional grade stacking and company focus.")
        plc1, plc2 = st.columns(2)
        with plc1:
            focus_companies = st.multiselect("Focus companies (optional)", options=sorted(piv_f.get("CompanyName", pd.Series([], dtype=str)).dropna().unique()), default=[])
        with plc2:
            stack_by_grade = st.toggle("Stack bars by Grade", value=True)

        pl_df = piv_f.copy()
        if focus_companies and "CompanyName" in pl_df.columns:
            pl_df = pl_df[pl_df["CompanyName"].isin(focus_companies)]

        if {"Product","rows"}.issubset(pl_df.columns):
            pl_pivot_tbl = (pl_df.pivot_table(index="Product", columns="Grade", values="rows", aggfunc="sum", fill_value=0)
                               .reset_index())
            st.dataframe(pl_pivot_tbl, use_container_width=True)

            if stack_by_grade and {"Grade","Product","rows"}.issubset(pl_df.columns):
                ch = alt.Chart(pl_df).mark_bar().encode(
                    x=alt.X("sum(rows):Q", title="Total"),
                    y=alt.Y("Product:N", sort='-x'),
                    color=alt.Color("Grade:N", legend=alt.Legend(title="Grade")),
                    tooltip=["Product","Grade","rows"]
                ).properties(height=400)
            else:
                agg = pl_df.groupby("Product")["rows"].sum().reset_index()
                ch = alt.Chart(agg).mark_bar().encode(
                    x=alt.X("rows:Q", title="Total"),
                    y=alt.Y("Product:N", sort='-x'),
                    tooltip=["Product","rows"]
                ).properties(height=400)
            st.altair_chart(ch, use_container_width=True)

    with t_co:
        st.write("Breakdown by Company with optional grade stacking and PL focus.")
        coc1, coc2 = st.columns(2)
        with coc1:
            focus_products = st.multiselect("Focus PLs (optional)", options=sorted(piv_f.get("Product", pd.Series([], dtype=str)).dropna().unique()), default=[])
        with coc2:
            stack_by_grade_co = st.toggle("Stack bars by Grade (company)", value=True)

        co_df = piv_f.copy()
        if focus_products and "Product" in co_df.columns:
            co_df = co_df[co_df["Product"].isin(focus_products)]

        if {"CompanyName","rows"}.issubset(co_df.columns):
            co_pivot_tbl = (co_df.pivot_table(index="CompanyName", columns="Grade", values="rows", aggfunc="sum", fill_value=0)
                               .reset_index())
            st.dataframe(co_pivot_tbl, use_container_width=True)

            if stack_by_grade_co and {"Grade","CompanyName","rows"}.issubset(co_df.columns):
                ch2 = alt.Chart(co_df).mark_bar().encode(
                    x=alt.X("sum(rows):Q", title="Total"),
                    y=alt.Y("CompanyName:N", sort='-x'),
                    color=alt.Color("Grade:N", legend=alt.Legend(title="Grade")),
                    tooltip=["CompanyName","Grade","rows"]
                ).properties(height=400)
            else:
                agg2 = co_df.groupby("CompanyName")["rows"].sum().reset_index()
                ch2 = alt.Chart(agg2).mark_bar().encode(
                    x=alt.X("rows:Q", title="Total"),
                    y=alt.Y("CompanyName:N", sort='-x'),
                    tooltip=["CompanyName","rows"]
                ).properties(height=400)
            st.altair_chart(ch2, use_container_width=True)

    with t_top:
        st.write("Select top performers by metric.")
        colA, colB, colC = st.columns(3)
        with colA:
            group_by = st.selectbox("Group by", ["CompanyName","Product","Company+Product"], index=0)
        with colB:
            top_k = st.number_input("Top N", min_value=3, max_value=100, value=10, step=1)
        with colC:
            asc = st.toggle("Ascending (lowest first)", value=False)

        if group_by == "Company+Product":
            grp_cols = ["CompanyName","Product"]
        else:
            grp_cols = [group_by]

        # Table (user-configurable K)
        top_tbl = top_k_table(piv_f, group_cols=grp_cols, value_col="rows", k=int(top_k), ascending=asc)
        st.dataframe(top_tbl, use_container_width=True)

        # --- Top 20 Chart (always shows highest 20 by current metric) ---
        if set(grp_cols).issubset(piv_f.columns):
            agg = piv_f.groupby(grp_cols)["rows"].sum().reset_index()
            agg = agg.sort_values("rows", ascending=False).head(20).copy()
            agg = _make_label(agg, grp_cols)
            st.markdown("#### Top 20 (chart)")
            ch_top20 = alt.Chart(agg).mark_bar().encode(
                x=alt.X("rows:Q", title=f"{metric}"),
                y=alt.Y("label:N", sort='-x', title="Group"),
                tooltip=grp_cols + ["rows"]
            ).properties(height=500)
            st.altair_chart(ch_top20, use_container_width=True)

    with t_out:
        st.write("Find outliers by z-score or IQR")
        o1, o2, o3, o4 = st.columns(4)
        with o1:
            group_by = st.selectbox("Group by", ["CompanyName","Product","Company+Product"], index=0, key="out_g")
        with o2:
            method = st.selectbox("Method", ["Z","IQR"], index=0)
        with o3:
            zthr = st.number_input("Z-score |σ| >=", value=2.5, step=0.1)
        with o4:
            iqr_k = st.number_input("IQR multiplier k", value=1.5, step=0.1)

        if group_by == "Company+Product":
            grp_cols = ["CompanyName","Product"]
        else:
            grp_cols = [group_by]

        out_tbl = detect_outliers(piv_f, group_cols=grp_cols, value_col="rows", method=method, z_threshold=float(zthr), iqr_k=float(iqr_k))
        st.dataframe(out_tbl, use_container_width=True)

        # --- Top 20 Outliers Chart ---
        if not out_tbl.empty:
            if method.upper().startswith("Z") and "zscore" in out_tbl.columns:
                out_tbl = out_tbl.assign(score=out_tbl["zscore"].abs())
                score_title = "|Z-score|"
            else:
                # IQR path uses 'distance' column
                out_tbl = out_tbl.assign(score=out_tbl.get("distance", 0.0))
                score_title = "Outlier distance (IQR)"

            top20o = out_tbl.sort_values("score", ascending=False).head(20).copy()
            top20o = _make_label(top20o, grp_cols)

            st.markdown("#### Top 20 Outliers (chart)")
            ch_out20 = alt.Chart(top20o).mark_bar().encode(
                x=alt.X("score:Q", title=score_title),
                y=alt.Y("label:N", sort='-x', title="Group"),
                tooltip=grp_cols + ["rows", "score"] if set(grp_cols).issubset(top20o.columns) else ["rows", "score"]
            ).properties(height=500)
            st.altair_chart(ch_out20, use_container_width=True)
