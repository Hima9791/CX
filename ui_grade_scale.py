# ui_grade_scale.py — bigger charts, axis & compare controls (UPDATED)
from typing import Optional, Tuple, Dict, Any

import numpy as np
import pandas as pd
import streamlit as st

from charts import grade_ribbons, ranges_highlight, bar_counts
from analytics import classify_ranges, confusion_table, DEFAULT_BANDS
from parser_sheet1 import reconstruct_ranges


def _make_signature(s1: Optional[pd.DataFrame], s2: Optional[pd.DataFrame]) -> Tuple:
    """Build a lightweight signature to detect when inputs change."""

    def _sig(df: Optional[pd.DataFrame]) -> Tuple[int, Tuple[str, ...]]:
        if df is None:
            return (0, tuple())
        return (len(df), tuple(df.columns))

    return _sig(s1) + _sig(s2)


def _prepare_grade_data(
    s1: Optional[pd.DataFrame], s2: Optional[pd.DataFrame]
) -> Tuple[pd.DataFrame, list[str], float, float]:
    """Reconstruct ranges and gather helper metadata."""

    if s1 is None or s1.empty:
        ranges = pd.DataFrame(columns=["Grade_current", "tmin_c", "tmax_c"])
        return ranges, [], -60.0, 160.0

    ranges = reconstruct_ranges(s1)

    if "Grade_current" not in ranges.columns and "Grade" in s1.columns:
        try:
            ranges = ranges.copy()
            ranges["Grade_current"] = s1["Grade"].astype(str).values[: len(ranges)]
        except Exception:
            ranges["Grade_current"] = "Unknown"

    s1_grades = []
    if "Grade" in s1.columns:
        s1_grades = s1["Grade"].dropna().astype(str).tolist()

    s2_grades = []
    if s2 is not None and "Grade" in s2.columns:
        s2_grades = s2["Grade"].dropna().astype(str).tolist()

    discovered = sorted({str(x) for x in s1_grades + s2_grades})

    if ranges.empty:
        global_min, global_max = -60.0, 160.0
    else:
        global_min = float(min([-60.0] + ranges["tmin_c"].dropna().tolist()))
        global_max = float(max([160.0] + ranges["tmax_c"].dropna().tolist()))

    return ranges, discovered, global_min, global_max


def _default_grade_state(s1: pd.DataFrame, s2: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """Return a baseline state dictionary used across the split views."""

    ranges, discovered, global_min, global_max = _prepare_grade_data(s1, s2)
    bands = DEFAULT_BANDS.copy()
    classified = classify_ranges(
        ranges_df=ranges,
        bands=bands,
        policy="smallest_enclosing",
        boundary_inclusive=True,
        nearest_metric="ends_max",
        nearest_threshold=None,
    )

    return {
        "signature": _make_signature(s1, s2),
        "ranges": ranges,
        "discovered_grades": discovered,
        "global_min": global_min,
        "global_max": global_max,
        "bands": bands,
        "mode": "Defaults",
        "selected_bands": list(bands.keys()),
        "slider_min": float(global_min - 5),
        "slider_max": float(global_max + 5),
        "policy": "smallest_enclosing",
        "inclusive": True,
        "primary_metric": "ends_max",
        "nearest_threshold": None,
        "overlay_defaults": True,
        "ui_scale": 1.06,
        "axis_min": global_min,
        "axis_max": global_max,
        "tick_count": 13,
        "label_angle": 0,
        "grid_on": True,
        "rib_height": 160,
        "lines_height": 90,
        "classified": classified,
        "cm": confusion_table(classified),
    }


def _ensure_state(s1: pd.DataFrame, s2: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """Fetch shared state, resetting if inputs changed."""

    signature = _make_signature(s1, s2)
    state = st.session_state.get("grade_scale_state")
    if not state or state.get("signature") != signature:
        state = _default_grade_state(s1, s2)
        st.session_state["grade_scale_state"] = state
    return state


def _inject_css(zoom: float = 1.05):
    st.markdown(
        f"""
        <style>
        :root {{ --base-zoom:{zoom}; }}
        html, body, [data-testid="stAppViewContainer"] {{
            font-size: calc(16px * var(--base-zoom));
        }}
        .stTabs [role="tab"] {{ font-size: calc(0.95rem * var(--base-zoom)); }}
        [data-testid="stMetricValue"] {{ font-size: calc(1.4rem * var(--base-zoom)); }}
        .vega-embed .actions {{ visibility: hidden; }}
        .block-container {{ padding-top: 0.6rem; padding-bottom: 1.2rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _seed_bands_from_data(grade_names, ranges_df):
    bands = {}
    if ranges_df is None or ranges_df.empty:
        return bands
    gmin_all = float(np.nanmin(ranges_df["tmin_c"])) if not ranges_df["tmin_c"].isna().all() else -55.0
    gmax_all = float(np.nanmax(ranges_df["tmax_c"])) if not ranges_df["tmax_c"].isna().all() else 155.0
    for g in grade_names:
        sub = ranges_df[ranges_df["Grade_current"].astype(str) == str(g)]
        if not sub.empty:
            mn = float(sub["tmin_c"].min())
            mx = float(sub["tmax_c"].max())
            bands[str(g)] = (mn, mx)
        else:
            bands[str(g)] = (gmin_all, gmax_all)
    return bands


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _edge_fail_note(row, bands: dict):
    name = str(row.get("Grade_computed", ""))
    if name not in bands:
        return pd.Series({"FailEdge": "NoRef", "Overflow_C": np.nan})
    gmin, gmax = [float(x) for x in bands[name]]
    tmin = float(row["tmin_c"])
    tmax = float(row["tmax_c"])
    below = max(0.0, gmin - tmin)
    above = max(0.0, tmax - gmax)
    if below > 0 and above > 0:
        edge = "Both"
        mag = max(below, above)
    elif below > 0:
        edge = "BelowMin"
        mag = below
    elif above > 0:
        edge = "AboveMax"
        mag = above
    else:
        edge = "Inside"
        mag = 0.0
    return pd.Series({"FailEdge": edge, "Overflow_C": mag})


def render_grade_bands_page(s1: pd.DataFrame, s2: Optional[pd.DataFrame]):
    """Main band editor & classification controls."""

    if s1 is None or s1.empty:
        st.info("Upload or load Sheet1 to edit grade bands.")
        return

    state = _ensure_state(s1, s2)
    _inject_css(state.get("ui_scale", 1.06))

    ranges = state["ranges"]
    discovered_grades = state["discovered_grades"]
    global_min = state["global_min"]
    global_max = state["global_max"]

    st.markdown("### Band Editor")
    with st.expander("Edit / Filter grade bands (data-driven)", expanded=True):
        left, right = st.columns([3, 2])
        mode_options = ["Defaults", "Grades found in data"]
        mode_value = state.get("mode", "Defaults")
        mode_index = mode_options.index(mode_value) if mode_value in mode_options else 0
        data_seed_bands = _seed_bands_from_data(discovered_grades, ranges)
        with left:
            mode = st.radio("Seed bands from:", mode_options, horizontal=True, index=mode_index)
            if mode == "Defaults":
                initial_bands = DEFAULT_BANDS.copy()
                default_names = list(initial_bands.keys())
            else:
                initial_bands = data_seed_bands
                default_names = list(initial_bands.keys())

            default_selection = [
                b for b in state.get("selected_bands", default_names) if b in default_names
            ] or default_names
            selected_bands = st.multiselect(
                "Active standards to classify into",
                options=default_names,
                default=default_selection,
                help="Acts as a filter: only the selected bands are used.",
            )
            if not selected_bands:
                st.warning("Select at least one band to classify against.")
        with right:
            st.caption("Preset limits for sliders")
            slider_min = st.number_input(
                "Scale Min (°C)",
                value=float(state.get("slider_min", global_min - 5)),
                step=1.0,
            )
            slider_max = st.number_input(
                "Scale Max (°C)",
                value=float(state.get("slider_max", global_max + 5)),
                step=1.0,
            )
            if slider_min > slider_max:
                slider_min, slider_max = slider_max, slider_min

        overlay_defaults = st.toggle(
            "Overlay alternate bands (dashed)",
            value=state.get("overlay_defaults", True),
        )

        bands = {}
        prev_bands = state.get("bands", {})
        for name in selected_bands:
            base = initial_bands.get(name, (global_min, global_max))
            stored = prev_bands.get(name, base)
            lo_default = float(max(slider_min, stored[0]))
            hi_default = float(min(slider_max, stored[1]))
            lo, hi = st.slider(
                f"{name} (°C)",
                min_value=float(slider_min),
                max_value=float(slider_max),
                value=(lo_default, hi_default),
                step=1.0,
            )
            if lo > hi:
                lo, hi = hi, lo
            bands[name] = (lo, hi)

        colP, colB, colM, colT = st.columns([1, 1, 1, 1])
        policy_options = ["smallest_enclosing", "priority", "nearest"]
        metric_options = ["ends_max", "jaccard", "l1"]
        with colP:
            policy_value = state.get("policy", "smallest_enclosing")
            policy_index = policy_options.index(policy_value) if policy_value in policy_options else 0
            policy = st.selectbox("Classification policy", policy_options, index=policy_index)
        with colB:
            inclusive = st.toggle("Inclusive boundaries [a,b]", value=state.get("inclusive", True))
        with colM:
            metric_value = state.get("primary_metric", "ends_max")
            metric_index = metric_options.index(metric_value) if metric_value in metric_options else 0
            primary_metric = st.selectbox("Nearest metric", metric_options, index=metric_index)
        with colT:
            use_thresh_default = state.get("nearest_threshold") is not None
            use_thresh = st.toggle("Use distance threshold", value=use_thresh_default)
            if use_thresh:
                nearest_threshold = st.number_input(
                    "Threshold (primary)",
                    value=float(state.get("nearest_threshold", 0.15)),
                    step=0.01,
                )
            else:
                nearest_threshold = None

        band_tbl = (
            pd.DataFrame([{ "Band": k, "Min_C": v[0], "Max_C": v[1]} for k, v in bands.items()])
            .sort_values("Band")
            if bands
            else pd.DataFrame(columns=["Band", "Min_C", "Max_C"])
        )
        st.dataframe(band_tbl, use_container_width=True)

        band_csv = _df_to_csv_bytes(band_tbl) if not band_tbl.empty else b""
        st.download_button(
            "⬇️ Export current bands (CSV)",
            data=band_csv,
            file_name="bands_current.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=band_tbl.empty,
        )

    bands_to_use = bands if bands else DEFAULT_BANDS.copy()

    reference_bands = None
    if overlay_defaults:
        if mode == "Defaults":
            reference_source = data_seed_bands
        else:
            reference_source = DEFAULT_BANDS
        if reference_source is not None:
            if selected_bands:
                reference_bands = {
                    name: reference_source[name]
                    for name in selected_bands
                    if name in reference_source
                }
            if not reference_bands:
                reference_bands = reference_source.copy()

    classified = classify_ranges(
        ranges_df=ranges,
        bands=bands_to_use,
        policy=policy,
        boundary_inclusive=inclusive,
        nearest_metric=primary_metric,
        nearest_threshold=nearest_threshold,
    )
    cm = confusion_table(classified)

    state.update(
        {
            "mode": mode,
            "selected_bands": selected_bands,
            "slider_min": slider_min,
            "slider_max": slider_max,
            "overlay_defaults": overlay_defaults,
            "bands": bands_to_use,
            "policy": policy,
            "inclusive": inclusive,
            "primary_metric": primary_metric,
            "nearest_threshold": nearest_threshold,
            "classified": classified,
            "cm": cm,
            "reference_bands": reference_bands,
        }
    )
    st.session_state["grade_scale_state"] = state

    st.markdown("#### Classification preview")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total ranges", f"{len(classified):,}")
    with c2:
        st.metric("Bands used", len(state["bands"]))
    with c3:
        st.metric("Unique computed grades", classified["Grade_computed"].astype(str).nunique())

    st.dataframe(cm, use_container_width=True)


def render_overlay_page(s1: pd.DataFrame, s2: Optional[pd.DataFrame]):
    """Visual overlay of classified ranges on top of grade ribbons."""

    if s1 is None or s1.empty:
        st.info("Load Sheet1 to see overlay charts.")
        return

    state = _ensure_state(s1, s2)
    _inject_css(state.get("ui_scale", 1.06))

    bands = state.get("bands", DEFAULT_BANDS.copy())
    classified = state.get("classified", pd.DataFrame())
    reference_bands = state.get("reference_bands")
    global_min = state.get("global_min", -60.0)
    global_max = state.get("global_max", 160.0)

    st.markdown("### Display & Axis")
    with st.expander("Axis and UI controls", expanded=True):
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            ui_scale = st.slider(
                "UI scale (CSS)",
                0.9,
                1.3,
                float(state.get("ui_scale", 1.06)),
                0.01,
            )
        with c2:
            default_min = min(v[0] for v in bands.values()) if bands else global_min
            axis_min = st.number_input("X min (°C)", value=float(state.get("axis_min", default_min)))
        with c3:
            default_max = max(v[1] for v in bands.values()) if bands else global_max
            axis_max = st.number_input("X max (°C)", value=float(state.get("axis_max", default_max)))
        with c4:
            tick_count = st.number_input(
                "Tick count",
                min_value=5,
                max_value=25,
                value=int(state.get("tick_count", 13)),
                step=1,
            )
        with c5:
            angle_options = [0, 45, 90]
            angle_value = int(state.get("label_angle", 0))
            angle_index = angle_options.index(angle_value) if angle_value in angle_options else 0
            label_angle = st.selectbox("Label angle", angle_options, index=angle_index)

        c6, c7, c8 = st.columns(3)
        with c6:
            grid_on = st.toggle("Show grid", value=state.get("grid_on", True))
        with c7:
            rib_height = st.number_input(
                "Ribbons height",
                min_value=80,
                max_value=300,
                value=int(state.get("rib_height", 160)),
                step=10,
            )
        with c8:
            lines_height = st.number_input(
                "Overlay height",
                min_value=60,
                max_value=200,
                value=int(state.get("lines_height", 90)),
                step=5,
            )

    _inject_css(zoom=ui_scale)

    st.markdown("### Overlay controls")
    cc1, cc2, cc3 = st.columns([2, 1, 1])
    color_options = ["Grade_computed", "Grade_current", "Mismatch (Current→Computed)"]
    with cc1:
        color_value = state.get("overlay_color_by", "Grade_computed")
        color_index = color_options.index(color_value) if color_value in color_options else 0
        overlay_color_by = st.selectbox(
            "Color overlay by",
            color_options,
            index=color_index,
            help="Choose which field to color overlay lines by.",
        )
    with cc2:
        show_overlay = st.checkbox(
            "Draw computed ranges overlay",
            value=state.get("show_overlay", True),
        )
    with cc3:
        overlay_sample = st.number_input(
            "Overlay sample",
            value=int(state.get("overlay_sample", 600)),
            min_value=0,
            step=100,
        )

    overlay_defaults = state.get("overlay_defaults", True)
    overlay_df = None
    color_field = "Grade_computed"
    overlay_title = "Overlay"
    if show_overlay and not classified.empty:
        overlay_df = classified.copy()
        if overlay_color_by == "Mismatch (Current→Computed)":
            overlay_df["OverlayLabel"] = np.where(
                overlay_df["Grade_current"].astype(str) == overlay_df["Grade_computed"].astype(str),
                "Match",
                overlay_df["Grade_current"].astype(str)
                + "→"
                + overlay_df["Grade_computed"].astype(str),
            )
            color_field = "OverlayLabel"
            overlay_title = "Mismatch"
        else:
            color_field = overlay_color_by
            overlay_title = overlay_color_by.replace("_", " ")
        if overlay_sample and len(overlay_df) > overlay_sample:
            overlay_df = overlay_df.sample(overlay_sample, random_state=7)

    st.markdown("### Grade Scale")
    if overlay_defaults and reference_bands is None:
        reference_bands = DEFAULT_BANDS

    rib = grade_ribbons(
        bands,
        axis_min,
        axis_max,
        height=int(rib_height),
        overlay_df=(overlay_df if show_overlay else None),
        overlay_color_field=(color_field if show_overlay else "Grade_computed"),
        overlay_title=overlay_title if show_overlay else "Overlay",
        axis_title="Temperature (°C)",
        axis_tick_count=int(tick_count),
        axis_grid=bool(grid_on),
        axis_label_angle=int(label_angle),
        axis_label_size=12,
        axis_title_size=13,
        reference_bands=(reference_bands if overlay_defaults else None),
    )
    st.altair_chart(rib, use_container_width=True)

    if show_overlay and overlay_df is not None and not overlay_df.empty:
        cols = [
            "Value_E",
            "tmin_c",
            "tmax_c",
            "Grade_current",
            "Grade_computed",
            "Decision",
        ]
        export_overlay = overlay_df[cols + ([color_field] if color_field in overlay_df.columns else [])].copy()
        st.download_button(
            "⬇️ Export overlay ranges (CSV)",
            data=_df_to_csv_bytes(export_overlay),
            file_name="overlay_ranges.csv",
            mime="text/csv",
            use_container_width=True,
        )

    state.update(
        {
            "ui_scale": ui_scale,
            "axis_min": axis_min,
            "axis_max": axis_max,
            "tick_count": tick_count,
            "label_angle": label_angle,
            "grid_on": grid_on,
            "rib_height": rib_height,
            "lines_height": lines_height,
            "overlay_color_by": overlay_color_by,
            "show_overlay": show_overlay,
            "overlay_sample": overlay_sample,
        }
    )
    st.session_state["grade_scale_state"] = state


def render_highlight_page(s1: pd.DataFrame, s2: Optional[pd.DataFrame]):
    """Exception-to-standard insights and exports."""

    if s1 is None or s1.empty:
        st.info("Load Sheet1 to inspect highlights and outliers.")
        return

    state = _ensure_state(s1, s2)
    _inject_css(state.get("ui_scale", 1.06))

    classified = state.get("classified", pd.DataFrame())
    if classified.empty:
        st.warning("No classified ranges available. Configure bands in the first tab.")
        return

    ranges_df = classified.copy()

    st.markdown("#### Exceptions that map to a standard")
    fc1, fc2 = st.columns(2)
    with fc1:
        current_options = sorted(ranges_df["Grade_current"].astype(str).unique())
        filt_curr = st.multiselect(
            "Filter by current grade",
            options=current_options,
            default=state.get("filter_current"),
        )
    with fc2:
        computed_options = sorted(ranges_df["Grade_computed"].astype(str).unique())
        filt_comp = st.multiselect(
            "Filter by computed grade",
            options=computed_options,
            default=state.get("filter_computed"),
        )

    filtered = ranges_df.copy()
    if filt_curr:
        filtered = filtered[filtered["Grade_current"].astype(str).isin(filt_curr)]
    if filt_comp:
        filtered = filtered[filtered["Grade_computed"].astype(str).isin(filt_comp)]

    exc_to_std_filtered = filtered[
        (filtered["Grade_current"] == "Exception") & (filtered["Grade_computed"] != "Exception")
    ].copy()

    k1, k2 = st.columns([1, 3])
    with k1:
        st.metric("Exceptions that match a standard (within filters)", f"{len(exc_to_std_filtered):,}")

    counts_exc_to = (
        exc_to_std_filtered.groupby("Grade_computed")
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
    with k2:
        if counts_exc_to.empty:
            st.info("No Exception → Standard rows in the current filter.")
        else:
            st.altair_chart(
                bar_counts(
                    counts_exc_to,
                    x_col="Grade_computed",
                    y_col="rows",
                    title="Exception → Standard (counts)",
                ),
                use_container_width=True,
            )

    st.markdown("##### Highlight (timeline overlay)")
    h1, h2, h3 = st.columns([2, 2, 2])
    default_focus = state.get("focus_grades")
    focus_options = (
        counts_exc_to["Grade_computed"].astype(str).tolist()
        if not counts_exc_to.empty
        else computed_options
    )
    if default_focus is None:
        default_focus = focus_options.copy()
    elif isinstance(default_focus, str):
        default_focus = [default_focus]
    else:
        default_focus = [str(value) for value in default_focus]

    default_focus = [value for value in default_focus if value in focus_options]
    with h1:
        focus_grades = st.multiselect(
            "Highlight which computed grades?",
            options=focus_options,
            default=default_focus,
        )
    with h2:
        compare_against_all = st.toggle(
            "Compare highlight against ALL ranges",
            value=state.get("compare_against_all", True),
            help="If ON: base layer = ALL classified ranges. If OFF: base layer = filtered ranges.",
        )
    with h3:
        sample_base = st.number_input(
            "Base sample (rules)",
            value=int(state.get("sample_base", 1000)),
            min_value=0,
            step=100,
        )

    base_df = ranges_df if compare_against_all else filtered
    highlight_df = ranges_df[
        (ranges_df["Grade_current"] == "Exception") & (ranges_df["Grade_computed"] != "Exception")
    ].copy()
    if focus_grades:
        highlight_df = highlight_df[highlight_df["Grade_computed"].isin(focus_grades)]

    overlay = ranges_highlight(
        base_df,
        highlight_df,
        sample_base=int(sample_base),
        sample_highlight=800,
        height=int(state.get("lines_height", 90)),
        axis_title="Temperature (°C)",
        axis_tick_count=int(state.get("tick_count", 13)),
        axis_grid=bool(state.get("grid_on", True)),
        axis_label_angle=int(state.get("label_angle", 0)),
        axis_label_size=12,
        axis_title_size=13,
    )
    st.altair_chart(overlay, use_container_width=True)

    if not highlight_df.empty:
        st.download_button(
            "⬇️ Export Exception→Standard (highlight) (CSV)",
            data=_df_to_csv_bytes(
                highlight_df[
                    [
                        "Value_E",
                        "tmin_c",
                        "tmax_c",
                        "Grade_current",
                        "Grade_computed",
                        "Decision",
                    ]
                ]
            ),
            file_name="exc_to_std_highlight.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("### Outliers vs Default Standards & Export")
    ref_classified = classify_ranges(
        ranges_df=state["ranges"],
        bands=DEFAULT_BANDS,
        policy="smallest_enclosing",
        boundary_inclusive=state.get("inclusive", True),
        nearest_metric=state.get("primary_metric", "ends_max"),
        nearest_threshold=None,
    )
    outliers_ref = ref_classified[ref_classified["Decision"] != "Enclosed"].copy()
    if not outliers_ref.empty:
        edges = outliers_ref.apply(_edge_fail_note, axis=1, bands=DEFAULT_BANDS)
        outliers_ref = pd.concat([outliers_ref, edges], axis=1)
        st.metric("Outliers vs defaults", f"{len(outliers_ref):,}")
        st.dataframe(
            outliers_ref[
                [
                    "Value_E",
                    "tmin_c",
                    "tmax_c",
                    "Grade_computed",
                    "Decision",
                    "FailEdge",
                    "Overflow_C",
                ]
            ],
            use_container_width=True,
        )
        st.download_button(
            "⬇️ Export outliers vs defaults (CSV)",
            data=_df_to_csv_bytes(
                outliers_ref[
                    [
                        "Value_E",
                        "tmin_c",
                        "tmax_c",
                        "Grade_computed",
                        "Decision",
                        "FailEdge",
                        "Overflow_C",
                    ]
                ]
            ),
            file_name="outliers_vs_defaults.csv",
            mime="text/csv",
            use_container_width=True,
        )
    else:
        st.info("No outliers found against default standards.")

    st.markdown("#### Confusion table (Current vs Computed)")
    st.dataframe(state.get("cm", confusion_table(classified)), use_container_width=True)

    state.update(
        {
            "filter_current": filt_curr,
            "filter_computed": filt_comp,
            "focus_grades": focus_grades,
            "compare_against_all": compare_against_all,
            "sample_base": sample_base,
        }
    )
    st.session_state["grade_scale_state"] = state


def render_grade_scale_tab(s1: pd.DataFrame, s2: Optional[pd.DataFrame]):
    """Backward-compatible wrapper for the former single-tab layout."""

    st.info("The grade scale view is now split across dedicated tabs. Showing the full flow below.")
    render_grade_bands_page(s1, s2)
    st.markdown("---")
    render_overlay_page(s1, s2)
    st.markdown("---")
    render_highlight_page(s1, s2)
