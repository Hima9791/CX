# charts.py — axis-aware, cleaner visuals
import altair as alt
import pandas as pd

def _axis(
    title: str = "Temperature (°C)",
    tick_count: int = 11,
    grid: bool = True,
    label_angle: int = 0,
    label_font_size: int = 12,
    title_font_size: int = 13,
):
    return alt.Axis(
        title=title,
        tickCount=tick_count,
        grid=grid,
        labelAngle=label_angle,
        labelFontSize=label_font_size,
        titleFontSize=title_font_size,
    )

def bar_counts(df: pd.DataFrame, x_col: str, y_col: str = "rows", title: str = ""):
    if df is None or df.empty:
        return alt.Chart(pd.DataFrame({x_col: [], y_col: []})).mark_bar()
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x_col}:N", sort="-y", title=x_col),
            y=alt.Y(f"{y_col}:Q", title=y_col),
            tooltip=[x_col, y_col],
        )
        .properties(height=220, title=title)
    )
def grade_ribbons(
    bands: dict,
    x_min: float,
    x_max: float,
    height: int = 160,
    overlay_df: pd.DataFrame | None = None,
    overlay_color_field: str = "Grade_computed",
    overlay_title: str = "Overlay",
    axis_title: str = "Temperature (°C)",
    axis_tick_count: int = 13,
    axis_grid: bool = True,
    axis_label_angle: int = 0,
    axis_label_size: int = 12,
    axis_title_size: int = 13,
    # NEW: dashed outlines for a reference standard (e.g., DEFAULT_BANDS)
    reference_bands: dict | None = None,
):
    rows = [{"band": str(name), "start": float(a), "end": float(b)} for name, (a, b) in bands.items()]
    base = pd.DataFrame(rows)
    domain = [float(x_min), float(x_max)]
    band_order = list(bands.keys())

    ax = _axis(axis_title, axis_tick_count, axis_grid, axis_label_angle, axis_label_size, axis_title_size)

    rects = (
        alt.Chart(base, height=height)
        .mark_rect(opacity=0.45)
        .encode(
            x=alt.X("start:Q", scale=alt.Scale(domain=domain, nice=False), axis=ax),
            x2="end:Q",
            y=alt.Y("band:N", sort=band_order, title=None),
        )
    )

    # NEW: dashed outlines for reference bands (drawn only where names align)
    if reference_bands:
        ref_rows = []
        for name in band_order:
            if name in reference_bands:
                a, b = reference_bands[name]
                ref_rows.append({"band": str(name), "start": float(a), "end": float(b)})
        if ref_rows:
            ref_df = pd.DataFrame(ref_rows)
            rects_ref = (
                alt.Chart(ref_df, height=height)
                .mark_rect(fillOpacity=0, strokeDash=[6, 3], strokeWidth=2, stroke="black")
                .encode(
                    x=alt.X("start:Q", scale=alt.Scale(domain=domain, nice=False), axis=ax),
                    x2="end:Q",
                    y=alt.Y("band:N", sort=band_order, title=None),
                )
            )
            rects = rects + rects_ref

    labels = (
        alt.Chart(base)
        .mark_text(dy=0, baseline="middle")
        .encode(
            x=alt.X("start:Q", scale=alt.Scale(domain=domain, nice=False), axis=alt.Axis(labels=False, ticks=False, domain=False)),
            x2="end:Q",
            y=alt.Y("band:N", sort=band_order, title=None),
            text="band:N",
        )
    )

    chart = rects + labels

    if overlay_df is not None and not overlay_df.empty:
        ov = overlay_df.copy()
        ov[overlay_color_field] = ov[overlay_color_field].astype(str)
        overlay_chart = (
            alt.Chart(ov, height=height)
            .mark_rule(strokeWidth=3, opacity=0.9)
            .encode(
                x=alt.X("tmin_c:Q", scale=alt.Scale(domain=domain, nice=False), axis=ax),
                x2="tmax_c:Q",
                y=alt.Y(f"{overlay_color_field}:N", sort=band_order, title=None),
                color=alt.Color(f"{overlay_color_field}:N", legend=alt.Legend(title=overlay_title)),
                tooltip=["Value_E", "tmin_c", "tmax_c", "Grade_current", "Grade_computed", "Decision"],
            )
        )
        chart = rects + overlay_chart + labels

    return chart
def ranges_highlight(
    base_df: pd.DataFrame,
    highlight_df: pd.DataFrame,
    sample_base: int = 800,
    sample_highlight: int = 800,
    height: int = 90,
    axis_title: str = "Temperature (°C)",
    axis_tick_count: int = 13,
    axis_grid: bool = True,
    axis_label_angle: int = 0,
    axis_label_size: int = 12,
    axis_title_size: int = 13,
):
    base = base_df.copy()
    hi = highlight_df.copy()

    if sample_base and len(base) > sample_base:
        base = base.sample(sample_base, random_state=1)
    if sample_highlight and len(hi) > sample_highlight:
        hi = hi.sample(sample_highlight, random_state=1)

    ax = _axis(axis_title, axis_tick_count, axis_grid, axis_label_angle, axis_label_size, axis_title_size)

    base_layer = (
        alt.Chart(base)
        .mark_rule(opacity=0.22, strokeWidth=1.5)
        .encode(
            x=alt.X("tmin_c:Q", title=axis_title, axis=ax),
            x2="tmax_c:Q",
            y=alt.value(0),
            color=alt.value("#AAB0B6"),
            tooltip=["Value_E", "tmin_c", "tmax_c", "Grade_current", "Grade_computed", "Decision"],
        )
        .properties(height=height)
    )

    if hi.empty:
        return base_layer

    hi_layer = (
        alt.Chart(hi)
        .mark_rule(strokeWidth=3)
        .encode(
            x=alt.X("tmin_c:Q", title=axis_title, axis=ax),
            x2="tmax_c:Q",
            y=alt.value(0),
            color=alt.Color("Grade_computed:N", legend=alt.Legend(title="Highlight (Exc → Std)")),
            tooltip=["Value_E", "tmin_c", "tmax_c", "Grade_current", "Grade_computed", "Decision"],
        )
        .properties(height=height)
    )

    return base_layer + hi_layer
