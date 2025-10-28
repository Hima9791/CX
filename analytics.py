# =============================
# analytics.py  (UPDATED)
# =============================
from typing import Dict, Tuple, Optional, List
import pandas as pd
import numpy as np
from intervals import nearest_grade, jaccard_distance

DEFAULT_BANDS = {
    "Commercial": (0.0, 70.0),
    "Industrial": (-40.0, 85.0),
    "Automotive": (-40.0, 125.0),
    "Military/Aerospace": (-55.0, 155.0),
}

def nearest_grade_with_direction(
    df: pd.DataFrame,
    tmin_col: str,
    tmax_col: str,
    bands: Dict[str, Tuple[float, float]],
    out_prefix: str = "Nearest",
) -> pd.DataFrame:
    """Attach nearest grade information and directional gaps to ``df``.

    For each row, pick the band with the smallest L1 distance between range
    endpoints and return directional gaps plus pass/fail metadata. Additional
    columns are appended to the input dataframe using the ``out_prefix``.
    """
    if df.empty:
        return df.assign(**{
            f"{out_prefix}Grade": [],
            f"{out_prefix}L1": [],
            f"{out_prefix}LowGap_C": [],
            f"{out_prefix}HighGap_C": [],
            f"{out_prefix}Pass": [],
            f"{out_prefix}Miss_C": [],
            f"{out_prefix}FailingEdge": [],
        })

    tmin = pd.to_numeric(df[tmin_col], errors="coerce")
    tmax = pd.to_numeric(df[tmax_col], errors="coerce")

    best_grade = np.full(len(df), "", dtype=object)
    best_L1 = np.full(len(df), np.inf, dtype=float)
    best_low = np.full(len(df), np.nan, dtype=float)
    best_high = np.full(len(df), np.nan, dtype=float)

    for name, (gmin, gmax) in bands.items():
        low_gap = tmin - gmin
        high_gap = gmax - tmax
        L1 = (tmin - gmin).abs() + (tmax - gmax).abs()

        improve = L1 < best_L1
        best_L1[improve] = L1[improve]
        best_low[improve] = low_gap[improve]
        best_high[improve] = high_gap[improve]
        best_grade[improve] = name

    passes = (best_low >= 0) & (best_high >= 0)
    miss = np.maximum(0, np.maximum(-best_low, -best_high))

    edge = np.where(
        ~passes & (best_low < 0) & (best_high >= 0), "LowOnly",
        np.where(
            ~passes & (best_high < 0) & (best_low >= 0), "HighOnly",
            np.where(
                ~passes & (best_low < 0) & (best_high < 0), "Both", "—"
            ),
        ),
    )

    out = pd.DataFrame(
        {
            f"{out_prefix}Grade": best_grade,
            f"{out_prefix}L1": best_L1,
            f"{out_prefix}LowGap_C": best_low,
            f"{out_prefix}HighGap_C": best_high,
            f"{out_prefix}Pass": passes,
            f"{out_prefix}Miss_C": miss,
            f"{out_prefix}FailingEdge": edge,
        },
        index=df.index,
    )

    return pd.concat([df, out], axis=1)

def classify_ranges(ranges_df: pd.DataFrame,
                    bands: Dict[str, Tuple[float,float]] = None,
                    policy: str = "smallest_enclosing",
                    boundary_inclusive: bool = True,
                    nearest_metric: str = "ends_max",
                    nearest_threshold: Optional[float] = None):
    if bands is None or not len(bands):
        bands = DEFAULT_BANDS

    def contains(a,b,x,y):
        if boundary_inclusive:
            return (x >= a) and (y <= b)
        else:
            return (x > a) and (y < b)

    order = list(bands.keys())

    out = ranges_df.copy()
    if "Grade_current" not in out.columns:
        out["Grade_current"] = "Unknown"

    out["Grade_computed"] = "Exception"
    out["Decision"] = "Exception"
    out["Distance"] = np.nan

    # map UI label -> nearest_grade primary key
    metric_key = {
        "jaccard": "jaccard",
        "l1": "l1",
        "linf": "linf",
        "ends": "linf",
        "ends_max": "linf",
    }.get(str(nearest_metric).lower(), "jaccard")

    for i, row in out.iterrows():
        tmin, tmax = float(row["tmin_c"]), float(row["tmax_c"])

        if policy in ("smallest_enclosing","priority"):
            encl = [(name, bnds) for name,bnds in bands.items() if contains(bnds[0], bnds[1], tmin, tmax)]
            if encl:
                if policy == "smallest_enclosing":
                    sel = sorted(encl, key=lambda kv: (kv[1][1]-kv[1][0], kv[0]))[0][0]
                else:
                    sel = order[0]
                    for name in order:
                        if any(name==e[0] for e in encl):
                            sel = name
                            break
                out.at[i,"Grade_computed"] = sel
                out.at[i,"Decision"] = "Enclosed"
                dJ = jaccard_distance(tmin,tmax, *bands[sel])
                out.at[i,"Distance"] = dJ
                continue

        best, _ = nearest_grade(tmin, tmax, bands, primary=metric_key)
        if best:
            name, dists = best
            dist = dists[metric_key]
            if nearest_threshold is not None and dist > float(nearest_threshold):
                out.at[i,"Grade_computed"] = "Exception"
                out.at[i,"Decision"] = "Too far"
                out.at[i,"Distance"] = dist
            else:
                out.at[i,"Grade_computed"] = name
                out.at[i,"Decision"] = "Nearest"
                out.at[i,"Distance"] = dist
        else:
            out.at[i,"Grade_computed"] = "Exception"
            out.at[i,"Decision"] = "No match"

    return out

def confusion_table(df: pd.DataFrame):
    cols = [c for c in ["Grade_current","Grade_computed"] if c in df.columns]
    if len(cols) < 2:
        return pd.DataFrame(columns=["Grade_current","Grade_computed","rows"])
    tbl = (df.groupby(["Grade_current","Grade_computed"]).size()
             .reset_index(name="rows")
             .sort_values("rows", ascending=False))
    return tbl

def sheet2_pivots(sheet2: pd.DataFrame, metric: str = "rows"):
    df = sheet2.copy()
    for col in ["CompanyName","Product","Grade"]:
        if col not in df.columns:
            df[col] = "Unknown"
    if metric == "rows":
        out = (df.groupby(["CompanyName","Product","Grade"])
                 .size().reset_index(name="rows"))
    else:
        if metric not in df.columns:
            # fallback gracefully to row counts
            out = (df.groupby(["CompanyName","Product","Grade"])
                     .size().reset_index(name="rows"))
        else:
            out = (df.groupby(["CompanyName","Product","Grade"])[metric]
                     .sum().reset_index(name="rows"))
    return out

# ---- NEW: Top‑N and Outliers helpers ----
def top_k_table(piv_df: pd.DataFrame, group_cols: List[str], value_col: str = "rows", k: int = 10, ascending: bool = False) -> pd.DataFrame:
    """Return top‑k rows after grouping by `group_cols` and summing `value_col`.
    If multiple group columns are given, they are treated as a composite key.
    """
    cols = [c for c in group_cols if c in piv_df.columns]
    if not cols:
        return pd.DataFrame()
    agg = piv_df.groupby(cols)[value_col].sum().reset_index()
    agg = agg.sort_values(value_col, ascending=ascending).head(int(k))
    return agg

def detect_outliers(piv_df: pd.DataFrame, group_cols: List[str], value_col: str = "rows", method: str = "Z", z_threshold: float = 2.5, iqr_k: float = 1.5) -> pd.DataFrame:
    """Flag outliers across aggregated groups using Z‑score or IQR.
    Returns table sorted by extremeness with helper columns.
    """
    cols = [c for c in group_cols if c in piv_df.columns]
    if not cols:
        return pd.DataFrame()
    agg = piv_df.groupby(cols)[value_col].sum().reset_index()

    x = agg[value_col].astype(float)
    if method.upper().startswith("Z"):
        mu = x.mean(); sd = x.std(ddof=0) or 1.0
        z = (x - mu) / sd
        agg["zscore"] = z
        mask = z.abs() >= float(z_threshold)
        out = agg.loc[mask].copy().sort_values("zscore", key=lambda s: s.abs(), ascending=False)
        return out
    else:
        q1, q3 = x.quantile(0.25), x.quantile(0.75)
        iqr = q3 - q1
        low = q1 - float(iqr_k)*iqr
        high = q3 + float(iqr_k)*iqr
        agg["low"], agg["high"] = low, high
        mask = (x < low) | (x > high)
        out = agg.loc[mask].copy().assign(
            distance=lambda d: np.where(d[value_col] < low, low - d[value_col], d[value_col] - high)
        ).sort_values("distance", ascending=False)
        return out
