from typing import Tuple, Dict
import pandas as pd
import numpy as np

NAME_MAP = {
    "Min. Main value - P0/M0":"min_val",
    "Min. Main unit - P0/M0":"min_unit",
    "Max. Main value - P0/M0":"max_val",
    "Max. Main unit - P0/M0":"max_unit",
}

def _to_float(x):
    try:
        return float(str(x).replace(',','').strip())
    except:
        return np.nan

def _unit_norm(u: str) -> str:
    u = str(u).strip().lower()
    if "c" in u: return "C"
    if "f" in u: return "F"
    return ""

def _f_to_c(v: float) -> float:
    try:
        return (float(v) - 32.0) * (5.0/9.0)
    except:
        return np.nan

def _to_celsius(val: float, unit: str) -> float:
    if pd.isna(val): return np.nan
    if unit == "F": return _f_to_c(val)
    return val

def reconstruct_ranges(sheet1: pd.DataFrame) -> pd.DataFrame:
    df = sheet1[["Value_E","Grade","New_FeatureCode","Value_processed"]].copy()
    df = df[df["New_FeatureCode"].isin(NAME_MAP.keys())].copy()
    df["feat"] = df["New_FeatureCode"].map(NAME_MAP)

    pivot = (df.pivot_table(index="Value_E", columns="feat", values="Value_processed", aggfunc="first")
               .reset_index())

    grade_mode = (df.groupby("Value_E")["Grade"]
                    .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
                    .reset_index()
                    .rename(columns={"Grade":"Grade_current"}))

    ranges = pivot.merge(grade_mode, on="Value_E", how="left")

    ranges["min_val_num"] = ranges["min_val"].apply(_to_float)
    ranges["max_val_num"] = ranges["max_val"].apply(_to_float)

    ranges["min_u"] = ranges["min_unit"].apply(_unit_norm)
    ranges["max_u"] = ranges["max_unit"].apply(_unit_norm)

    ranges["tmin_c"] = ranges.apply(lambda r: _to_celsius(r["min_val_num"], r["min_u"]), axis=1)
    ranges["tmax_c"] = ranges.apply(lambda r: _to_celsius(r["max_val_num"], r["max_u"]), axis=1)

    ranges = ranges[(~ranges["tmin_c"].isna()) & (~ranges["tmax_c"].isna()) & (ranges["tmin_c"] <= ranges["tmax_c"])].copy()
    return ranges[["Value_E","tmin_c","tmax_c","Grade_current"]].drop_duplicates()