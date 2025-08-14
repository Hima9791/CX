#!/usr/bin/env python3
"""
Key-scoped RequestedSeries matcher (difflib-only, Arrow-friendly)

• Compares each input row's "RequestedSeries" against the MASTER's "RequestedSeries".
• Critically: the candidate pool is ALWAYS restricted by your key
  (VariantID, ManufacturerName, Manufacturer Part Number, Category, Family).
  - Exact key match first
  - Else case-insensitive CONTAINS on any key column
  - Else difflib fuzzy on concatenated key (>= min_key_ratio)
  - If still nothing → NO search (we do NOT scan the whole master)
• Returns a flat table (strings/ints/floats only) to avoid Streamlit Arrow errors.

Use:
    from python_script import compare_requested_series

    df = compare_requested_series(
        comparison_path="input.xlsx",           # CSV/XLSX/Parquet/URL
        master_path=MASTER_URL,                 # optional override
        top_n=2,
        key_cols=None,                          # or custom list
        min_key_ratio=0.82
    )
"""

from __future__ import annotations
import io
import sys
from typing import List, Tuple
from difflib import SequenceMatcher

import pandas as pd

# Default master in your GitHub (raw). Change if needed.
MASTER_URL = "https://raw.githubusercontent.com/AbdallahHesham44/Series_2/main/MasterSeriesHistory.xlsx"

KEY_COLS_DEFAULT = [
    "VariantID",
    "ManufacturerName",
    "Manufacturer Part Number",
    "Category",
    "Family",
]


# ───────────────────────── Helpers ─────────────────────────

def _norm(s):
    if pd.isna(s):
        return ""
    return str(s).strip()

def _norm_cf(s):
    return _norm(s).casefold()

def _concat_key_values(row_like, cols: List[str]) -> str:
    parts = [_norm(row_like.get(c, "")) for c in cols]
    return " | ".join(parts)

def _concat_key_values_cf(row_like, cols: List[str]) -> str:
    parts = [_norm_cf(row_like.get(c, "")) for c in cols]
    return " | ".join(parts)

def _difflib_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def _arrow_friendly(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if c in ("MatchRank", "KeyMatchCount"):
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")
        elif c == "SimilarityPct":
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).astype("float64")
        else:
            if out[c].dtype == "object":
                out[c] = out[c].astype("string")
    return out


def _read_any(path_or_url: str) -> pd.DataFrame:
    """Load CSV/XLSX/Parquet from local path or URL (GitHub raw ok)."""
    p = str(path_or_url)
    lower = p.lower()

    if lower.endswith(".parquet") or lower.endswith(".pq"):
        return pd.read_parquet(p)

    if lower.endswith(".csv") or ".csv?" in lower:
        return pd.read_csv(p)

    # Default to Excel for everything else (handles .xlsx/.xls and many URLs)
    try:
        return pd.read_excel(p, engine="openpyxl")
    except Exception:
        # Try CSV fallback if the URL actually serves CSV
        try:
            return pd.read_csv(p)
        except Exception:
            raise


def _scope_candidates_by_key(row: pd.Series,
                             df_master: pd.DataFrame,
                             key_cols: List[str],
                             min_key_ratio: float = 0.82) -> Tuple[pd.DataFrame, str]:
    """
    Returns (scoped_master, scope_mode) with scope_mode ∈ {"exact","contains","fuzzy","none"}.

    Strategy (never whole master):
      1) exact equality on provided key columns (trimmed)
      2) else case-insensitive CONTAINS on any key col
      3) else difflib fuzzy on concatenated key (>= min_key_ratio)
      4) else → empty (no search)
    """
    cols = [c for c in key_cols if c in df_master.columns and c in row.index]
    if not cols:
        return df_master.iloc[0:0], "none"

    # 1) EXACT
    mask = pd.Series(True, index=df_master.index)
    for c in cols:
        val = _norm(row[c])
        mask &= (df_master[c].astype(str).str.strip() == val)
    if mask.any():
        return df_master.loc[mask], "exact"

    # Prepare lowercase/casefold caches
    for c in cols:
        cfcol = f"__cf_{c}"
        if cfcol not in df_master.columns:
            df_master[cfcol] = df_master[c].astype(str).map(_norm_cf)

    # 2) CONTAINS (any key)
    any_mask = pd.Series(False, index=df_master.index)
    for c in cols:
        q = _norm_cf(row[c])
        if not q:
            continue
        any_mask |= df_master[f"__cf_{c}"].str.contains(q, na=False)
    if any_mask.any():
        return df_master.loc[any_mask], "contains"

    # 3) FUZZY on concatenated key
    concat_col = "__key_cf_concat"
    if concat_col not in df_master.columns:
        df_master[concat_col] = df_master.apply(lambda r: _concat_key_values_cf(r, cols), axis=1)

    q_concat = _concat_key_values_cf(row, cols)
    if q_concat:
        scores = df_master[concat_col].map(lambda s: _difflib_ratio(q_concat, s))
        scoped = df_master.loc[scores >= min_key_ratio]
        if not scoped.empty:
            return scoped, "fuzzy"

    # 4) none
    return df_master.iloc[0:0], "none"


# ───────────────────────── Core API ─────────────────────────

def compare_requested_series(
    comparison_path: str,
    master_path: str = MASTER_URL,
    top_n: int = 2,
    key_cols: List[str] | None = None,
    min_key_ratio: float = 0.82,
) -> pd.DataFrame:
    """
    DIFflib-only compare, strictly scoped by your key.

    Output columns (all Arrow-friendly):
      VariantID, ManufacturerName, Manufacturer Part Number, Category, Family,
      RequestedSeries, KeyScope, KeyMatchCount, MatchRank, MatchSeries, SimilarityPct
    """
    key_cols = key_cols or KEY_COLS_DEFAULT

    df_master = _read_any(master_path)
    df_comp   = _read_any(comparison_path)

    if "RequestedSeries" not in df_master.columns:
        raise ValueError("Master must contain 'RequestedSeries'.")
    if "RequestedSeries" not in df_comp.columns:
        raise ValueError("Input must contain 'RequestedSeries'.")

    df_master = df_master.copy()
    if "__req_cf" not in df_master.columns:
        df_master["__req_cf"] = df_master["RequestedSeries"].astype(str).map(_norm_cf)

    rows_out = []
    for _, r in df_comp.iterrows():
        scoped, scope_mode = _scope_candidates_by_key(r, df_master, key_cols, min_key_ratio=min_key_ratio)
        key_count = int(scoped.shape[0])

        query_raw = r.get("RequestedSeries", "")
        query_cf  = _norm_cf(query_raw)

        if scoped.empty or not query_cf:
            base = {c: _norm(r.get(c, "")) for c in key_cols}
            base.update({
                "RequestedSeries": _norm(query_raw),
                "KeyScope": scope_mode,
                "KeyMatchCount": key_count,
                "MatchRank": 1,
                "MatchSeries": "",
                "SimilarityPct": 0.0,
            })
            rows_out.append(base)
            continue

        cand = scoped[["RequestedSeries", "__req_cf"]].dropna().drop_duplicates()
        cand["_score"] = cand["__req_cf"].map(lambda s: _difflib_ratio(query_cf, s))
        cand = cand.sort_values("_score", ascending=False).head(max(1, top_n))

        rank = 1
        for _, m in cand.iterrows():
            base = {c: _norm(r.get(c, "")) for c in key_cols}
            base.update({
                "RequestedSeries": _norm(query_raw),
                "KeyScope": scope_mode,
                "KeyMatchCount": key_count,
                "MatchRank": int(rank),
                "MatchSeries": _norm(m["RequestedSeries"]),
                "SimilarityPct": round(float(m["_score"]) * 100.0, 2),
            })
            rows_out.append(base)
            rank += 1

    return _arrow_friendly(pd.DataFrame(rows_out))


# ───────────────────────── Optional CLI ─────────────────────────

def _main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Key-scoped RequestedSeries matcher (difflib-only)")
    ap.add_argument("--input", required=True, help="Path/URL to comparison file (CSV/XLSX/Parquet)")
    ap.add_argument("--master", default=MASTER_URL, help="Path/URL to master (default: repo xlsx)")
    ap.add_argument("--top-n", type=int, default=2, help="Top matches per row")
    ap.add_argument("--min-key-ratio", type=float, default=0.82, help="Fuzzy key cut-off (0..1)")
    ap.add_argument("--out", help="Optional CSV to write results")
    args = ap.parse_args(argv)

    df = compare_requested_series(
        comparison_path=args.input,
        master_path=args.master,
        top_n=args.top_n,
        min_key_ratio=args.min_key_ratio,
    )

    if args.out:
        df.to_csv(args.out, index=False)
        print(f"wrote: {args.out}  rows={len(df)}")
    else:
        print(df.head(20).to_string())

if __name__ == "__main__":
    _main()
