
import os
import pandas as pd
import streamlit as st

from data_loader import load_excel

# New, separated views
from ui_grade_scale import (
    render_grade_bands_page,
    render_overlay_page,
    render_highlight_page,
)
from ui_sheet2 import render_sheet2_tab

st.set_page_config(page_title="Grade Analysis", page_icon="📊", layout="wide")
st.title("📊 Grade Analysis — Clean, Separated Views")

# -----------------------------
# 0) FILE INPUT
# -----------------------------
colL, colR = st.columns([2,1])
with colL:
    uploaded = st.file_uploader("Upload Grade__.xlsx (two sheets: Sheet1, Sheet2)", type=["xlsx"], accept_multiple_files=False)
with colR:
    use_repo_file = st.toggle("Use local file Grade__.xlsx if no upload", value=True)

s1 = s2 = None
if uploaded is not None:
    try:
        s1, s2 = load_excel(uploaded)
        st.success("Loaded from uploaded file")
    except Exception as e:
        st.error(f"Could not read uploaded file: {e}")
elif use_repo_file and os.path.exists("Grade__.xlsx"):
    try:
        s1, s2 = load_excel("Grade__.xlsx")
        st.success("Loaded local Grade__.xlsx")
    except Exception as e:
        st.error(f"Could not read local Grade__.xlsx: {e}")
else:
    st.info("Please upload Grade__.xlsx or place it next to app.py.")

# Overview
if s1 is not None and s2 is not None:
    st.subheader("Data Overview")
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Sheet1 rows", f"{len(s1):,}")
    with c2: st.metric("Sheet2 rows", f"{len(s2):,}")
    with c3: st.metric("Distinct Grades S1", s1['Grade'].nunique() if 'Grade' in s1.columns else 0)
    with c4: st.metric("Distinct Grades S2", s2['Grade'].nunique() if 'Grade' in s2.columns else 0)

    # Separated main tabs
    t1, t2, t3, t4 = st.tabs([
        "Grade Bands",
        "Overlay (sampled)",
        "Highlight",
        "Sheet2 Explorer"
    ])

    with t1:
        render_grade_bands_page(s1, s2)
    with t2:
        render_overlay_page(s1, s2)
    with t3:
        render_highlight_page(s1, s2)
    with t4:
        render_sheet2_tab(s2)
