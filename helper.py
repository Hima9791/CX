import pandas as pd
import requests
from difflib import SequenceMatcher
import io
import os

from io import BytesIO

def load_file(path_or_url):
    """Load file from local path or URL."""
    if os.path.exists(path_or_url):  # ✅ Local file
        if path_or_url.endswith((".xlsx", ".xls")):
            return pd.read_excel(path_or_url, engine="openpyxl")
        elif path_or_url.endswith(".csv"):
            return pd.read_csv(path_or_url)
        else:
            raise ValueError("Unsupported file type")

    # Otherwise assume it's a URL
    response = requests.get(path_or_url)
    response.raise_for_status()

    if path_or_url.endswith((".xlsx", ".xls")):
        return pd.read_excel(BytesIO(response.content), engine="openpyxl")
    elif path_or_url.endswith(".csv"):
        return pd.read_csv(BytesIO(response.content))
    else:
        raise ValueError("Unsupported file type")

def similarity_ratio(a, b):
    """Return similarity ratio as percentage with 2 decimal places."""
    return round(SequenceMatcher(None, a, b).ratio() * 100, 2)

def normalize_series(series):
    """Normalize series name for comparison."""
    if pd.isna(series):
        return ""
    return str(series).strip().upper()
