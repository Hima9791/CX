import io
import pandas as pd

def load_excel(uploaded_file_or_path):
    if uploaded_file_or_path is None:
        raise FileNotFoundError("No file provided.")
    if hasattr(uploaded_file_or_path, "read"):
        data = uploaded_file_or_path.read()
        xls = pd.ExcelFile(io.BytesIO(data))
    else:
        xls = pd.ExcelFile(uploaded_file_or_path)
    sheet1 = xls.parse("Sheet1")
    sheet2 = xls.parse("Sheet2")
    return sheet1, sheet2