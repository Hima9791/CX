# Grade Analysis App (Streamlit)

This app analyzes temperature grade ranges and inventory/qualification data from Grade__.xlsx with two sheets:

- Sheet1: Value_E, Grade, Code, New_FeatureCode, Value_processed
- Sheet2: Product, CompanyName, Code, Grade, Partscount (PL-Comp), Codecount, Military Qualified, Automotive Qualified

It includes:
- Grade Scale editor (Commercial/Industrial/Automotive/Military-Aero) with live classification policies.
- Nearest-Grade Mapper with Jaccard and L1 endpoint distances.
- Confusion matrix (current vs computed).
- Company x Product pivots and Qualification overlays (from Sheet2).
- Exportable review queues (mismatches, exceptions).

## Run Locally

pip install -r requirements.txt
streamlit run app.py

## Data

Place your Excel in the same folder as the app and name it Grade__.xlsx.
Or use the file uploader inside the app.