import pandas as pd

# === CONFIG ===
raw_file = "us30_raw.csv"       # your input CSV
output_file = "us30_ready.csv"  # backtest-ready CSV

# Possible column name variations
col_map = {
    "Date": ["Date", "Time", "Timestamp"],
    "Open": ["Open", "OpenPrice", "O"],
    "High": ["High", "HighPrice", "H"],
    "Low": ["Low", "LowPrice", "L"],
    "Close": ["Close", "ClosePrice", "C"],
    "Vol": ["Volume", "Vol", "V"]
}

# Load CSV
df = pd.read_csv(raw_file)

# Standardize column names
new_cols = {}
for std_col, variants in col_map.items():
    for var in variants:
        if var in df.columns:
            new_cols[var] = std_col
            break
df.rename(columns=new_cols, inplace=True)

# Ensure all required columns exist
required_cols = ["Date", "Open", "High", "Low", "Close", "Vol"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns in CSV: {missing}")

# Convert Date to standard format
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
if df['Date'].isnull().any():
    raise ValueError("Some dates could not be parsed. Check your input CSV.")
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# Remove duplicates
df = df.drop_duplicates(subset=['Date', 'Open', 'High', 'Low', 'Close'], keep='first')

# Remove incomplete bars
df = df.dropna(subset=required_cols)

# Sort by Date ascending
df = df.sort_values('Date').reset_index(drop=True)

# Save ready CSV
df.to_csv(output_file, index=False)
print(f"Backtest-ready CSV saved as {output_file} ({len(df)} rows)")
