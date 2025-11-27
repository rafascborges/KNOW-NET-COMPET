import pandas as pd
from pathlib import Path

file_path = Path('data/contracts_2009_2024.parquet')
if file_path.exists():
    df = pd.read_parquet(file_path)
    print("Columns:", df.columns.tolist())
    print("First row:", df.iloc[0].to_dict())
else:
    print("File not found")
