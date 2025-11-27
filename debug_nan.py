import pandas as pd
import numpy as np

data = [
    {"amount": 100.0},
    {"amount": float('nan')}
]

df = pd.DataFrame(data)
print("Original DF:")
print(df)
print(df.dtypes)

df = df.where(pd.notnull(df), None)
print("\nAfter where:")
print(df)
print(df.dtypes)

result = df.to_dict(orient='records')
print("\nResult:")
print(result)

val = result[1]['amount']
print(f"\nValue: {val}, Type: {type(val)}")
print(f"Is None? {val is None}")
print(f"Is NaN? {np.isnan(val) if isinstance(val, float) else 'N/A'}")
