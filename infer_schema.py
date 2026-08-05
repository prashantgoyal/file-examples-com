import sys
import os
import pandas as pd
import numpy as np
from dateutil.parser import parse

os.makedirs('ddls', exist_ok=True)

def map_dtype(series):
    s_nonnull = series.dropna().astype(str)
    if len(s_nonnull) == 0:
        return "NVARCHAR(4000)"
    # integer
    try:
        s_num = pd.to_numeric(s_nonnull, errors='raise')
        if np.issubdtype(s_num.dtype, np.integer):
            return "BIGINT"
        if np.issubdtype(s_num.dtype, np.floating):
            return "FLOAT"
    except Exception:
        pass
    # datetime
    try:
        parsed = s_nonnull.map(lambda x: bool(parse(x)))
        if parsed.all():
            return "DATETIME2"
    except Exception:
        pass
    # boolean
    lowered = s_nonnull.str.lower()
    if lowered.isin(["true","false","0","1"]).all():
        return "BIT"
    # string length
    maxlen = s_nonnull.map(len).max()
    if maxlen <= 4000:
        return f"NVARCHAR({maxlen if maxlen>0 else 100})"
    return "NVARCHAR(MAX)"


def infer_table(csv_path, table_name=None, nrows=20000):
    df = pd.read_csv(csv_path, nrows=nrows, dtype=str, keep_default_na=False, na_values=[''])
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
    cols = []
    for c in df.columns:
        s = df[c].replace('', pd.NA)
        dtype = map_dtype(s)
        cols.append((c, dtype))
    ddl = f"CREATE SCHEMA IF NOT EXISTS Source_data_0308;\n\n"
    ddl += f"CREATE TABLE Source_data_0308.{table_name} (\n"
    ddl += ",\n".join([f"  [{name}] {dtype}" for name,dtype in cols])
    ddl += "\n);\n"
    outpath = os.path.join('ddls', f"{table_name}.sql")
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(ddl)
    return outpath, ddl


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python infer_schema.py file1.csv [file2.csv ...]")
        sys.exit(1)
    for path in sys.argv[1:]:
        if not os.path.isfile(path):
            print(f"File not found: {path}")
            continue
        out, ddl = infer_table(path)
        print(f"Wrote: {out}\n")
        print(ddl)
