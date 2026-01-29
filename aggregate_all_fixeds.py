"""
aggregate_all_fixeds.py

Provides aggregate_fixeds(folder) which:
- finds all files in `folder` containing '(修正)' and ending with .xls/.xlsx
- reads each into a DataFrame, ensures the first column (日期) is normalized and cast to string
- merges all tables on 日期 using outer join (dates as rows, other columns preserved with source prefixes)
- writes the combined table to 總經指標_<YYYYMMDD>.xlsx in the same folder

This is designed to be called from run_all_preprocess.py after preprocessors finish.
"""
from typing import Optional
import os, re
import pandas as pd

DATE_RE = re.compile(r"^\d{4}-\d{2}$")


def safe_read_excel(path: str) -> Optional[pd.DataFrame]:
    """Use pandas.read_excel (openpyxl engine) only; return None on failure.

    Attempts:
    1) pd.read_excel(..., sheet_name=0, engine='openpyxl', dtype=str)
    2) pd.read_excel(..., sheet_name=None, engine='openpyxl', dtype=str) -> take first non-empty sheet
    Logs exception messages to help diagnose files that pandas can't parse.
    """
    import traceback
    try:
        return pd.read_excel(path, sheet_name=0, header=0, engine='openpyxl', dtype=str)
    except Exception as e:
        print(f'pd.read_excel(sheet 0) failed for {path}: {e}')
        # try reading all sheets and pick the first non-empty DataFrame
        try:
            all_sheets = pd.read_excel(path, sheet_name=None, engine='openpyxl', dtype=str)
            for name, df in all_sheets.items():
                if df is not None and df.shape[0] > 0 and df.shape[1] > 0:
                    print(f'using sheet "{name}" from {path}')
                    return df
            print(f'no non-empty sheets found in {path}')
            return None
        except Exception as e2:
            print(f'pd.read_excel(sheet=None) also failed for {path}: {e2}')
            traceback.print_exc()
            return None


def ensure_date_string_firstcol(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the first column is named '日期' and its values are strings in YYYY-MM or as-is strings."""
    if df is None or df.shape[1] == 0:
        return df
    # promote numeric column names if needed
    first_col = df.columns[0]
    # rename to 日期
    df = df.copy()
    df.rename(columns={first_col: '日期'}, inplace=True)
    # coerce values to string
    df['日期'] = df['日期'].astype(str).map(lambda x: x.strip() if x is not None else '')
    return df


def normalize_date_str(s: str) -> Optional[str]:
    """Normalize various date strings to YYYY-MM.

    Handles:
    - 'YYYY-MM' or 'YYYY/MM' or 'YYYY.MM'
    - 'YYY年MM月' (ROC or AD). If ROC (year length 3), convert to AD by adding 1911.
    - Strings that already look like 'YYYYMM' -> 'YYYY-MM'.
    Returns normalized 'YYYY-MM' or None if cannot parse.
    """
    if s is None:
        return None
    s = str(s).strip()
    if s == '':
        return None
    # direct YYYY-MM or YYYY/MM or YYYY.MM
    import re
    m = re.match(r'^(\d{4})[\-/.](\d{1,2})$', s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}"
    # compact YYYYMM
    m = re.match(r'^(\d{6})$', s)
    if m:
        y = int(s[:4])
        mo = int(s[4:6])
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}"
    # ROC like 110年07月 or 民國110年7月
    m = re.match(r'^(\d{2,4})\s*[年\-/.](\d{1,2})', s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        # assume ROC if year < 1912
        if y < 1912:
            y = y + 1911
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}"
    # try to find year and month anywhere
    m = re.search(r'(\d{4}).{0,2}(\d{1,2})', s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}"
    return None


def aggregate_fixeds(folder: str) -> Optional[str]:
    folder = os.fspath(folder)
    if not os.path.isdir(folder):
        print('folder not found:', folder)
        return None

    files = [f for f in os.listdir(folder) if '(修正)' in f and f.lower().endswith(('.xls', '.xlsx'))]
    if not files:
        print('no (修正) files found in', folder)
        return None

    merged = None

    def first_nonnull(s):
        # return first non-empty, non-null value from series, else None
        for v in s:
            if pd.isna(v):
                continue
            vs = str(v).strip()
            if vs == '' or vs.lower() in ('nan', 'none'):
                continue
            return v
        return None

    for fn in sorted(files):
        p = os.path.join(folder, fn)
        print('reading', p)
        df = safe_read_excel(p)
        if df is None:
            print('failed to read', p)
            continue
        df = ensure_date_string_firstcol(df)
        if df is None or df.shape[0] == 0:
            print('empty after read', p)
            continue

        # normalize date column and filter
        df['norm_date'] = df['日期'].map(lambda x: normalize_date_str(str(x)))
        # keep only rows with norm_date >= 2025-08 (i.e., 2025-08 and later)
        df = df[df['norm_date'].notnull()]
        df = df[df['norm_date'] >= '2025-08']
        if df.shape[0] == 0:
            print('no rows after date normalization/filter for', p)
            continue

        value_cols = [c for c in df.columns if c not in ('日期', 'norm_date')]
        if not value_cols:
            print('no value columns in', p)
            continue

        # group by normalized date, taking first non-null per column
        grouped = df.groupby('norm_date', as_index=False).agg(lambda s: first_nonnull(s))

        # prefix columns with source name to avoid collisions (do not rename norm_date)
        src = os.path.splitext(fn)[0]
        rename_map = {c: f"{src}_{c}" for c in grouped.columns if c != 'norm_date'}
        grouped.rename(columns=rename_map, inplace=True)

        if merged is None:
            merged = grouped
        else:
            merged = pd.merge(merged, grouped, on='norm_date', how='outer')

    if merged is None or merged.shape[0] == 0:
        print('no tables loaded')
        return None

    # ensure the final date column is named '日期' and sorted
    merged.rename(columns={'norm_date': '日期'}, inplace=True)
    merged = merged.sort_values(by='日期')

    date_tag = os.path.basename(folder.rstrip(os.sep))
    out_name = f'總經指標_{date_tag}.xlsx'
    out_path = os.path.join(folder, out_name)
    try:
        merged.to_excel(out_path, index=False)
        print('wrote', out_path)
        return out_path
    except Exception as e:
        print('write failed:', e)
        return None
