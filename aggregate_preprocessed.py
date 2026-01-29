"""
aggregate_preprocessed.py

Usage:
    python3 aggregate_preprocessed.py /path/to/YYYYMMDD

This script finds all files in the given folder that contain '(修正)' in their
filename and attempts to read them (Excel .xls/.xlsx). It then detects the
column that contains dates in the format YYYY-MM (prefers a column where many
values match the pattern), melts each table into long form with columns:

    日期, 資料來源, 欄位名稱, 數值

and writes the combined DataFrame to

    總經指標彙整_<YYYYMMDD>.xlsx

If a file cannot be read, it is skipped with a printed warning.
"""

import os
import sys
import re
from typing import Optional
import pandas as pd

DATE_RE = re.compile(r"^\d{4}-\d{2}$")


def normalize_date_str(s: str) -> str:
    """Normalize date-like strings to YYYY-MM when possible.
    Handles:
      - 民國年格式 like '113年11月' -> '2024-11'
      - 'YYYY/MM' or 'YYYY/MM/DD' -> 'YYYY-MM'
      - 'YYY-MM' where YYY is ROC year -> convert if detected
      - already 'YYYY-MM' -> unchanged
    Returns normalized string or original stripped string if unable.
    """
    if s is None:
        return ''
    t = str(s).strip()
    if t == '':
        return ''
    # already YYYY-MM
    if DATE_RE.match(t):
        return t
    # match YYYY/MM or YYYY/MM/DD
    m = re.match(r"^(\d{4})[\-/](\d{1,2})(?:[\-/]\d{1,2})?$", t)
    if m:
        y = int(m.group(1)); mm = int(m.group(2))
        return f"{y:04d}-{mm:02d}"
    # match ROC '113年11月' or '113-11' etc
    m2 = re.search(r"(\d{2,4})\s*年\s*(\d{1,2})\s*月", t)
    if m2:
        roc = int(m2.group(1)); mm = int(m2.group(2))
        ad = 1911 + roc
        return f"{ad:04d}-{mm:02d}"
    # match '113-11' (ROC-year dash month) heuristics: if year < 1900 assume ROC
    m3 = re.match(r"^(\d{2,4})[\-/](\d{1,2})$", t)
    if m3:
        y = int(m3.group(1)); mm = int(m3.group(2))
        if y < 1900:
            y = 1911 + y
        return f"{y:04d}-{mm:02d}"
    return t


def detect_date_column(df: pd.DataFrame) -> Optional[str]:
    """Return the column name that appears to be the date column (YYYY-MM).
    Preference order:
      1) any column where >= 0.6 of non-null values match YYYY-MM
      2) first column
    """
    cols = list(df.columns)
    if not cols:
        return None
    best = None
    best_frac = 0.0
    for c in cols:
        col = df[c]
        # if duplicate column names, df[c] may be a DataFrame; coerce to a single Series
        if isinstance(col, pd.DataFrame):
            # pick the first non-empty column within
            try:
                ser = col.iloc[:, 0].dropna().astype(str)
            except Exception:
                ser = col.stack().astype(str)
        else:
            ser = col.dropna().astype(str)
        if ser.empty:
            continue
        # try normalizing samples first
        normed = ser.map(lambda x: normalize_date_str(x))
        matched = normed.str.match(DATE_RE)
        frac = matched.sum() / len(ser)
        if frac > best_frac:
            best_frac = frac
            best = c
    if best_frac >= 0.6:
        return best
    # fallback: choose column with most non-null values
    counts = {c: df[c].dropna().shape[0] for c in cols}
    sorted_cols = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    if sorted_cols and sorted_cols[0][1] > 0:
        return sorted_cols[0][0]
    return cols[0]


def read_table(path: str) -> Optional[pd.DataFrame]:
    """Try to read an excel file into DataFrame. Return None on failure."""
    try:
        df = pd.read_excel(path, sheet_name=0, header=0)
        return df
    except Exception as e:
        print(f"read failed with pandas for {path}: {e}")
        # try fallback: read as binary xls via xlrd or via openpyxl handled by pandas
        try:
            df = pd.read_excel(path, sheet_name=0, header=0, engine='xlrd')
            return df
        except Exception:
            try:
                df = pd.read_excel(path, sheet_name=0, header=0, engine='openpyxl')
                return df
            except Exception as e2:
                print(f"fallback read failed for {path}: {e2}")
                # final fallback: parse xlsx as zip/xml
                df2 = read_xlsx_zip(path)
                if df2 is not None:
                    return df2
                return None


def read_xlsx_zip(path: str) -> Optional[pd.DataFrame]:
    """Lightweight fallback: parse xlsx (zip) and return first worksheet as DataFrame.
    This avoids openpyxl dependency by using ElementTree on the sheet xml.
    """
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(path, 'r') as z:
            namelist = z.namelist()
            ss = []
            if 'xl/sharedStrings.xml' in namelist:
                root = ET.fromstring(z.read('xl/sharedStrings.xml'))
                for si in root.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si'):
                    texts = []
                    for t in si.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t'):
                        texts.append(t.text or '')
                    ss.append(''.join(texts))

            # find first worksheet xml
            sheet_name = None
            for n in namelist:
                if n.startswith('xl/worksheets/sheet') and n.endswith('.xml'):
                    sheet_name = n
                    break
            if sheet_name is None:
                return None
            root = ET.fromstring(z.read(sheet_name))
            ns = {'d': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            rows = []
            for row in root.findall('.//d:row', ns):
                cells = []
                for c in row.findall('d:c', ns):
                    t = c.get('t')
                    v = c.find('d:v', ns)
                    if v is None:
                        cells.append('')
                    else:
                        val = v.text or ''
                        if t == 's':
                            try:
                                idx = int(val)
                                cells.append(ss[idx] if idx < len(ss) else val)
                            except Exception:
                                cells.append(val)
                        else:
                            cells.append(val)
                rows.append(cells)
            maxc = max((len(r) for r in rows), default=0)
            rows = [r + [''] * (maxc - len(r)) for r in rows]
            return pd.DataFrame(rows)
    except Exception as e:
        print('zip/xml fallback failed for', path, e)
        return None


def aggregate_folder(folder: str) -> Optional[str]:
    """Aggregate all '(修正)' files in folder into a long-form Excel file.

    Returns path to written file or None if nothing written.
    """
    folder = os.fspath(folder)
    if not os.path.isdir(folder):
        print("folder not found:", folder)
        return None

    files = [f for f in os.listdir(folder) if '(修正)' in f and f.lower().endswith(('.xls', '.xlsx'))]
    if not files:
        print('no (修正) files found in', folder)
        return None

    parts = []
    for fn in sorted(files):
        path = os.path.join(folder, fn)
        print('reading', path)
        df = read_table(path)
        if df is None:
            print('skipping', fn)
            continue
        # If read_xlsx_zip returned rows without header, scan the first few rows
        # to find a candidate header row (many non-numeric or date-like cells)
        try:
            if all(isinstance(c, (int, float)) or str(c).isdigit() for c in df.columns):
                header_row_idx = None
                max_rows = min(6, len(df))
                for r in range(0, max_rows):
                    row = df.iloc[r].astype(str).str.strip().fillna('')
                    if len(row) == 0:
                        continue
                    # count date-like cells after normalization
                    date_like = 0
                    text_like = 0
                    for v in row:
                        if not v:
                            continue
                        nv = normalize_date_str(v)
                        if DATE_RE.match(nv):
                            date_like += 1
                        if re.search(r"\D", v):
                            text_like += 1
                    # prefer rows with many date-like cells or many text-like cells
                    if len(row) > 0 and (date_like / len(row) >= 0.4 or text_like / len(row) >= 0.4):
                        header_row_idx = r
                        break
                if header_row_idx is not None:
                    hdr = df.iloc[header_row_idx].astype(str).str.strip().tolist()
                    df = df[(header_row_idx + 1):].copy()
                    df.columns = hdr
                    df = df.reset_index(drop=True)
        except Exception:
            pass

        # detect date column
        date_col = detect_date_column(df)
        # if detected date column is mostly empty, but many column NAMES look like dates,
        # treat this sheet as 'columns are months' layout: transpose and re-detect
        try:
            non_null_dates = df[date_col].dropna().astype(str).str.strip()
            non_null_frac = 0.0 if non_null_dates.empty else (non_null_dates.str.len() > 0).sum() / len(df)
        except Exception:
            non_null_frac = 0.0
        colname_date_like = 0
        for cn in df.columns:
            if DATE_RE.match(str(cn).strip()) or re.match(r"^\d{4}[\-/]\d{1,2}$", str(cn).strip()):
                colname_date_like += 1
        # if date column is mostly empty and few column names look like dates,
        # check if any early ROW looks like a month header (many cells 1..12)
        month_row_idx = None
        if non_null_frac < 0.1 and colname_date_like < 2:
            max_rows = min(6, len(df))
            for r in range(0, max_rows):
                row = df.iloc[r].astype(str).str.strip().fillna('')
                if row.empty:
                    continue
                month_like = 0
                total = 0
                for v in row:
                    if not v:
                        continue
                    total += 1
                    mv = re.match(r"^0*(\d{1,2})$", v)
                    if mv:
                        iv = int(mv.group(1))
                        if 1 <= iv <= 12:
                            month_like += 1
                if total and (month_like / total) >= 0.4 and month_like >= 3:
                    month_row_idx = r
                    break
        if month_row_idx is not None:
            # promote that row as header, then transpose so months become rows
            hdr = df.iloc[month_row_idx].astype(str).str.strip().tolist()
            df = df[(month_row_idx + 1):].copy()
            df.columns = hdr
            df = df.reset_index(drop=True)
            df_t = df.copy()
            df_t.columns = [str(x) for x in range(df_t.shape[1])]
            df_t = df_t.T.reset_index(drop=True)
            # promote first row as header if textual
            hdr2 = df_t.iloc[0].astype(str).str.strip().tolist()
            df_t = df_t[1:].copy()
            df_t.columns = hdr2
            df = df_t.reset_index(drop=True)
            date_col = detect_date_column(df)
        elif non_null_frac < 0.1 and colname_date_like >= 2:
            # transpose: rows become columns; reset index
            df_t = df.copy()
            df_t.columns = [str(x) for x in range(df_t.shape[1])]
            df_t = df_t.T.reset_index(drop=True)
            # promote first row as header if looks like textual
            hdr = df_t.iloc[0].astype(str).str.strip().tolist()
            df_t = df_t[1:].copy()
            df_t.columns = hdr
            df = df_t.reset_index(drop=True)
            date_col = detect_date_column(df)
        else:
            # attempt combining first 2 or 3 rows as a composite header then transpose
            for combine_n in (2, 3):
                if len(df) <= combine_n:
                    continue
                rows = [df.iloc[i].astype(str).str.strip().fillna('') for i in range(combine_n)]
                combined = []
                for col_idx in range(df.shape[1]):
                    parts = [rows[r].iat[col_idx] for r in range(combine_n) if rows[r].iat[col_idx]]
                    combined.append(' '.join(parts))
                # if many combined headers look like dates or month labels, use it
                date_like = sum(1 for v in combined if DATE_RE.match(v) or re.search(r"\d{1,2}月|年", v))
                if date_like >= 2:
                    df2 = df[combine_n:].copy()
                    df2.columns = combined
                    df2 = df2.reset_index(drop=True)
                    # transpose
                    df_t = df2.copy()
                    df_t.columns = [str(x) for x in range(df_t.shape[1])]
                    df_t = df_t.T.reset_index(drop=True)
                    hdr2 = df_t.iloc[0].astype(str).str.strip().tolist()
                    df_t = df_t[1:].copy()
                    df_t.columns = hdr2
                    df = df_t.reset_index(drop=True)
                    date_col = detect_date_column(df)
                    break
        if date_col is None:
            print('no date column detected for', fn, '; skipping')
            continue

        # melt other columns
        value_vars = [c for c in df.columns if c != date_col]
        if not value_vars:
            print('no value columns for', fn, '; skipping')
            continue

        melt = df[[date_col] + value_vars].copy()
        # normalize date column to YYYY-MM strings (attempt to parse if necessary)
        melt = melt.reset_index(drop=True)
        # coerce date values into a new '日期' column (string)
        melt['日期'] = melt[date_col].astype(str).map(lambda x: normalize_date_str(x))
        # ensure value_vars are strings and unique
        safe_value_vars = [str(c) for c in value_vars]
        # create source column
        src = os.path.splitext(fn)[0]
        # melt using the explicit '日期' column
        long = melt.melt(id_vars=['日期'], value_vars=safe_value_vars, var_name='欄位名稱', value_name='數值')
        long.insert(1, '資料來源', src)
        parts.append(long)

    if not parts:
        print('no data collected')
        return None

    combined = pd.concat(parts, ignore_index=True, sort=False)
    # optional: drop rows where 數值 is null or empty string? keep for now
    # write to excel
    date_tag = os.path.basename(folder.rstrip(os.sep))
    out_name = f'總經指標彙整_{date_tag}.xlsx'
    out_path = os.path.join(folder, out_name)
    try:
        combined.to_excel(out_path, index=False)
        print('wrote', out_path)
        return out_path
    except Exception as e:
        print('write failed:', e)
        return None


if __name__ == '__main__':
    folder = sys.argv[1] if len(sys.argv) > 1 else '.'
    aggregate_folder(folder)
