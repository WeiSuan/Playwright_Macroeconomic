import os
import pickle
import re
from typing import Any
import pandas as pd


def process_folder(folder: str):
    prefix = '外銷訂單_'
    # prefer exact pattern: 外銷訂單_YYYYMMDD.pickle
    found = None
    candidates = []
    for fn in os.listdir(folder):
        if re.match(rf'^{re.escape(prefix)}\d{{8}}\.pickle$', fn):
            candidates.append(fn)
    if candidates:
        # pick latest by name (lexicographic on YYYYMMDD)
        fn = sorted(candidates)[-1]
        found = os.path.join(folder, fn)
    else:
        # fallback: accept any file starting with prefix and .pickle/.pkl
        for fn in os.listdir(folder):
            if fn.startswith(prefix) and (fn.endswith('.pickle') or fn.endswith('.pkl')):
                candidates.append(fn)
        if candidates:
            # prefer .pickle over .pkl and pick latest
            candidates = sorted(candidates)
            # try to pick a .pickle if present
            pickle_candidates = [c for c in candidates if c.endswith('.pickle')]
            fn = (pickle_candidates or candidates)[-1]
            found = os.path.join(folder, fn)
    if not found:
        return
    # perform conversion and write output (no printing)
    _convert_pickle_to_excel(found, folder)
    # note: silent operation — file is written (or exception raised)


def _roc_to_ad_year(roc_year_str: str) -> int:
    # accept strings like '113年' or '113' -> return 2024 for 113
    s = str(roc_year_str).strip()
    m = re.search(r'(\d{2,4})', s)
    if not m:
        raise ValueError('invalid roc year: ' + s)
    y = int(m.group(1))
    return y + 1911


def _convert_pickle_to_excel(pickle_path: str, folder: str):
    # reads pickle, parses thead/tbody per rules, merges first two cols into ROC date, propagates year, converts to YYYY-MM
    with open(pickle_path, 'rb') as f:
        data = pickle.load(f)

    thead = data.get('thead')
    if isinstance(thead, list) and len(thead) == 1 and isinstance(thead[0], str):
        header_line = thead[0]
    elif isinstance(thead, list):
        header_line = ','.join(str(x) for x in thead)
    else:
        header_line = str(thead)
    raw_headers = [h for h in header_line.split(',')]
    # trim whitespace but keep empty names as ''
    raw_headers = [h.strip() for h in raw_headers]
    # make headers unique by appending suffix for duplicates
    headers = []
    seen = {}
    for h in raw_headers:
        key = h
        if key in seen:
            seen[key] += 1
            newh = f"{h}__dup{seen[key]}"
        else:
            seen[key] = 1
            newh = h
        headers.append(newh)

    tbody = data.get('tbody', [])
    rows = []
    for r in tbody:
        s = r if isinstance(r, str) else str(r)
        fields = [f.strip() for f in s.split(',')]
        rows.append(fields)

    maxcols = max(len(headers), max((len(r) for r in rows), default=0))
    # extend headers if needed
    if len(headers) < maxcols:
        for i in range(len(headers), maxcols):
            headers.append(f'UNNAMED_{i+1}')

    # normalize rows
    norm_rows = []
    for fields in rows:
        if len(fields) < maxcols:
            fields = fields + [''] * (maxcols - len(fields))
        else:
            fields = fields[:maxcols]
        norm_rows.append(fields)

    # build DataFrame
    df = pd.DataFrame(norm_rows, columns=headers)

    # combine first two positional columns into ROC date and propagate year when month-only rows appear
    if df.shape[1] >= 2:
        y_series = df.iloc[:, 0].astype(str).fillna('').tolist()
        m_series = df.iloc[:, 1].astype(str).fillna('').tolist()
        combined = []
        last_roc_year = None
        for yv, mv in zip(y_series, m_series):
            yv_s = yv.strip(); mv_s = mv.strip()
            if re.search(r'\d{2,4}年?', yv_s):
                last_roc_year = re.search(r'(\d{2,4})', yv_s).group(1)
                roc = f"{last_roc_year}年{mv_s}月" if mv_s else f"{last_roc_year}年"
            else:
                if yv_s == '' and mv_s:
                    if last_roc_year is None:
                        roc = mv_s
                    else:
                        roc = f"{last_roc_year}年{mv_s}月" if not re.search(r'年', mv_s) else f"{last_roc_year}{mv_s}"
                else:
                    roc = (yv_s + mv_s).strip()
            combined.append(roc)

        def roc_to_yyyy_mm(s):
            s = s.strip()
            m = re.search(r'(\d{2,4})', s)
            if not m:
                return ''
            # find month after the year match
            rest = s[m.end():]
            mm = re.search(r'(\d{1,2})', rest)
            roc_y = int(m.group(1))
            ad_y = roc_y + 1911
            if mm:
                month = int(mm.group(1))
                return f"{ad_y:04d}-{month:02d}"
            return f"{ad_y:04d}-01"

        df.insert(0, '日期', [roc_to_yyyy_mm(x) for x in combined])
        # drop the original first two positional columns
        cols_to_drop = list(df.columns[1:3])
        df = df.drop(columns=cols_to_drop)

    # write to xlsx named 外銷訂單_YYYYMMDD(修正).xlsx where YYYYMMDD taken from pickle filename
    bn = os.path.basename(pickle_path)
    m = re.match(r'外銷訂單_(\d{8})\.pickle$', bn)
    date_tag = m.group(1) if m else ''
    out_name = f"外銷訂單_{date_tag}(修正).xlsx" if date_tag else f"{bn}(修正).xlsx"
    out_path = os.path.join(folder, out_name)
    # remove auto-generated duplicate-empty columns like '__dup2'
    dup_cols = [c for c in df.columns if '__dup' in str(c) and df[c].replace('', pd.NA).isna().all()]
    if dup_cols:
        df = df.drop(columns=dup_cols)

    # write (overwrite allowed)
    with pd.ExcelWriter(out_path, engine='openpyxl') as w:
        df.to_excel(w, index=False)



if __name__ == '__main__':
    # compatible CLI wrapper: prefer a main(date_tag) if present, else call process_folder(folder)
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else '.'
    # try a main() style if exists
    try:
        _main = globals().get('main', None)
        if callable(_main):
            try:
                _main(folder)
            except TypeError:
                _main()
        else:
            process_folder(folder)
    except Exception:
        process_folder(folder)


def main(date_tag: str = None):
    """Entrypoint for run_all_preprocess: accepts a date_tag (YYYYMMDD) or defaults to current folder '.'"""
    if not date_tag:
        process_folder('.')
    else:
        # date_tag refers to folder name under workspace
        process_folder(date_tag)
