"""
preprocess_motc.py

Reads three MOTC pickle files in a dated folder (YYYYMMDD):
- 汽車客貨運量概況_YYYYMMDD.pickle
- 高速公路計程收費通行量_YYYYMMDD.pickle
- 國際商港貨櫃裝卸量_YYYYMMDD.pickle

Each pickle is expected to be a dict with 'thead' and 'tbody' (both lists of strings).
This script prints a short preview (length and first 5 items) for each file's thead/tbody.
"""
import sys, os, pickle, json, re
import warnings
# suppress benign warnings early (before importing pandas) so import-time UserWarnings are filtered
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='Pandas requires version')
import pandas as pd


def inspect_pickle(path, max_items=5):
    out={'path': path, 'exists': False}
    if not os.path.exists(path):
        out['error'] = 'not found'
        return out
    out['exists'] = True
    with open(path, 'rb') as f:
        try:
            data = pickle.load(f)
        except Exception as e:
            out['error'] = f'pickle load error: {e}'
            return out
    thead = data.get('thead')
    tbody = data.get('tbody')
    out['thead_len'] = len(thead) if isinstance(thead, list) else None
    out['tbody_len'] = len(tbody) if isinstance(tbody, list) else None
    out['thead_sample'] = (thead[:max_items] if isinstance(thead, list) else thead)
    out['tbody_sample'] = (tbody[:max_items] if isinstance(tbody, list) else tbody)
    return out


def main(folder: str = None):
    # main processing
    if not folder:
        # if not provided, fall back to CLI arg or current directory
        folder = sys.argv[1] if len(sys.argv) > 1 else '.'
    if not os.path.isdir(folder):
        print('Folder not found:', folder)
        sys.exit(1)

    date_tag = os.path.basename(folder.rstrip(os.sep))

    # helper utilities
    def load_pickle(path):
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            return pickle.load(f)

    def split_keep_empty(s):
        if s is None:
            return ['']
        if not isinstance(s, str):
            s = str(s)
        return s.split(',')

    def pad_rows(rows, n):
        return [r + [''] * (n - len(r)) if len(r) < n else r[:n] for r in rows]

    def roc_to_yyyy_mm(s):
        # accept strings like '113年11月' or '114年01月' or '113年,11月'
        if s is None:
            return None
        t = str(s)
        m = re.search(r"(\d{2,4})[^\d]*(\d{1,2})", t)
        if not m:
            return None
        y = int(m.group(1))
        mo = int(m.group(2))
        if not (1 <= mo <= 12):
            return None
        yyyy = 1911 + y
        return f"{yyyy:04d}-{mo:02d}"

    def write_xlsx(df, out_path):
        try:
            df.to_excel(out_path, index=False)
            return True, None
        except Exception as e:
            # fallback via openpyxl writing rows (avoid pandas openpyxl version mismatch)
            try:
                from openpyxl import Workbook
                wb = Workbook()
                ws = wb.active
                ws.append(list(df.columns))
                for row in df.itertuples(index=False, name=None):
                    ws.append(list(row))
                wb.save(out_path)
                return True, None
            except Exception as e2:
                return False, f'pandas error: {e}; fallback error: {e2}'

    def clean_numeric_value(x):
        # remove whitespace and non-numeric characters except dot and minus
        try:
            s = str(x)
        except Exception:
            return ''
        s = s.strip()
        # remove commas and non-digit chars except . and -
        import re as _re
        s2 = _re.sub(r"[^0-9.\-]", "", s)
        return s2

    # processing: 汽車客貨運量概況
    fname1 = f'汽車客貨運量概況_{date_tag}.pickle'
    path1 = os.path.join(folder, fname1)
    print('\n=== 處理 汽車客貨運量概況 ===')
    data1 = load_pickle(path1)
    if data1 is None:
        print('file not found:', path1)
    else:
        thead = data1.get('thead', [])
        tbody = data1.get('tbody', [])
        # header: take first thead element and remove blank items
        raw_header = thead[0] if thead else []
        header = [h.strip() for h in raw_header if str(h).strip() != '']
        # build table from tbody
        rows = [r for r in tbody]
        maxc = max(len(header), max((len(r) for r in rows), default=0))
        # if header shorter than maxc, pad header with placeholders
        if len(header) < maxc:
            header = header + [f'col{i}' for i in range(len(header), maxc)]
        rows_p = pad_rows(rows, maxc)
        df1 = pd.DataFrame(rows_p, columns=header)
        # remove completely empty rows/cols
        df1.replace({'': pd.NA}, inplace=True)
        df1.dropna(axis=0, how='all', inplace=True)
        df1.dropna(axis=1, how='all', inplace=True)
        # rename first column to 日期 and convert ROC->YYYY-MM
        first_col = df1.columns[0]
        df1.rename(columns={first_col: '日期'}, inplace=True)
        df1['日期'] = df1['日期'].astype(str).apply(lambda x: roc_to_yyyy_mm(x))
        # Ensure 日期 column is stored as string type to avoid Excel/pandas auto-conversion
        df1['日期'] = df1['日期'].astype(str)
        # drop rows with invalid 日期 or where all non-date columns empty
        df1 = df1[df1['日期'].notna() & df1['日期'].ne('None')]
        non_date_cols = [c for c in df1.columns if c != '日期']
        df1.dropna(axis=0, how='all', subset=non_date_cols, inplace=True)
        # coerce numeric for other cols
        for c in non_date_cols:
            df1[c] = pd.to_numeric(df1[c].apply(lambda x: clean_numeric_value(x)), errors='coerce')
        out1 = os.path.join(folder, f'汽車客貨運量概況_{date_tag}(修正).xlsx')
        ok, err = write_xlsx(df1, out1)
        if ok:
            print('wrote', out1)
            try:
                print(f'OUTPUT: {out1}')
            except Exception:
                print('OUTPUT: ' + str(out1))
        else:
            print('write failed:', err)

    # processing: 高速公路計程收費通行量
    fname2 = f'高速公路計程收費通行量_{date_tag}.pickle'
    path2 = os.path.join(folder, fname2)
    print('\n=== 處理 高速公路計程收費通行量 ===')
    data2 = load_pickle(path2)
    if data2 is None:
        print('file not found:', path2)
    else:
        thead = data2.get('thead', [])
        tbody = data2.get('tbody', [])
        # header: take third thead element (index 2) and remove blanks, append suffix
        raw_header = thead[2] if len(thead) > 2 else (thead[0] if thead else [])
        header = [h.strip() for h in raw_header if str(h).strip() != '']
        header = [h + '_高速公路通行量' for h in header]
        rows = [r for r in tbody]
        maxc = max(len(header), max((len(r) for r in rows), default=0))
        if len(header) < maxc:
            header = header + [f'col{i}_高速公路通行量' for i in range(len(header), maxc)]
        rows_p = pad_rows(rows, maxc)
        df2 = pd.DataFrame(rows_p, columns=header)
        df2.replace({'': pd.NA}, inplace=True)
        df2.dropna(axis=0, how='all', inplace=True)
        df2.dropna(axis=1, how='all', inplace=True)
        # rename first col to 日期 and convert ROC->YYYY-MM
        first_col = df2.columns[0]
        df2.rename(columns={first_col: '日期'}, inplace=True)
        df2['日期'] = df2['日期'].astype(str).apply(lambda x: roc_to_yyyy_mm(x))
        # Ensure 日期 column is stored as string type to avoid Excel/pandas auto-conversion
        df2['日期'] = df2['日期'].astype(str)
        df2 = df2[df2['日期'].notna() & df2['日期'].ne('None')]
        non_date_cols = [c for c in df2.columns if c != '日期']
        df2.dropna(axis=0, how='all', subset=non_date_cols, inplace=True)
        for c in non_date_cols:
            df2[c] = pd.to_numeric(df2[c].apply(lambda x: clean_numeric_value(x)), errors='coerce')
        out2 = os.path.join(folder, f'高速公路計程收費通行量_{date_tag}(修正).xlsx')
        ok, err = write_xlsx(df2, out2)
        if ok:
            print('wrote', out2)
            try:
                print(f'OUTPUT: {out2}')
            except Exception:
                print('OUTPUT: ' + str(out2))
        else:
            print('write failed:', err)

    # 國際商港貨櫃裝卸量 處理
    print('\n=== 處理 國際商港貨櫃裝卸量 ===')
    fname3 = f'國際商港貨櫃裝卸量_{date_tag}.pickle'
    path3 = os.path.join(folder, fname3)
    data3 = load_pickle(path3)
    if data3 is None:
        print('file not found:', path3)
    else:
        thead = data3.get('thead', [])
        tbody = data3.get('tbody', [])
        # use thead element 2 as base header (index 2)
        raw_header = thead[2] if len(thead) > 2 else (thead[0] if thead else [])
        if not isinstance(raw_header, list):
            try:
                raw_header = list(raw_header)
            except Exception:
                raw_header = [str(raw_header)]
        header = [str(h).strip() for h in raw_header]

        def prefix_for_idx(i):
            if i in (1, 2):
                return '總計_'
            if i in (3, 4):
                return '實櫃_'
            if i in (5, 6):
                return '空櫃_'
            return ''

        header_prefixed = [f"{prefix_for_idx(i)}{h}_貨櫃裝卸量" for i, h in enumerate(header)]

        # create DataFrame directly from rows and then assign columns
        df3 = pd.DataFrame(tbody)
        # ncols = df3.shape[1]
        # if len(header_prefixed) < ncols:
        #     header_prefixed += [f'col{i}_貨櫃裝卸量' for i in range(len(header_prefixed), ncols)]
        # elif len(header_prefixed) > ncols:
        #     header_prefixed = header_prefixed[:ncols]
        df3.columns = header_prefixed

        # remove empty rows/cols
        df3.replace({'': pd.NA}, inplace=True)
        df3.dropna(axis=0, how='all', inplace=True)
        df3.dropna(axis=1, how='all', inplace=True)

        # rename first column to 日期 and convert ROC->YYYY-MM
        first_col = df3.columns[0]
        df3.rename(columns={first_col: '日期'}, inplace=True)
        df3['日期'] = df3['日期'].astype(str).apply(lambda x: roc_to_yyyy_mm(x))
        # Ensure 日期 column is stored as string type to avoid Excel/pandas auto-conversion
        df3['日期'] = df3['日期'].astype(str)
        df3 = df3[df3['日期'].notna() & df3['日期'].ne('None')]

        non_date_cols = [c for c in df3.columns if c != '日期']
        df3.dropna(axis=0, how='all', subset=non_date_cols, inplace=True)

        out3 = os.path.join(folder, f'國際商港貨櫃裝卸量_{date_tag}(修正).xlsx')
        ok, err = write_xlsx(df3, out3)
        if ok:
            print('wrote', out3)
            try:
                print(f'OUTPUT: {out3}')
            except Exception:
                print('OUTPUT: ' + str(out3))
        else:
            print('write failed:', err)


if __name__ == '__main__':
    main()


def main_wrapper(date_tag: str = None):
    """Compatibility wrapper for run_all_preprocess: accepts optional date_tag folder"""
    folder = date_tag if date_tag else None
    main(folder)
