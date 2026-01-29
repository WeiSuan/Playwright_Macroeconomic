import os
import sys
import re
from pathlib import Path
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET

# preprocess_moi.py
# usage: python preprocess_moi.py /path/to/YYYYMMDD

TARGET_FILES = [
    '4.5-辦理建物所有權登記',
    '8.1-核發建築物建造執照按用途別分',
    '8.5-核發建築物使用執照按用途別分'
]

# mapping from Chinese month markers to month number
CHINESE_MONTH_MAP = {
    '一':1, '二':2, '三':3, '四':4, '五':5, '六':6, '七':7, '八':8, '九':9, '十':10, '十一':11, '十二':12
}

# helper: normalize whitespace and fullwidth spaces
def norm(s):
    if s is None:
        return ''
    return re.sub(r"\s+", " ", str(s)).strip()


def chinese_month_to_num(s):
    # expect strings like '一　月 Jan.' or '　一　月 Jan.' or '一月'
    s = norm(s)
    # extract the leading Chinese numerals
    m = re.search(r'([一二三四五六七八九十]{1,3})', s)
    if not m:
        return None
    key = m.group(1).replace(' ', '')
    # normalize 十一/十二 etc
    if key in CHINESE_MONTH_MAP:
        return CHINESE_MONTH_MAP[key]
    # handle cases like '十', '十一', '十二'
    if key == '十':
        return 10
    if key == '十一':
        return 11
    if key == '十二':
        return 12
    return None


def is_year_total_row(cell_text):
    # detect rows that are year totals like '一○五年 2016' or contain '年' and a 3-digit ROC year
    if cell_text is None:
        return False
    s = str(cell_text)
    if '年' in s and re.search(r'\d{3,4}', s):
        return True
    # sometimes total rows contain '年度' or '總計'
    if '年度' in s or '總計' in s or '合計' in s:
        return True
    return False


def parse_45(path: str) -> pd.DataFrame:
    """
    Parse '4.5-辦理建物所有權登記' xlsx.
    Steps:
    - read entire sheet into DataFrame without header
    - remove row 0 and row 4 (1-indexed rows in user description)
    - combine rows 1-3 into header names
    - from row 5 (0-based index) onward, keep rows that are month rows, skip year total rows
    - convert month labels to YYYY-MM; if year is in ROC (e.g., 105), convert to AD
    """
    # Implement a dedicated parser for the 4.5 sheet layout.
    # Observed layout (1-indexed rows):
    # 1: noise (drop)
    # 2-4: header rows to be combined
    # 5+: data rows (month rows and possible year-total rows)

    # prefer the named sheet first as requested
    raw = None
    try:
        raw = read_sheet_by_name(path, '年月Monthly')
    except Exception:
        try:
            raw = robust_read_sheet(path)
        except Exception:
            raw = None

    if raw is None:
        # fallback to generic parser
        return parse_multiheader_table(path, header_rows=4)

    # Ensure we have enough rows
    if raw.shape[0] < 6:
        return parse_multiheader_table(path, header_rows=4)

    # According to spec:
    # - drop first row (index 0)
    # - rows 1..3 (index 1,2,3) are multiple header rows to combine
    # - drop row 4 (index 4)
    # - data starts at index 5 (6th row)
    hr_rows = raw.iloc[1:4].fillna('').astype(str)
    headers = []
    for col in hr_rows.columns:
        parts = [norm(x) for x in hr_rows[col].tolist() if norm(x) != '']
        hdr = ' '.join(parts) if parts else f'col{col}'
        headers.append(hdr)

    # make unique headers
    def make_unique_local(lst):
        seen = {}
        out = []
        for x in lst:
            if x in seen:
                seen[x] += 1
                out.append(f"{x}_{seen[x]}")
            else:
                seen[x] = 0
                out.append(x)
        return out
    headers = make_unique_local(headers)

    # mapping to canonical single-level column names for 4.5
    def map_moi_headers(headers):
        """Given a list of combined header strings, return a list of canonical names.
        Rules per user:
        1) 所有權第一次下層有兩種: 棟數 and 面積(平方公尺)
        2) 移轉登記下層有: 合計、買賣、拍賣、繼承、贈與、其他；每一項再分為 棟數 and 面積(平方公尺)
        We'll scan header strings for keywords and produce names like:
        - 所有權第一次登記_棟數
        - 所有權第一次登記_面積(平方公尺)
        - 移轉登記_買賣_棟數
        - 移轉登記_買賣_面積(平方公尺)
        Fallback: keep original header (whitespace-normalized).
        """
        out = []
        for h in headers:
            s = str(h)
            # normalize
            s_norm = re.sub(r"\s+", " ", s).strip()
            # detect 所有權第一次登記 block
            if any(k in s_norm for k in ['所有權第一次登記', 'First Registration']):
                # decide if this column is 棟數 or 面積
                if any(k in s_norm for k in ['棟數', '件數', 'Count', 'Cases']):
                    out.append('所有權第一次登記_棟數')
                    continue
                if any(k in s_norm for k in ['面積', '平方公尺', 'm²', 'm2', 'Area', 'Floor Area']):
                    out.append('所有權第一次登記_面積(平方公尺)')
                    continue
                # fallback keep header
                out.append('所有權第一次登記_' + s_norm)
                continue

            # detect 移轉登記 block
            if any(k in s_norm for k in ['移轉登記', 'Registration of Ownership Transfer']):
                # try to detect subcategory: 合計、買賣、拍賣、繼承、贈與、其他
                sub = None
                subs_map = {
                    '合計': '合計', 'Total': '合計', '合  計': '合計',
                    '買賣': '買賣', 'Sale': '買賣', '賣': '買賣',
                    '拍賣': '拍賣', 'Auction': '拍賣',
                    '繼承': '繼承', 'Inheritance': '繼承',
                    '贈與': '贈與', 'Gift': '贈與',
                    '其他': '其他', 'Other': '其他'
                }
                for k, v in subs_map.items():
                    if k in s_norm:
                        sub = v
                        break
                if sub is None:
                    # sometimes the subcategory appears in adjacent header text like '合計 Total'
                    for k, v in subs_map.items():
                        if k.lower() in s_norm.lower():
                            sub = v
                            break
                if sub is None:
                    sub = '其他'

                # determine if it's 棟數 or 面積
                if any(k in s_norm for k in ['棟數', '件數', 'Count', 'Cases']):
                    out.append(f'移轉登記_{sub}_棟數')
                    continue
                if any(k in s_norm for k in ['面積', '平方公尺', 'm²', 'm2', 'Area', 'Floor Area']):
                    out.append(f'移轉登記_{sub}_面積(平方公尺)')
                    continue
                # fallback: attach sub but keep header
                out.append(f'移轉登記_{sub}_' + s_norm)
                continue

            # general fallback: normalize whitespace
            out.append(s_norm)
        # ensure unique
        seen = {}
        uniq = []
        for x in out:
            if x in seen:
                seen[x] += 1
                uniq.append(f"{x}_{seen[x]}")
            else:
                seen[x] = 0
                uniq.append(x)
        return uniq

    # apply mapping to headers
    try:
        mapped = map_moi_headers(headers)
        # ensure same length
        if len(mapped) == len(headers):
            headers = mapped
    except Exception:
        # fall back to original headers on any mapping error
        pass


    data = raw.iloc[5:].reset_index(drop=True).copy()
    # pad or trim columns to match headers length
    if data.shape[1] < len(headers):
        # add empty columns
        for i in range(len(headers) - data.shape[1]):
            data[i + data.shape[1]] = ''
    data = data.iloc[:, :len(headers)]
    data.columns = headers

    # process rows: first column that contains month-like label is assumed headers[0]
    first_col = headers[0]

    def to_ad_year(ystr: str):
        # accept ROC like '105' or AD like '2016'
        try:
            y = int(re.search(r'(\d{3,4})', str(ystr)).group(1))
        except Exception:
            return None
        if y < 1900:
            return y + 1911
        return y

    out_rows = []
    current_year = None
    for _, r in data.iterrows():
        cell0 = r[first_col]
        t = norm(cell0)
        if t == '':
            # skip empty rows
            continue
        # if it's a year marker like '一○五年 2016' or contains '年' with digits
        if is_year_total_row(t):
            y = to_ad_year(t)
            if y is not None:
                current_year = y
            # do not keep year-only rows
            continue

        # if it's a Chinese month like '　一　月 Jan.' or '二月'
        mnum = chinese_month_to_num(t)
        if mnum is not None:
            if current_year is None:
                # cannot determine year, skip this row
                continue
            date_str = f"{current_year:04d}-{int(mnum):02d}"
            row_dict = {col: r.get(col, '') for col in headers}
            row_dict[first_col] = date_str
            out_rows.append(row_dict)
            continue

        # english month short like 'Jan' etc
        m2 = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', t)
        if m2 is not None:
            if current_year is None:
                continue
            mapp = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
            mm = mapp.get(m2.group(1), None)
            if mm is None:
                continue
            date_str = f"{current_year:04d}-{mm:02d}"
            row_dict = {col: r.get(col, '') for col in headers}
            row_dict[first_col] = date_str
            out_rows.append(row_dict)
            continue

        # direct yyyy-mm like
        m3 = re.match(r'(\d{4})[\-/](\d{1,2})', t)
        if m3:
            y = int(m3.group(1)); mm = int(m3.group(2))
            date_str = f"{y:04d}-{mm:02d}"
            row_dict = {col: r.get(col, '') for col in headers}
            row_dict[first_col] = date_str
            out_rows.append(row_dict)
            continue

        # otherwise skip rows that are not month rows
        continue

    if not out_rows:
        # return empty tidy frame with headers
        empty = pd.DataFrame(columns=['日期'] + headers[1:])
        return empty

    out_df = pd.DataFrame(out_rows)
    # ensure column order
    out_df = out_df.loc[:, headers]
    # rename first col to 日期
    out_df = out_df.rename(columns={headers[0]: '日期'})
    # strip and clean
    out_df = out_df.fillna('').astype(str).apply(lambda col: col.str.strip())

    # try to coerce numeric columns
    for col in out_df.columns:
        if col == '日期':
            continue
        cleaned = out_df[col].astype(str).str.replace(r"[^0-9\-]", "", regex=True)
        cleaned = cleaned.replace({'': pd.NA})
        out_df[col] = pd.to_numeric(cleaned, errors='coerce').astype('Int64')

    # map to canonical columns as requested by user
    desired = [
        '日期',
        '所有權第一次登記_棟數', '所有權第一次登記_面積(平方公尺)',
        '移轉登記_合計_棟數', '移轉登記_合計_面積(平方公尺)',
        '移轉登記_買賣_棟數', '移轉登記_買賣_面積(平方公尺)',
        '移轉登記_拍賣_棟數', '移轉登記_拍賣_面積(平方公尺)',
        '移轉登記_繼承_棟數', '移轉登記_繼承_面積(平方公尺)',
        '移轉登記_贈與_棟數', '移轉登記_贈與_面積(平方公尺)',
        '移轉登記_其他_棟數', '移轉登記_其他_面積(平方公尺)'
    ]

    cols = list(out_df.columns)

    def find_col_for(patterns_any=None, patterns_all=None):
        # patterns_any: any of these tokens may appear; patterns_all: all must appear
        for c in cols:
            s = str(c)
            ok_any = True
            ok_all = True
            if patterns_any:
                ok_any = any(p.lower() in s.lower() for p in patterns_any)
            if patterns_all:
                ok_all = all(p.lower() in s.lower() for p in patterns_all)
            if ok_any and ok_all:
                return c
        return None

    lookup = {}
    # 所有權第一次登記
    lookup['所有權第一次登記_棟數'] = find_col_for(patterns_any=['所有權第一次登記', 'First Registration'], patterns_all=['棟','件','count','cases']) or find_col_for(patterns_any=['所有權第一次登記','First Registration'])
    lookup['所有權第一次登記_面積(平方公尺)'] = None
    # more robust: search for 面積 tokens across columns
    if lookup['所有權第一次登記_面積(平方公尺)'] is None:
        lookup['所有權第一次登記_面積(平方公尺)'] = find_col_for(patterns_any=['面積','平方公尺','m²','Area','Floor Area'])

    # 移轉登記 categories
    subs = [('合計', ['合計','Total']), ('買賣', ['買賣','Sale','Transaction']), ('拍賣', ['拍賣','Auction']), ('繼承', ['繼承','Inheritance']), ('贈與', ['贈與','Gift']), ('其他', ['其他','Other','Others'])]
    for sub_key, tokens in subs:
        key_cnt = f'移轉登記_{sub_key}_棟數'
        key_area = f'移轉登記_{sub_key}_面積(平方公尺)'
        # find count col
        ccol = find_col_for(patterns_any=['移轉登記','Registration of Ownership Transfer']+tokens, patterns_all=None)
        if ccol is None:
            # try matching tokens only
            ccol = find_col_for(patterns_any=tokens)
        lookup[key_cnt] = ccol
        # find area col
        acol = find_col_for(patterns_any=['面積','平方公尺','m²','Area','Floor Area']+tokens)
        lookup[key_area] = acol

    # build final tidy df
    final = {}
    for k in desired:
        if k == '日期':
            final[k] = out_df['日期'].astype(str)
            continue
        srccol = lookup.get(k)
        if srccol and srccol in out_df.columns:
            final[k] = out_df[srccol]
        else:
            final[k] = pd.Series([pd.NA]*len(out_df), dtype='Int64')

    tidy_final = pd.DataFrame(final)
    # add prefix for 4.5 output columns (except 日期)
    prefixed = tidy_final.copy()
    newcols = []
    for c in prefixed.columns:
        if c == '日期':
            newcols.append(c)
        else:
            newcols.append('所有權登記_' + c)
    prefixed.columns = newcols
    return prefixed


def parse_85(path: str) -> pd.DataFrame:
    # Per user instruction, 8.5 should use the same parsing logic as 8.1 for the
    # sheet '年月monthly(2018.02新修正格式update)'. Delegate to parse_81 which
    # already implements the required behavior.
    # parse_85 uses same logic as 8.1 but output columns must be prefixed for 使用執照
    df = parse_81(path)
    # normalize: if parse_81 already prefixed with '建造執照_', remove it so 8.5 uses only '使用執照_'
    pref = df.copy()
    newcols = []
    for c in pref.columns:
        if c == '日期':
            newcols.append(c)
        else:
            s = str(c)
            if s.startswith('建造執照_'):
                s = s[len('建造執照_'):]
            newcols.append('使用執照_' + s)
    pref.columns = newcols
    return pref


def parse_multiheader_table(path: str, header_rows: int = 4) -> pd.DataFrame:
    """Generic parser for tables with multiple header rows (e.g., 8.1 / 8.5).
    - Reads sheet (sheet 0) via robust_read_sheet
    - Combines first `header_rows` rows to make column headers
    - Scans subsequent rows for year-total rows and month rows (uses same helpers as parse_45)
    - Produces a tidy DataFrame with `日期` and cleaned numeric columns (Int64)
    """
    raw = robust_read_sheet(path)
    if raw.shape[0] < header_rows + 1:
        # fallback: return raw
        return raw

    # build headers from first header_rows rows
    hr = raw.iloc[:header_rows].fillna('').astype(str)
    headers = []
    for col in hr.columns:
        parts = [norm(x) for x in hr[col].tolist() if norm(x) != '']
        hdr = ' '.join(parts) if parts else f'col{col}'
        headers.append(hdr)
    # make unique
    def make_unique_local(lst):
        seen = {}
        out = []
        for x in lst:
            if x in seen:
                seen[x] += 1
                out.append(f"{x}_{seen[x]}")
            else:
                seen[x] = 0
                out.append(x)
        return out
    headers = make_unique_local(headers)

    data = raw.iloc[header_rows:].copy()
    data.columns = headers

    first_col = headers[0]
    out_rows = []
    current_year = None
    for _, r in data.iterrows():
        cell0 = r[first_col]
        if pd.isna(cell0):
            continue
        t = str(cell0).strip()
        if t == '':
            continue
        if is_year_total_row(t):
            m = re.search(r'(\d{3,4})', t)
            if m:
                y = int(m.group(1))
                if y < 1900:
                    y = y + 1911
                current_year = y
            continue
        mnum = chinese_month_to_num(t)
        if mnum is None:
            m2 = re.search(r'([JFMASOND][a-z]{2})', t)
            if m2:
                mapp = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                key = m2.group(1)[:3]
                mnum = mapp.get(key, None)
        if mnum is None:
            # sometimes the row starts with yyyy-mm already
            m3 = re.match(r'(\d{4})[\-/](\d{1,2})', t)
            if m3:
                y = int(m3.group(1)); mm = int(m3.group(2))
                date_str = f"{y:04d}-{mm:02d}"
                row = r.copy()
                row[first_col] = date_str
                out_rows.append(row)
                continue
            # otherwise skip
            continue
        if current_year is None:
            mfn = re.search(r'_(\d{6,8})', os.path.basename(path))
            if mfn:
                ymd = mfn.group(1)
                year = int(ymd[:4])
            else:
                year = 1900
        else:
            year = current_year
        mm = int(mnum)
        date_str = f"{year:04d}-{mm:02d}"
        row = r.copy()
        row[first_col] = date_str
        out_rows.append(row)

    if not out_rows:
        # fallback: return data with generated headers
        data.columns = headers
        return data

    out_df = pd.DataFrame(out_rows)
    out_df.columns = headers
    # drop empty cols
    tmp = out_df.replace({None: ''}).astype(str).apply(lambda col: col.str.strip())
    non_empty = [c for c in tmp.columns if not (tmp[c] == '').all()]
    out_df = out_df[non_empty].copy()
    # rename first col to 日期
    if len(out_df.columns) > 0:
        first = out_df.columns[0]
        out_df = out_df.rename(columns={first: '日期'})
    # normalize names
    newcols = [re.sub(r"\s+", " ", str(c)).strip() for c in out_df.columns]
    out_df.columns = newcols

    # clean numeric columns
    tidy = out_df.copy()
    # strip strings
    tidy = tidy.fillna('').astype(str).apply(lambda col: col.str.strip())
    for col in tidy.columns:
        if col == '日期':
            continue
        cleaned = tidy[col].astype(str).str.replace(r"[^0-9\-]", "", regex=True)
        cleaned = cleaned.replace('', pd.NA)
        tidy[col] = pd.to_numeric(cleaned, errors='coerce').astype('Int64')

    return tidy


def parse_81(path: str) -> pd.DataFrame:
    """Parse 8.1 file using the sheet name requested by user.
    Rules: drop first row, combine next 4 rows into headers, then extract month rows from the remainder.
    """
    # try to read the named sheet first (user requested specific sheet name)
    raw = None
    try:
        raw = read_sheet_by_name(path, '年月monthly(2018.02新修正格式update)')
    except Exception:
        try:
            raw = robust_read_sheet(path)
        except Exception:
            raw = None

    if raw is None:
        raise RuntimeError('cannot read file for 8.1: ' + str(path))

    if raw.shape[0] < 6:
        # fallback to generic parser
        return parse_multiheader_table(path, header_rows=4)

    # drop the very first row (noise)
    raw2 = raw.iloc[1:].reset_index(drop=True)

    # build headers from rows 0..3
    hr = raw2.iloc[0:4].fillna('').astype(str)
    headers = []
    for col in hr.columns:
        parts = [norm(x) for x in hr[col].tolist() if norm(x) != '']
        hdr = ' '.join(parts) if parts else f'col{col}'
        headers.append(hdr)

    # make unique
    seen = {}
    newh = []
    for x in headers:
        if x in seen:
            seen[x] += 1
            newh.append(f"{x}_{seen[x]}")
        else:
            seen[x] = 0
            newh.append(x)
    headers = newh

    data = raw2.iloc[4:].copy()
    data.columns = headers

    # extract month rows similar to parse_multiheader_table
    first_col = headers[0]
    out_rows = []
    current_year = None
    for _, r in data.iterrows():
        cell0 = r[first_col]
        if pd.isna(cell0):
            continue
        t = str(cell0).strip()
        if t == '':
            continue
        if is_year_total_row(t):
            m = re.search(r'(\d{3,4})', t)
            if m:
                y = int(m.group(1))
                if y < 1900:
                    y = y + 1911
                current_year = y
            continue
        mnum = chinese_month_to_num(t)
        if mnum is None:
            m2 = re.search(r'([JFMASOND][a-z]{2})', t)
            if m2:
                mapp = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                key = m2.group(1)[:3]
                mnum = mapp.get(key, None)
        if mnum is None:
            # sometimes yyyy-mm present
            m3 = re.match(r'(\d{4})[\-/](\d{1,2})', t)
            if m3:
                y = int(m3.group(1)); mm = int(m3.group(2))
                date_str = f"{y:04d}-{mm:02d}"
                row = r.copy()
                row[first_col] = date_str
                out_rows.append(row)
            continue
        if current_year is None:
            mfn = re.search(r'_(\d{6,8})', os.path.basename(path))
            if mfn:
                ymd = mfn.group(1)
                year = int(ymd[:4])
            else:
                year = 1900
        else:
            year = current_year
        mm = int(mnum)
        date_str = f"{year:04d}-{mm:02d}"
        row = r.copy()
        row[first_col] = date_str
        out_rows.append(row)

    if not out_rows:
        # fallback to generic
        return parse_multiheader_table(path, header_rows=4)

    df = pd.DataFrame(out_rows)
    df.columns = headers
    tmp = df.replace({None: ''}).astype(str).apply(lambda col: col.str.strip())
    non_empty = [c for c in tmp.columns if not (tmp[c] == '').all()]
    df = df[non_empty].copy()
    if len(df.columns) > 0:
        first = df.columns[0]
        df = df.rename(columns={first: '日期'})
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    # mapping per user: build tidy with special residential naming
    cols = df.columns.tolist()
    used = set()
    def find_col_local(keywords):
        for c in cols:
            if c in used:
                continue
            s = str(c)
            for k in keywords:
                if k in s:
                    used.add(c)
                    return c
        return None

    tidy = {'日期': df['日期'].astype(str).tolist()}
    mappings81 = [
        ('總計_件數', ['件數', 'Cases', 'Total Cases']),
        ('總計_總樓地板面積', ['總樓地板面積', 'Total Floor Area']),
        ('住宅類_住宅_宅數', ['宅數', 'Houses', '住宅']),
        ('住宅類_住宅_總樓地板面積', ['住宅', '總樓地板面積', 'H-2']),
        ('住宅類_宿舍安養_總樓地板面積', ['宿舍', '安養', 'Dormitory', 'Care']),
        ('商業類_總樓地板面積', ['商業', 'Commerce', 'B類']),
        ('工業倉儲類_總樓地板面積', ['工業', '倉儲', 'Industry', 'Storage', 'C類']),
        ('辦公服務類_總樓地板面積', ['辦公', '服務', 'Business', 'Service', 'G類']),
        ('休閒文教類_總樓地板面積', ['休閒', '文教', 'Leisure', 'Education', 'D類']),
        ('衛生福利類_總樓地板面積', ['衛生', '福利', '更生', 'Health', 'Welfare', 'F類']),
        ('其他_公共集會_總樓地板面積', ['公共集會', 'Assembly', 'A類']),
        ('其他_宗教殯葬_總樓地板面積', ['宗教', '殯葬', 'Religion', 'Funeral', 'E類']),
        ('其他_危險物品_總樓地板面積', ['危險', 'Hazard', 'I類']),
        ('其他_其他_總樓地板面積', ['其他類', 'Others']),
        ('其他_農業設施_總樓地板面積', ['農業', 'Agricultural', 'facility'])
    ]

    for tgt, keys in mappings81:
        col = find_col_local(keys)
        if col and col in df.columns:
            tidy[tgt] = df[col].tolist()
        else:
            tidy[tgt] = [''] * len(df)

    tidy_df = pd.DataFrame(tidy)
    for col in tidy_df.columns:
        if col == '日期':
            continue
        cleaned = pd.Series(tidy_df[col]).astype(str).str.replace(r"[^0-9\-]", "", regex=True)
        cleaned = cleaned.replace({'': pd.NA})
        tidy_df[col] = pd.to_numeric(cleaned, errors='coerce').astype('Int64')

    # add prefix for 8.1 output columns (except 日期)
    prefixed = tidy_df.copy()
    newcols = []
    for c in prefixed.columns:
        if c == '日期':
            newcols.append(c)
        else:
            newcols.append('建造執照_' + str(c))
    prefixed.columns = newcols
    return prefixed


def read_sheet_by_name(path: str, sheet_name: str) -> pd.DataFrame:
    """Parse an xlsx package and return a DataFrame for the named sheet (header=None).
    Uses ElementTree to correctly handle sharedStrings rich text and namespaces.
    """
    ns = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
          'rel': 'http://schemas.openxmlformats.org/package/2006/relationships'}
    with zipfile.ZipFile(path) as z:
        namelist = z.namelist()
        # parse sharedStrings
        ss = []
        if 'xl/sharedStrings.xml' in namelist:
            root = ET.fromstring(z.read('xl/sharedStrings.xml'))
            for si in root.findall('main:si', ns):
                texts = []
                # collect all <t> under this si
                for t in si.findall('.//main:t', ns):
                    texts.append(t.text if t.text is not None else '')
                ss.append(''.join(texts))
        # parse workbook and rels to map sheet name -> target
        wb = ET.fromstring(z.read('xl/workbook.xml'))
        sheets = wb.find('main:sheets', ns)
        target = None
        for s in sheets.findall('main:sheet', ns):
            name = s.get('name')
            if name == sheet_name:
                rid = s.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
                for r in rels.findall('rel:Relationship', ns):
                    if r.get('Id') == rid:
                        target = r.get('Target')
                        break
                break
        if target is None:
            raise RuntimeError(f"sheet {sheet_name} not found")
        sheet_path = 'xl/' + target
        sheet_root = ET.fromstring(z.read(sheet_path))
        rows = sheet_root.findall('.//main:sheetData/main:row', ns)
        parsed = []
        maxcol = 0
        for row in rows:
            rowcells = {}
            for c in row.findall('main:c', ns):
                ref = c.get('r')
                # column letters
                m = re.match(r'([A-Za-z]+)(\d+)', ref)
                if not m:
                    continue
                colletters = m.group(1)
                # convert letters to index
                idx = 0
                for ch in colletters:
                    idx = idx*26 + (ord(ch.upper())-64)
                if idx > maxcol:
                    maxcol = idx
                t = c.get('t')
                v = c.find('main:v', ns)
                if v is not None:
                    if t == 's':
                        val = ss[int(v.text)] if int(v.text) < len(ss) else v.text
                    else:
                        val = v.text
                else:
                    is_elem = c.find('main:is', ns)
                    if is_elem is not None:
                        texts = [t.text if t.text is not None else '' for t in is_elem.findall('.//main:t', ns)]
                        val = ''.join(texts)
                    else:
                        val = ''
                rowcells[idx-1] = val
            parsed.append([rowcells.get(i, '') for i in range(maxcol)])
        # normalize lengths
        maxlen = max((len(r) for r in parsed), default=0)
        parsed = [r + ['']*(maxlen-len(r)) for r in parsed]
        import pandas as pd
        return pd.DataFrame(parsed)


def xlsx_zip_parse(path: str) -> pd.DataFrame:
    import zipfile, re
    z = zipfile.ZipFile(path)
    ss = {}
    if 'xl/sharedStrings.xml' in z.namelist():
        s = z.read('xl/sharedStrings.xml').decode('utf-8')
        texts = re.findall(r'<t[^>]*>(.*?)</t>', s, flags=re.S)
        for i, t in enumerate(texts):
            ss[i] = t
    sheet = None
    for n in z.namelist():
        if n.startswith('xl/worksheets/sheet') and n.endswith('.xml'):
            sheet = z.read(n).decode('utf-8')
            break
    if sheet is None:
        raise RuntimeError('no sheet xml')
    rows = []
    row_matches = re.findall(r'<row[^>]*>(.*?)</row>', sheet, flags=re.S)
    for rm in row_matches:
        cells = re.findall(r'<c[^>]*>(.*?)</c>', rm, flags=re.S)
        vals = []
        for c in cells:
            v = re.search(r'<v>(.*?)</v>', c, flags=re.S)
            if v:
                txt = v.group(1)
                try:
                    idx = int(txt)
                    vals.append(ss.get(idx, txt))
                except Exception:
                    vals.append(txt)
            else:
                vals.append('')
        rows.append(vals)
    return pd.DataFrame(rows)


def robust_read_sheet(path: str) -> pd.DataFrame:
    # try openpyxl first, then pandas, then zip/xml
    try:
        return pd.read_excel(path, sheet_name=0)
    except Exception:
        try:
            return pd.read_excel(path, sheet_name=0, engine='openpyxl')
        except Exception:
            # try zip/xml for xlsx
            try:
                return xlsx_zip_parse(path)
            except Exception:
                raise


def process_folder(folder: str, overwrite: bool = False):
    folder = Path(folder)
    files = os.listdir(folder)
    results = {}
    for pref in TARGET_FILES:
        candidates = [f for f in files if f.startswith(pref) and f.lower().endswith(('.xls', '.xlsx'))]
        if not candidates:
            print('no file for', pref)
            results[pref] = None
            continue
        # prefer original files (exclude already-processed '(修正)')
        origs = [c for c in candidates if '(修正)' not in c]
        if origs:
            f = sorted(origs)[0]
        else:
            f = sorted(candidates)[0]
        src = folder / f
        # normalize base: if file already has '(修正)' remove it before appending
        base = os.path.splitext(f)[0]
        if base.endswith('(修正)'):
            base = base[: -len('(修正)')]
        out_name = f"{base}(修正).xlsx"
        out_path = folder / out_name
        try:
            if pref.startswith('4.5'):
                df = parse_45(src)
            elif pref.startswith('8.1'):
                df = parse_81(src)
            else:
                df = parse_85(src)
            # if file exists and overwrite is False, skip
            if out_path.exists() and not overwrite:
                print('skipping (exists):', out_path)
                results[str(src)] = str(out_path)
            else:
                df.to_excel(out_path, index=False)
                print('wrote', out_path)
                try:
                    print(f'OUTPUT: {out_path}')
                except Exception:
                    print('OUTPUT: ' + str(out_path))
                results[str(src)] = str(out_path)
        except Exception as e:
            print('failed processing', src, e)
            results[str(src)] = None
    return results


if __name__ == '__main__':
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else '.'
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
    folder = date_tag if date_tag else '.'
    process_folder(folder, overwrite=True)
