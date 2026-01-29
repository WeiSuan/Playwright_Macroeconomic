import os
import re
import sys
from pathlib import Path
import pandas as pd
import zipfile
import xml.etree.ElementTree as ET

"""
preprocess_moea.py
- reads files named: 各縣市加油站汽柴油銷售分析表_YYYYMMDD.xlsx
- reads sheet '銷售統計表'
- row2 contains title with ROC year/month like '114年11月份...': extract '114年11月' -> convert to YYYY-MM (AD) and place in column '日期' for all rows
- row3 is discarded
- row4 is header
- data starts at row5 until a row containing 合計 (or '總計') inclusive
- strip blanks and normalize
- write output as original base + '(修正).xlsx', overwrite if exists
"""


def roc_to_ad_year(roc_year_str: str) -> int:
    # convert ROC year like '114' to AD year 1911 + roc
    try:
        y = int(roc_year_str)
    except Exception:
        return None
    return 1911 + y


def extract_roc_year_month(title: str):
    # title example: '114年11月份各縣市汽車加油站汽、柴油銷售量統計表'
    if not isinstance(title, str):
        return None
    # accept 2-4 digit ROC year (e.g. 114) and an integer month
    m = re.search(r"(\d{2,4})年\s*(\d{1,2})\s*月", title)
    if m:
        roc = m.group(1)
        mm = int(m.group(2))
        y = roc_to_ad_year(roc)
        if y:
            return f"{y:04d}-{mm:02d}"
    # no match
    return None


def parse_moea_file(path: str) -> pd.DataFrame:
    # read sheet '銷售統計表' using pandas; fallback to zip/xml parser if openpyxl unavailable
    df = None
    try:
        xl = pd.ExcelFile(path)
        if '銷售統計表' not in xl.sheet_names:
            raise RuntimeError("sheet '銷售統計表' not found in " + str(path))
        df = xl.parse('銷售統計表', header=None, dtype=str)
    except Exception as e:
        # try fallback zip/xml reader
        try:
            def xlsx_zip_parse_sheet(p, sheet_name):
                ns = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
                      'rel': 'http://schemas.openxmlformats.org/package/2006/relationships'}
                with zipfile.ZipFile(p) as z:
                    namelist = z.namelist()
                    # parse sharedStrings
                    ss = []
                    if 'xl/sharedStrings.xml' in namelist:
                        root = ET.fromstring(z.read('xl/sharedStrings.xml'))
                        for si in root.findall('main:si', ns):
                            texts = [t.text if t.text is not None else '' for t in si.findall('.//main:t', ns)]
                            ss.append(''.join(texts))
                    # find sheet target
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
                        raise RuntimeError(f"sheet {sheet_name} not found in archive")
                    sheet_path = 'xl/' + target
                    sheet_root = ET.fromstring(z.read(sheet_path))
                    rows = sheet_root.findall('.//main:sheetData/main:row', ns)
                    parsed = []
                    maxcol = 0
                    for row in rows:
                        rowcells = {}
                        for c in row.findall('main:c', ns):
                            ref = c.get('r')
                            m = re.match(r'([A-Za-z]+)(\d+)', ref)
                            if not m:
                                continue
                            colletters = m.group(1)
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
                    maxlen = max((len(r) for r in parsed), default=0)
                    parsed = [r + ['']*(maxlen-len(r)) for r in parsed]
                    return pd.DataFrame(parsed)

            df = xlsx_zip_parse_sheet(path, '銷售統計表')
        except Exception:
            raise e
    if df.shape[0] < 5:
        raise RuntimeError('table too small')

    # row indexes are 0-based; user described row2 as title -> that's index 1
    title_row = df.iloc[1].fillna('').astype(str).tolist()
    title = ' '.join([t for t in title_row if t and str(t).strip() != ''])
    date_str = extract_roc_year_month(title)
    if date_str is None:
        # fallback: try to find pattern in any of first 4 rows
        for i in range(0, min(4, df.shape[0])):
            rowtxt = ' '.join([str(x) for x in df.iloc[i].fillna('')])
            d = extract_roc_year_month(rowtxt)
            if d:
                date_str = d
                break
    if date_str is None:
        date_str = ''

    # header is row4 -> index 3
    header = df.iloc[3].fillna('').astype(str).tolist()
    # data starts at row5 -> index 4
    data = df.iloc[4:].copy()
    data.columns = header

    # normalize header names: collapse whitespace and strip
    newcols = [re.sub(r"\s+", " ", str(c)).strip() for c in data.columns]
    data.columns = newcols
    # drop columns with empty header name
    non_empty_cols = [c for c in data.columns if c != '' and c is not None]
    data = data.loc[:, non_empty_cols].copy()

    # find end row: first row where first column contains 合計 or 總計
    first_col = data.columns[0]
    end_idx = None
    for i, r in data.iterrows():
        v = str(r[first_col]) if not pd.isna(r[first_col]) else ''
        if '合計' in v or '總計' in v or '合　計' in v:
            end_idx = i
            break
    if end_idx is not None:
        data = data.loc[:end_idx]

    # strip whitespace in all string cells and replace fullwidth spaces
    data = data.applymap(lambda x: str(x).replace('\u3000', ' ').strip() if not pd.isna(x) else '')

    # insert 日期 column at front; fill with date_str
    data.insert(0, '日期', [date_str] * len(data))

    # remove all whitespace characters from the county/地區 column (first data column)
    if data.shape[1] > 1:
        county_col = data.columns[1]
        # remove all whitespace (spaces, fullwidth spaces, tabs) inside the field
        data[county_col] = data[county_col].astype(str).str.replace(r"\s+", "", regex=True)

    return data


def process_folder(folder: str, overwrite: bool = True):
    folder = Path(folder)
    files = os.listdir(folder)
    for f in files:
        if f.startswith('各縣市加油站汽柴油銷售分析表_') and f.endswith('.xlsx') and '(修正)' not in f:
            src = folder / f
            base = os.path.splitext(f)[0]
            out_name = base + '(修正).xlsx'
            out_path = folder / out_name
            try:
                df = parse_moea_file(str(src))
                df.to_excel(out_path, index=False)
                print('wrote', out_path)
                try:
                    print(f'OUTPUT: {out_path}')
                except Exception:
                    print('OUTPUT: ' + str(out_path))
            except Exception as e:
                print('failed', src, e)


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
            process_folder(folder, overwrite=True)
    except Exception:
        process_folder(folder, overwrite=True)


def main(date_tag: str = None):
    """Entrypoint for run_all_preprocess: accepts a date_tag (folder name) or defaults to current folder '.'"""
    folder = date_tag if date_tag else '.'
    process_folder(folder, overwrite=True)
