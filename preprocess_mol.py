import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Optional

import pandas as pd


def roc_to_ad_year(roc_year: int) -> int:
    return roc_year + 1911


def extract_roc_year_month_from_str(s: str) -> Optional[str]:
    # match patterns like '114年11月' or '114年11月份' or '民國114年11月'
    if not isinstance(s, str):
        return None
    m = re.search(r"(\d{2,4})\s*年\s*(\d{1,2})\s*月", s)
    if not m:
        return None
    roc = int(m.group(1))
    mon = int(m.group(2))
    ad = roc_to_ad_year(roc)
    return f"{ad:04d}-{mon:02d}"


def read_sheet_fallback_xlsx(path: str, sheet_name: str = None) -> pd.DataFrame:
    # read sharedStrings and workbook to map sheet name -> sheet#.xml
    with zipfile.ZipFile(path, 'r') as z:
        # read sharedStrings
        ss = []
        try:
            with z.open('xl/sharedStrings.xml') as f:
                tree = ET.parse(f)
                root = tree.getroot()
                for si in root.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si'):
                    # concatenate text nodes
                    texts = []
                    for t in si.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t'):
                        texts.append(t.text or '')
                    ss.append(''.join(texts))
        except KeyError:
            ss = []

        # find sheet file for sheet_name
        sheet_path = None
        try:
            with z.open('xl/workbook.xml') as f:
                tree = ET.parse(f)
                ns = {'d': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
                sheets = tree.findall('.//d:sheets/d:sheet', ns)
                for s in sheets:
                    name = s.attrib.get('name')
                    rid = s.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                    if sheet_name is None or name == sheet_name:
                        # map rid to target via workbook rels
                        with z.open('xl/_rels/workbook.xml.rels') as relf:
                            reltree = ET.parse(relf)
                            for rel in reltree.findall('.//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship'):
                                if rel.attrib.get('Id') == rid:
                                    target = rel.attrib.get('Target') or ''
                                    # normalize target: remove leading slashes and ../ segments
                                    target = target.lstrip('/')
                                    while target.startswith('../'):
                                        target = target[3:]
                                    if not target.startswith('xl/'):
                                        target = 'xl/' + target
                                    sheet_path = target
                                    break
                        if sheet_path:
                            break
        except KeyError:
            sheet_path = None

        if not sheet_path:
            # fallback: pick first worksheet
            names = [n for n in z.namelist() if n.startswith('xl/worksheets/sheet') and n.endswith('.xml')]
            if not names:
                raise ValueError('no sheet xml found')
            sheet_path = names[0]

        with z.open(sheet_path) as f:
            tree = ET.parse(f)
            root = tree.getroot()
            ns = {'d': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            rows = []
            for row in root.findall('.//d:row', ns):
                cells = []
                for c in row.findall('d:c', ns):
                    t = c.attrib.get('t')
                    v = c.find('d:v', ns)
                    if v is None:
                        cells.append('')
                    else:
                        val = v.text or ''
                        if t == 's':
                            try:
                                idx = int(val)
                                cells.append(ss[idx] if idx < len(ss) else '')
                            except Exception:
                                cells.append(val)
                        else:
                            cells.append(val)
                rows.append(cells)

    # normalize ragged rows by padding
    maxc = max((len(r) for r in rows), default=0)
    rows = [r + [''] * (maxc - len(r)) for r in rows]
    return pd.DataFrame(rows)


def robust_read_sheet(path: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, header=None)
        return df
    except Exception:
        return read_sheet_fallback_xlsx(path, sheet_name=sheet_name)


def parse_unemployment(path: str) -> pd.DataFrame:
    df = robust_read_sheet(path, sheet_name='Sheet1')
    # row3 (1-indexed) is header -> pandas zero-index row 2
    if df.shape[0] < 3:
        return pd.DataFrame()
    header = df.iloc[2].fillna('')
    header_list = header.tolist()
    header_list[0] = '日期'
    data = df.iloc[3:].copy()
    data.columns = header_list
    # convert first column ROC to AD
    def conv(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        # allow strings like '114年11月' or '114/11' or '114-11'
        m = re.search(r"(\d{2,4})\D+(\d{1,2})", s)
        if not m:
            return None
        roc = int(m.group(1))
        mon = int(m.group(2))
        return f"{roc_to_ad_year(roc):04d}-{mon:02d}"

    data['日期'] = data.iloc[:, 0].apply(conv)
    # keep only rows with 日期
    data = data[data['日期'].notna()].reset_index(drop=True)
    return data


def parse_reduced_hours(path: str) -> pd.DataFrame:
    # same basic layout as unemployment, but drop rows where industry (col2) is blank
    df = robust_read_sheet(path, sheet_name='Sheet1')
    if df.shape[0] < 3:
        return pd.DataFrame()
    header = df.iloc[2].fillna('')
    header_list = header.tolist()
    header_list[0] = '日期'
    data = df.iloc[3:].copy()
    data.columns = header_list

    # convert first column ROC to AD
    def conv(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        m = re.search(r"(\d{2,4})\D+(\d{1,2})", s)
        if not m:
            return None
        roc = int(m.group(1))
        mon = int(m.group(2))
        return f"{roc_to_ad_year(roc):04d}-{mon:02d}"

    data['日期'] = data.iloc[:, 0].apply(conv)
    data = data[data['日期'].notna()].reset_index(drop=True)

    # drop rows where industry column (second column) is empty or whitespace
    if data.shape[1] >= 2:
        col2 = data.columns[1]
        data[col2] = data[col2].astype(str).apply(lambda x: x.replace('\u3000', ' ').strip())
        data = data[data[col2].astype(bool)].reset_index(drop=True)

    return data


def parse_avg_hours(path: str) -> pd.DataFrame:
    df = robust_read_sheet(path, sheet_name='Sheet1')
    if df.shape[0] < 3:
        return pd.DataFrame()
    header = df.iloc[2].fillna('')
    header_list = header.tolist()
    header_list[0] = '日期'
    data = df.iloc[3:].copy()
    data.columns = header_list
    # convert first column ROC to AD
    def conv(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        m = re.search(r"(\d{2,4})\D+(\d{1,2})", s)
        if not m:
            return None
        roc = int(m.group(1))
        mon = int(m.group(2))
        return f"{roc_to_ad_year(roc):04d}-{mon:02d}"

    data['日期'] = data.iloc[:, 0].apply(conv)
    data = data[data['日期'].notna()].reset_index(drop=True)

    # merge industry categories in column 2 (index 1)
    if data.shape[1] >= 2:
        col2 = data.columns[1]
        # detect rows that start with full-width ideographic space or normal space
        def merge_industry(series):
            merged = []
            current_parent = None
            for v in series.fillna('').astype(str):
                # normalize fullwidth space to normal
                v2 = v.replace('\u3000', ' ')
                if v2.startswith(' '):
                    # child -> combine
                    if current_parent:
                        merged.append(f"{current_parent}_{v2.strip()}")
                    else:
                        merged.append(v2.strip())
                else:
                    # parent
                    current_parent = v2.strip()
                    merged.append(current_parent)
            return pd.Series(merged)

        data[col2] = merge_industry(data.iloc[:, 1])

    return data


def process_folder(folder: str, overwrite: bool = True):
    # expected filenames
    files = {
        'unemployment': '失業率_',
        'reduced': '勞雇雙方協商減少工時概況_',
        'avg_hours': '僱員工每人每月平均工時_',
    }
    for key, prefix in files.items():
        # find file in folder starting with prefix
        found = None
        for fn in os.listdir(folder):
            if fn.startswith(prefix) and fn.endswith('.xlsx'):
                found = os.path.join(folder, fn)
                break
        if not found:
            print(f"no file found for {prefix} in {folder}")
            continue

        if key == 'unemployment':
            outdf = parse_unemployment(found)
        elif key == 'reduced':
            outdf = parse_reduced_hours(found)
        else:
            outdf = parse_avg_hours(found)

        base = os.path.basename(found)
        name, ext = os.path.splitext(base)
        outname = f"{name}(修正){ext}"
        outpath = os.path.join(folder, outname)
        if os.path.exists(outpath) and not overwrite:
            print('skip existing', outpath)
            continue
        # write
        try:
            outdf.to_excel(outpath, index=False)
            print('wrote', outpath)
            print(f'OUTPUT: {outpath}')
        except Exception as e:
            print('write error', e)


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
    folder = date_tag if date_tag else '.'
    process_folder(folder, overwrite=True)
