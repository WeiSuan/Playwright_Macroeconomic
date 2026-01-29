import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Optional

import pandas as pd


def roc_to_ad_year(roc_year: int) -> int:
    return roc_year + 1911


def read_sheet_fallback_xlsx(path: str, sheet_name: str = None) -> pd.DataFrame:
    with zipfile.ZipFile(path, 'r') as z:
        ss = []
        try:
            with z.open('xl/sharedStrings.xml') as f:
                tree = ET.parse(f)
                root = tree.getroot()
                for si in root.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si'):
                    texts = []
                    for t in si.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t'):
                        texts.append(t.text or '')
                    ss.append(''.join(texts))
        except KeyError:
            ss = []

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
                        with z.open('xl/_rels/workbook.xml.rels') as relf:
                            reltree = ET.parse(relf)
                            for rel in reltree.findall('.//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship'):
                                if rel.attrib.get('Id') == rid:
                                    target = rel.attrib.get('Target') or ''
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

    maxc = max((len(r) for r in rows), default=0)
    rows = [r + [''] * (maxc - len(r)) for r in rows]
    return pd.DataFrame(rows)


def robust_read_sheet(path: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, header=None)
        return df
    except Exception:
        return read_sheet_fallback_xlsx(path, sheet_name=sheet_name)


def roc_date_from_cell(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    m = re.search(r"(\d{2,4})\D+(\d{1,2})", s)
    if not m:
        return None
    roc = int(m.group(1))
    mon = int(m.group(2))
    return f"{roc_to_ad_year(roc):04d}-{mon:02d}"


def parse_mof_machine_exports(path: str) -> pd.DataFrame:
    df = robust_read_sheet(path, sheet_name='Sheet1')
    # row4 (1-indexed) is header -> iloc[3]
    if df.shape[0] < 4:
        return pd.DataFrame()
    header = df.iloc[3].fillna('')
    header_list = header.tolist()
    header_list[0] = '日期'
    # append (百萬美元) to other headers
    for i in range(1, len(header_list)):
        if header_list[i].strip():
            header_list[i] = f"{header_list[i].strip()}(百萬美元)"
    data = df.iloc[4:].copy()
    data.columns = header_list
    data['日期'] = data.iloc[:, 0].apply(roc_date_from_cell)
    data = data[data['日期'].notna()].reset_index(drop=True)
    return data


def process_folder(folder: str, overwrite: bool = True):
    prefix = '機械貨品別出口值_'
    found = None
    for fn in os.listdir(folder):
        if fn.startswith(prefix) and fn.endswith('.xlsx'):
            found = os.path.join(folder, fn)
            break
    if not found:
        print(f'no file found for {prefix} in {folder}')
        return
    outdf = parse_mof_machine_exports(found)
    base = os.path.basename(found)
    name, ext = os.path.splitext(base)
    outname = f"{name}(修正){ext}"
    outpath = os.path.join(folder, outname)
    if os.path.exists(outpath) and not overwrite:
        print('skip existing', outpath)
        return
    try:
        outdf.to_excel(outpath, index=False)
        print('wrote', outpath)
        try:
            print(f'OUTPUT: {outpath}')
        except Exception:
            # fallback safe print
            print('OUTPUT: ' + str(outpath))
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
