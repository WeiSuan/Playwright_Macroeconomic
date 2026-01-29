import os
import sys
import pandas as pd
import re
import traceback

# usage: python preprocess_ntc_index.py /path/to/YYYYMMDD
# expects files in folder named like:
# 製造業採購經理人指數PMI_YYYYMMDD.xls
# 非製造業經理人指數NMI_YYYYMMDD.xls
# 景氣指標及燈號_YYYYMMDD.xls

TARGET_PREFIXES = [
    '製造業採購經理人指數PMI_',
    '非製造業經理人指數NMI_',
    '景氣指標及燈號_'
]


def normalize_date(s: str) -> str:
    if pd.isna(s):
        return ''
    s = str(s).strip()
    # remove trailing .0 from float-like strings
    s = re.sub(r"\.0+$", "", s)

    # YYYYMM (e.g. 202501 or '202501') -> 'YYYY-MM'
    m0 = re.match(r'^(\d{4})(\d{2})$', s)
    if m0:
        y, mm = m0.group(1), m0.group(2)
        try:
            return f"{int(y):04d}-{int(mm):02d}"
        except Exception:
            return s

    # YYYY-MM or YYYY/MM -> 'YYYY-MM'
    m = re.match(r'^(\d{4})[-/](\d{1,2})', s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"

    # ROC formats like '113-01' or '113/1' -> convert to AD
    m2 = re.match(r'^(\d{2,3})[-/](\d{1,2})$', s)
    if m2:
        y = int(m2.group(1)) + 1911
        return f"{y:04d}-{int(m2.group(2)):02d}"

    # Chinese ROC like '110年07月'
    m3 = re.match(r'^(\d{2,3})年\s*(\d{1,2})月', s)
    if m3:
        y = int(m3.group(1)) + 1911
        return f"{y:04d}-{int(m3.group(2)):02d}"

    # fallback: try to extract first 4-digit year and month nearby
    m4 = re.search(r'(\d{4})\D+(\d{1,2})', s)
    if m4:
        return f"{int(m4.group(1)):04d}-{int(m4.group(2)):02d}"

    return s


def process_file(path: str, out_path: str):
    # try multiple readers and return a DataFrame (header=None)
    df = None
    # 1) pandas default (lets pandas pick engine)
    try:
        df = pd.read_excel(path, sheet_name=0, header=None)
    except Exception as e:
        print('pandas default read failed:', repr(e))
    # 2) explicit xlrd engine (for older xls)
    if df is None:
        try:
            df = pd.read_excel(path, sheet_name=0, header=None, engine='xlrd')
            print('read with engine=xlrd OK')
        except Exception as e:
            print('pandas engine=xlrd failed:', repr(e))
    # 3) try pyexcel as a fallback (pyexcel + pyexcel-xls)
    if df is None:
        try:
            import pyexcel as pe
            arr = pe.get_sheet(file_name=path).to_array()
            df = pd.DataFrame(arr)
            print('read with pyexcel OK')
        except Exception as e:
            print('pyexcel read failed:', repr(e))
    # 4) try reading OLE stream and hand to xlrd via file_contents
    if df is None:
        try:
            import olefile
            import xlrd
            oli = olefile.OleFileIO(path)
            # scan streams for workbook-like names
            candidate = None
            for entry in oli.listdir(streams=True, storages=False):
                # entry is a tuple like (name,)
                if not entry:
                    continue
                name = entry[0]
                lname = name.lower()
                if 'workbook' in lname or lname.endswith('book') or lname.endswith('workbook'):
                    candidate = entry
                    break
            # fallback: pick first stream that looks like a stream
            if candidate is None:
                entries = [e for e in oli.listdir(streams=True, storages=False) if e]
                if entries:
                    candidate = entries[0]
            if candidate is not None:
                # candidate may be a tuple; openstream accepts the tuple/list
                raw = oli.openstream(candidate).read()
                bk = xlrd.open_workbook(file_contents=raw)
                sh = bk.sheet_by_index(0)
                rows = [sh.row_values(i) for i in range(sh.nrows)]
                df = pd.DataFrame(rows)
                print('read via olefile+xlrd OK, rows=', len(rows))
            else:
                print('olefile: no suitable stream found')
        except Exception as e:
            print('olefile/xlrd fallback failed:', repr(e))
            traceback.print_exc()

    if df is None:
        print('All read attempts failed for', path)
        return False
    if df.shape[0] < 3:
        print('unexpected small table', path)
        return False
    # header is row1
    headers = [str(x).strip() for x in df.iloc[0].tolist()]
    headers[0] = '日期'
    # drop row2
    data = df.iloc[2:].copy()
    data.columns = headers
    # normalize date column first column
    date_col = headers[0]
    data[date_col] = data[date_col].apply(normalize_date)
    # save to excel xlsx (use pandas)
    try:
        data.to_excel(out_path, index=False)
    except Exception as e:
        print('write failed', out_path, e)
        return False
    # ensure the saved file is writable and not immutable (macOS 'uchg')
    try:
        try:
            os.chmod(out_path, 0o644)
        except Exception as e:
            print('chmod failed for', out_path, e)
        # on macOS, remove immutable flag if set
        try:
            import subprocess
            subprocess.run(['chflags', 'nouchg', out_path], check=False)
        except Exception:
            pass
    except Exception:
        pass
    return True


def main(folder: str):
    files = os.listdir(folder)
    results = []
    for prefix in TARGET_PREFIXES:
        candidates = [f for f in files if f.startswith(prefix) and f.lower().endswith(('.xls', '.xlsx'))]
        if not candidates:
            print('no file for', prefix)
            continue
        # pick latest by name (or first)
        f = sorted(candidates)[0]
        src = os.path.join(folder, f)
        base = f.replace('.xls', '').replace('.xlsx', '')
        out_name = f"{base}(修正).xlsx"
        out_path = os.path.join(folder, out_name)
        ok = process_file(src, out_path)
        print('processed', src, '->', out_path, 'ok=', ok)
        if ok:
            try:
                print(f'OUTPUT: {out_path}')
            except Exception:
                print('OUTPUT: ' + str(out_path))
        results.append((src, out_path, ok))
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
            main(folder)
    except Exception:
        main(folder)


def main(date_tag: str = None):
    """Entrypoint for run_all_preprocess: accepts a date_tag (folder) or defaults to current folder '.'"""
    folder = date_tag if date_tag else '.'
    # original main(folder) exists above; call it
    return globals().get('main', None) and globals()['main'](folder)
