from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import logging

URL = 'https://statis.moi.gov.tw/micst/webMain.aspx?k=menum'
DEFAULT_WAIT = 60

TARGET_TITLES = [
    '4.5-辦理建物所有權登記',
    '8.1-核發建築物建造執照按用途別分',
    '8.5-核發建築物使用執照按用途別分'
]


def run(output_dir='.', keep_browser_open=False):
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    outdir = os.path.join(output_dir, today)
    os.makedirs(outdir, exist_ok=True)

    with sync_playwright() as p:
        # if keep_browser_open is True we want headful mode for inspection
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # find the table summary="內政統計月報"
        try:
            tbl = page.query_selector('table[summary="內政統計月報"]')
            if not tbl:
                logging.getLogger(__name__).warning('table not found')
                if not keep_browser_open:
                    browser.close()
                return
        except Exception as e:
            logging.getLogger(__name__).exception('table lookup error')
            if not keep_browser_open:
                browser.close()
            return

        # parse tbody rows
        try:
            rows = tbl.query_selector_all('tbody tr')
        except Exception:
            rows = []

        found = {}
        for t in TARGET_TITLES:
            found[t] = None

        for r in rows:
            try:
                text = r.inner_text() or ''
            except Exception:
                text = ''
            for t in TARGET_TITLES:
                if t in text and found[t] is None:
                    # find a[title="下載XLSX"] in this row
                    try:
                        a = r.query_selector('a[title="下載XLSX"]')
                        if a:
                            found[t] = a
                            logging.getLogger(__name__).info('found download link for %s', t)
                    except Exception:
                        pass

        # click downloads and save with sanitized TARGET title + _YYYYMMDD suffix
        def sanitize(s: str) -> str:
            # remove or replace characters invalid for filenames but keep dots
            repl = s.replace('/', '_').replace(' ', '_')
            # keep a reasonable subset of chars including the dot
            return ''.join(c for c in repl if c.isalnum() or c in ('_', '-', '.'))
        for t in TARGET_TITLES:
            a = found.get(t)
            if not a:
                logging.getLogger(__name__).warning('not found link for %s', t)
                continue
            try:
                with page.expect_download(timeout=20000) as dl:
                    a.click()
                download = dl.value
                orig = download.suggested_filename or 'download.xlsx'
                _, ext = os.path.splitext(orig)
                base_name = sanitize(t)
                name = f"{base_name}_{today}{ext}"
                path = os.path.join(outdir, name)
                download.save_as(path)
                logging.getLogger(__name__).info('downloaded %s', path)
            except Exception as e:
                logging.getLogger(__name__).exception('download error for %s', t)

        if not keep_browser_open:
            browser.close()


if __name__ == '__main__':
    run('.')
