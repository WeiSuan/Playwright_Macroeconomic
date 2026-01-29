from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import pickle
import time
import re

DEFAULT_WAIT = 60


def ensure_outfolder(base='.'):
    tag = datetime.now().strftime('%Y%m%d')
    out = os.path.join(base, tag)
    os.makedirs(out, exist_ok=True)
    return out, tag


def parse_table_html(page):
    # find table
    tbl = page.query_selector('table.pvtTable.table.table-bordered')
    if not tbl:
        return None

    # extract thead
    thead = []
    th_el = tbl.query_selector('thead')
    if th_el:
        for tr in th_el.query_selector_all('tr'):
            row = [th.inner_text().strip() for th in tr.query_selector_all('th')]
            thead.append(row)

    tbody = []
    tb_el = tbl.query_selector('tbody')
    if tb_el:
        for tr in tb_el.query_selector_all('tr'):
            # first child is th (if present), then all td
            cells = []
            ths = tr.query_selector_all('th')
            if ths:
                # take first th
                cells.append(ths[0].inner_text().strip())
            tds = tr.query_selector_all('td')
            for td in tds:
                txt = td.inner_text().strip()
                # remove thousands separators like '1,234' -> '1234'
                if re.match(r'^[0-9]{1,3}(?:,[0-9]{3})+(?:\.?[0-9]+)?$', txt):
                    txt = txt.replace(',', '')
                cells.append(txt)
            tbody.append(cells)

    return {'thead': thead, 'tbody': tbody}


def run_seq(seq, target_name, output_dir='.', keep_browser_open=False):
    URL = f'https://statis.motc.gov.tw/motc/Statistics/Display?Seq={seq}'
    out_folder, tag = ensure_outfolder(output_dir)
    fname = f'{target_name}_{tag}.pickle'
    dest = os.path.join(out_folder, fname)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context()
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # checkbox logic
        try:
            # use evaluate-based clicks to avoid Playwright auto-scrolling
            page.evaluate("() => { const y = document.querySelector('#check-period-show-year'); if(y && y.checked) y.click(); const m = document.querySelector('#check-period-show-month'); if(m && !m.checked) m.click(); }")
        except Exception:
            pass

        # click set period button
        try:
            # click via evaluate to avoid scrolling
            page.evaluate("() => { const b = document.getElementById('btn-set-period'); if(b) b.click(); }")
        except Exception:
            pass

        # wait a bit for table to render
        time.sleep(1)
        page.wait_for_load_state('networkidle')

        table_dict = parse_table_html(page)
        if table_dict is None:
                import logging
                logging.getLogger(__name__).warning('table not found for seq %s', seq)
        else:
            # save as pickle
            with open(dest, 'wb') as f:
                pickle.dump(table_dict, f)
            import logging
            logging.getLogger(__name__).info('saved %s', dest)

        if not keep_browser_open:
            context.close()
            browser.close()


def run_seq901(output_dir='.', keep_browser_open=False):
    run_seq(901, '汽車客貨運量概況', output_dir, keep_browser_open)


def run_seq97(output_dir='.', keep_browser_open=False):
    run_seq(97, '高速公路計程收費通行量', output_dir, keep_browser_open)


def run_seq206(output_dir='.', keep_browser_open=False):
    run_seq(206, '國際商港貨櫃裝卸量', output_dir, keep_browser_open)

if __name__ == '__main__':
    run_seq901('.', False)
    run_seq97('.', False)
    run_seq206('.', False)


# unified run wrapper for run_all
def run(output_dir='.', keep_browser_open=False):
    # run all three sequences
    run_seq901(output_dir, keep_browser_open)
    run_seq97(output_dir, keep_browser_open)
    run_seq206(output_dir, keep_browser_open)