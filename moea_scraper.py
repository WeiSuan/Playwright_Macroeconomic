from playwright.sync_api import sync_playwright
from datetime import datetime
import csv
import os
import time

URL = 'https://www.moeaea.gov.tw/ECW/populace/content/wfrmStatistics.aspx?type=2&menu_id=1300'
DEFAULT_WAIT = 60


def run(output_dir='.', keep_browser_open=False):
    # ensure dated output dir exists
    date_tag = datetime.now().strftime('%Y%m%d')
    out_folder = os.path.join(output_dir, date_tag)
    os.makedirs(out_folder, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        # set download acceptance; downloads will be saved explicitly with download.save_as
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # find the table
        try:
            page.wait_for_selector('table.DataTable_List', timeout=DEFAULT_WAIT)
        except Exception:
            print('table not found')
            browser.close()
            return

        table = page.query_selector('table.DataTable_List')
        if not table:
            print('table not found')
            browser.close()
            return

        # extract rows
        rows = table.query_selector_all('tr')
        parsed = []
        for r in rows:
            cells = [c.inner_text().strip() for c in r.query_selector_all('th,td')]
            if cells:
                parsed.append(cells)

        # take the first data row after header
        first_data = None
        if len(parsed) > 1:
            first_data = parsed[1]
        elif parsed:
            first_data = parsed[0]

        # click the target img to download
        try:
            img = page.query_selector('#ctl00_holderContent_grdStatistics_ctl02_imgOffice')
            if img:
                # trigger download
                with page.expect_download(timeout=10000) as download_info:
                    img.click()
                download = download_info.value
                # save to output_dir with deterministic filename preserving extension
                orig_name = download.suggested_filename or ''
                _, ext = os.path.splitext(orig_name)
                new_name = f"各縣市加油站汽柴油銷售分析表_{date_tag}{ext}"
                path = os.path.join(out_folder, new_name)
                download.save_as(path)
                print('downloaded', path)
            else:
                print('download img not found')
        except Exception as e:
            print('download error=', e)

        browser.close()


if __name__ == '__main__':
    today = datetime.now().strftime('%Y%m%d')
    outdir = os.path.join(os.getcwd(), today)
    run(outdir)
