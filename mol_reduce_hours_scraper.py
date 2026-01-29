from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import time
from typing import Callable
import logging

URL = 'https://statdb.mol.gov.tw/statiscla/webMain.aspx?sys=210&kind=21&type=1&funid=q06062&rdm=R656502'
OUTPUT_PREFIX = '勞雇雙方協商減少工時概況'


def retry(action: Callable, retries: int = 3, delay: float = 1.0):
    for i in range(retries):
        try:
            return action()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay)


def run(output_dir='.', keep_browser_open=False):
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    outdir = os.path.join(output_dir, today)
    os.makedirs(outdir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # select first option for ymt and set ymf to same

        # set cycle=1 and outmode=1
        try:
            retry(lambda: page.select_option('select[name="cycle"]', '1'), retries=3, delay=0.3)
        except Exception:
            pass
        try:
            retry(lambda: page.select_option('select[name="outmode"]', '1'), retries=3, delay=0.3)
        except Exception:
            pass

        # check checkboxes under #item8 and #folder10
        try:
            def check_ids():
                for id_ in ('item8', 'folder10'):
                    tbl = page.query_selector(f'table#{id_}')
                    if not tbl:
                        # continue if missing
                        continue
                    chks = tbl.query_selector_all('input[type="checkbox"]')
                    for c in chks:
                        try:
                            if not c.get_attribute('checked'):
                                c.click()
                        except Exception:
                            pass
                return True

            retry(check_ids, retries=4, delay=0.5)
        except Exception as e:
            logging.getLogger(__name__).exception('checkbox error')

        # click search image and download
        try:
            def click_and_download():
                img = page.query_selector('img[alt="查詢圖檔"]')
                if not img:
                    raise RuntimeError('search image not found')
                with page.expect_download(timeout=30000) as dl:
                    img.click()
                download = dl.value
                orig = download.suggested_filename or 'download'
                base, ext = os.path.splitext(orig)
                name = f"{OUTPUT_PREFIX}_{today}{ext}"
                path = os.path.join(outdir, name)
                download.save_as(path)
                return path

            path = retry(click_and_download, retries=4, delay=1.0)
            logging.getLogger(__name__).info('downloaded %s', path)
        except Exception as e:
            logging.getLogger(__name__).exception('download error')

        if not keep_browser_open:
            browser.close()


if __name__ == '__main__':
    run('.')
