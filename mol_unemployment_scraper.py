from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import time
from typing import Callable
import logging

URL = 'https://statdb.mol.gov.tw/statiscla/webMain.aspx?sys=210&kind=21&type=1&funid=q02071&rdm=R9696345'
DEFAULT_WAIT = 60

OUTPUT_PREFIX = '失業率'

def retry(action: Callable, retries: int = 3, delay: float = 1.0):
    for i in range(retries):
        try:
            return action()
        except Exception as e:
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

        # select first option for ymt and set ymf to same (with retries)
        try:
            def pick_dates():
                ymt_sel = page.query_selector('select[name="ymt"]')
                if not ymt_sel:
                    raise RuntimeError('ymt select not found')
                opts = ymt_sel.query_selector_all('option')
                if not opts:
                    raise RuntimeError('no ymt options')
                first_opt = opts[0].get_attribute('value')
                page.select_option('select[name="ymt"]', first_opt)
                page.select_option('select[name="ymf"]', first_opt)
                return first_opt

            first_val = retry(pick_dates, retries=5, delay=1.0)
            # small wait for dependent JS
            time.sleep(0.3)
        except Exception as e:
            logging.getLogger(__name__).exception('error selecting ymt/ymf')

        # set cycle=1 and outmode=1 (with retries)
        try:
            retry(lambda: page.select_option('select[name="cycle"]', '1'), retries=3, delay=0.5)
        except Exception:
            pass
        try:
            retry(lambda: page.select_option('select[name="outmode"]', '1'), retries=3, delay=0.5)
        except Exception:
            pass

        # find table summary and then label with text 總計
        # find table summary and then label with text 總計 (with retries)
        try:
            def check_and_click_totals():
                tbl = page.query_selector('table[summary="統計資料庫表格資料"]')
                if not tbl:
                    raise RuntimeError('table not found')
                labels = tbl.query_selector_all('label')
                found_any = False
                for lab in labels:
                    try:
                        if '總計' in (lab.inner_text() or ''):
                            found_any = True
                            parent_handle = lab.evaluate_handle('el => el.closest("span")')
                            if not parent_handle:
                                continue
                            parent_el = parent_handle.as_element()
                            chks = parent_el.query_selector_all('input[type="checkbox"]')
                            for c in chks:
                                try:
                                    # if checkbox not checked, click it
                                    is_checked = c.get_attribute('checked')
                                    if not is_checked:
                                        c.click()
                                except Exception:
                                    pass
                    except Exception:
                        pass
                if not found_any:
                    raise RuntimeError('no 總計 label found')
                return True

            retry(check_and_click_totals, retries=5, delay=1.0)
        except Exception as e:
            logging.getLogger(__name__).exception('error finding table/labels')

        # click image with alt="查詢圖檔"
        try:
            def click_search_and_download():
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

            path = retry(click_search_and_download, retries=4, delay=1.0)
            logging.getLogger(__name__).info('downloaded %s', path)
        except Exception as e:
            logging.getLogger(__name__).exception('download error')

        if not keep_browser_open:
            browser.close()


if __name__ == '__main__':
    run('.')
