from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import time
from typing import Callable

URL = 'https://statdb.mol.gov.tw/statiscla/webMain.aspx?sys=210&kind=21&type=1&funid=q04022&rdm=R8360730'
OUTPUT_PREFIX = '僱員工每人每月平均工時'


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

        # set cycle=1 and outmode=1
        try:
            retry(lambda: page.select_option('select[name="cycle"]', '1'), retries=3, delay=0.3)
        except Exception:
            pass
        try:
            retry(lambda: page.select_option('select[name="outmode"]', '1'), retries=3, delay=0.3)
        except Exception:
            pass

        # check checkboxes by name fldsel and codsel0
        try:
            def check_by_name():
                for name in ('fldsel', 'codsel0'):
                    try:
                        c = page.query_selector(f'input[name="{name}"][type="checkbox"]')
                        if c and not c.get_attribute('checked'):
                            c.click()
                    except Exception:
                        pass
                return True

            retry(check_by_name, retries=4, delay=0.5)
        except Exception as e:
            print('check by name error', e)

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
            print('downloaded', path)
        except Exception as e:
            print('download error', e)

        if not keep_browser_open:
            browser.close()


if __name__ == '__main__':
    run('.')
