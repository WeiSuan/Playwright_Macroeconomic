from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import time
import logging

DEFAULT_WAIT = 60

def sanitize(s: str) -> str:
    return ''.join(c for c in s if c.isalnum() or c in ' _-.').strip()

def ensure_outfolder(base='.'):
    tag = datetime.now().strftime('%Y%m%d')
    out = os.path.join(base, tag)
    os.makedirs(out, exist_ok=True)
    return out, tag

def _click_fallback(page, selectors):
    for sel in selectors:
        try:
            # wait a short moment for element to appear
            el = page.wait_for_selector(sel, timeout=3000)
        except Exception:
            el = None
        if el:
            for _ in range(3):
                try:
                    el.click()
                    return True
                except Exception:
                    try:
                        page.evaluate('(e) => e.click()', el)
                        return True
                    except Exception:
                        time.sleep(0.3)
                        continue
    return False

def download_with_name(page, out_folder, target_name, timeout=30000):
    with page.expect_download(timeout=timeout) as dr:
        # assume page triggered download already
        pass
    # unreachable in direct form; caller should use expect_download context

def run_pmi(output_dir='.', keep_browser_open=False):
    out_folder, tag = ensure_outfolder(output_dir)
    target = f'製造業採購經理人指數(PMI)_{tag}'
    URL = 'https://index.ndc.gov.tw/n/zh_tw/data'
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # go to PMI section: prefer menu item under div#menu with title
        try:
            page.wait_for_selector("div#menu [title='製造採購經理人指數']", timeout=10000)
            menu_el = page.query_selector("div#menu [title='製造採購經理人指數']")
            if menu_el:
                try:
                    menu_el.click()
                except Exception:
                    page.evaluate('(e)=>e.click()', menu_el)
            else:
                _click_fallback(page, ["a[href='/n/zh_tw/data/PMI#/']", "a:has-text('PMI')"])
        except Exception:
            _click_fallback(page, ["a[href='/n/zh_tw/data/PMI#/']", "a:has-text('PMI')"])
        page.wait_for_load_state('networkidle')
        time.sleep(1)

        # click download button by ng-click or title
        selectors = ["[ng-click='download_xls()']", "[title^='確定輸出']", "button:has-text('下載')"]
        el = None
        for sel in selectors:
            try:
                page.wait_for_selector(sel, timeout=5000)
                el = page.query_selector(sel)
            except Exception:
                el = None
            if el:
                break

        if el:
            # try multiple times to trigger download reliably
            for attempt in range(3):
                try:
                    with page.expect_download(timeout=30000) as dd:
                        el.click()
                    dl = dd.value
                    ext = os.path.splitext(dl.suggested_filename or '')[1] or '.xls'
                    dest = os.path.join(out_folder, sanitize(target) + ext)
                    dl.save_as(dest)
                    logging.getLogger(__name__).info('downloaded %s', dest)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    time.sleep(1)
        else:
            logging.getLogger(__name__).warning('PMI download element not found')

        if not keep_browser_open:
            context.close(); browser.close()

def run_nmi(output_dir='.', keep_browser_open=False):
    out_folder, tag = ensure_outfolder(output_dir)
    target = f'非製造業經理人指數(NMI)_{tag}'
    URL = 'https://index.ndc.gov.tw/n/zh_tw/data'
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # go to NMI section: prefer menu item under div#menu with title
        try:
            page.wait_for_selector("div#menu [title='非製造業經理人指數']", timeout=10000)
            menu_el = page.query_selector("div#menu [title='非製造業經理人指數']")
            if menu_el:
                try:
                    menu_el.click()
                except Exception:
                    page.evaluate('(e)=>e.click()', menu_el)
            else:
                _click_fallback(page, ["a[href='/n/zh_tw/data/NMI#']", "a:has-text('NMI')"])
        except Exception:
            _click_fallback(page, ["a[href='/n/zh_tw/data/NMI#']", "a:has-text('NMI')"])
        page.wait_for_load_state('networkidle')
        time.sleep(1)

        selectors = ["[ng-click='download_xls()']", "[title^='確定輸出']", "button:has-text('下載')"]
        el = None
        for sel in selectors:
            try:
                page.wait_for_selector(sel, timeout=5000)
                el = page.query_selector(sel)
            except Exception:
                el = None
            if el:
                break

        if el:
            for attempt in range(3):
                try:
                    with page.expect_download(timeout=30000) as dd:
                        el.click()
                    dl = dd.value
                    ext = os.path.splitext(dl.suggested_filename or '')[1] or '.xls'
                    dest = os.path.join(out_folder, sanitize(target) + ext)
                    dl.save_as(dest)
                    print('downloaded', dest)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(1)
        else:
            print('NMI download element not found')

        if not keep_browser_open:
            context.close(); browser.close()

def run_eco(output_dir='.', keep_browser_open=False):
    out_folder, tag = ensure_outfolder(output_dir)
    target = f'景氣指標及燈號_{tag}'
    URL = 'https://index.ndc.gov.tw/n/zh_tw/data'
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # navigate to eco
        _click_fallback(page, ["a[href='n/zh_tw/data/eco#/']", "a[href='/n/zh_tw/data/eco#/']", "a:has-text('景氣指標')"])
        page.wait_for_load_state('networkidle')

        # open 指標構成項目
        if not _click_fallback(page, ["[title='指標構成項目']", "button:has-text('指標構成項目')"]):
            print('指標構成項目 按鈕未找到')

        # click select_all buttons
        for bid in ('#select_all_1', '#select_all_2', '#select_all_3'):
            _click_fallback(page, [f"button{bid}", f"[role='button']{bid}", f"#{bid.lstrip('#')}"])

        # click download
        selectors = ["[ng-click='download_xls()']", "[title^='確定輸出']", "button:has-text('下載')"]
        el = None
        for sel in selectors:
            try:
                page.wait_for_selector(sel, timeout=5000)
                el = page.query_selector(sel)
            except Exception:
                el = None
            if el:
                break

        if el:
            for attempt in range(3):
                try:
                    with page.expect_download(timeout=30000) as dd:
                        el.click()
                    dl = dd.value
                    ext = os.path.splitext(dl.suggested_filename or '')[1] or '.xls'
                    dest = os.path.join(out_folder, sanitize(target) + ext)
                    dl.save_as(dest)
                    print('downloaded', dest)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(1)
        else:
            print('ECO download element not found')

        if not keep_browser_open:
            context.close(); browser.close()

def run_all(output_dir='.', keep_browser_open=False):
    run_pmi(output_dir, keep_browser_open)
    run_nmi(output_dir, keep_browser_open)
    run_eco(output_dir, keep_browser_open)

if __name__ == '__main__':
    run_all('.', True)


# compatibility wrapper for run_all_scrapers
def run(output_dir='.', keep_browser_open=False):
    run_all(output_dir, keep_browser_open)
