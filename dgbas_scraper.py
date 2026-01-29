from playwright.sync_api import sync_playwright
import os
from datetime import datetime
import time

DEFAULT_WAIT = 60

def sanitize(s: str) -> str:
    return ''.join(c for c in s if c.isalnum() or c in ' _-.').strip()

def run(output_dir='.', keep_browser_open=False):
    """Scrape DGBAS A030502015 and download file as 營造工程物價指數_YYYYMMDD in ./YYYYMMDD/"""
    URL = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sys=210&funid=A030502015'
    date_tag = datetime.now().strftime('%Y%m%d')
    out_folder = os.path.join(output_dir, date_tag)
    os.makedirs(out_folder, exist_ok=True)
    target_name = f'營造工程物價指數_{date_tag}'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # select cycle=1
        sel_cycle = page.query_selector('select[name="cycle"]')
        if sel_cycle:
            try:
                sel_cycle.select_option(value='1')
            except Exception:
                # fallback: evaluate
                page.evaluate("() => { const s=document.querySelector('select[name=\\\"cycle\\\"]'); if(s) s.value='1'; }")

        # select outmode=1
        sel_out = page.query_selector('select[name="outmode"]')
        if sel_out:
            try:
                sel_out.select_option(value='1')
            except Exception:
                page.evaluate("() => { const s=document.querySelector('select[name=\\\"outmode\\\"]'); if(s) s.value='1'; }")

        # click the button (type=button). There might be multiple; prefer visible one.
        btn = None
        for e in page.query_selector_all('input[type="button"], button[type="button"]'):
            # choose the first visible enabled
            try:
                visible = e.is_visible()
            except Exception:
                visible = True
            if visible:
                btn = e
                break

        if not btn:
            # last resort, try to find by value/text
            btn = page.query_selector('input[type="button"]')

        if btn:
            with page.expect_download(timeout=30000) as dr:
                try:
                    btn.click()
                except Exception:
                    page.evaluate('e => e.click()', btn)
            download = dr.value
            # determine extension from suggested filename
            suggested = download.suggested_filename or 'download'
            ext = os.path.splitext(suggested)[1] or ''
            dest = os.path.join(out_folder, sanitize(target_name) + ext)
            download.save_as(dest)
            print('downloaded', dest)
        else:
            print('button not found; no download performed')

        if not keep_browser_open:
            context.close()
            browser.close()


if __name__ == '__main__':
    run('.', keep_browser_open=False)
