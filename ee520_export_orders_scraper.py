from playwright.sync_api import sync_playwright
import os
from datetime import datetime
import pickle
import time

DEFAULT_WAIT = 60

def sanitize(s: str) -> str:
    # allow dots, spaces, underscores, hyphens and chinese chars
    return ''.join(c for c in s if c.isalnum() or c in ' _-./\u4e00-\u9fff').strip()

def parse_table(page):
    # locate the table by id
    tbl = page.query_selector('#divTableReport #ContentPlaceHolder1_tabResult')
    if not tbl:
        return None

    result = {'thead': [], 'tbody': []}

    thead = tbl.query_selector('thead')
    if thead:
        for tr in thead.query_selector_all('tr'):
            row = []
            for td in tr.query_selector_all('th,td'):
                txt = td.inner_text().strip()
                if __import__('re').match(r'^[0-9]{1,3}(?:,[0-9]{3})+(?:\.?[0-9]+)?$', txt):
                    txt = txt.replace(',', '')
                row.append(txt)
            if row:
                result['thead'].append(','.join(row))

    tbody = tbl.query_selector('tbody')
    if tbody:
        for tr in tbody.query_selector_all('tr'):
            cells = []
            for cell in tr.query_selector_all('th,td'):
                txt = cell.inner_text().strip()
                if __import__('re').match(r'^[0-9]{1,3}(?:,[0-9]{3})+(?:\.?[0-9]+)?$', txt):
                    txt = txt.replace(',', '')
                cells.append(txt)
            if cells:
                result['tbody'].append(','.join(cells))

    return result

def run(output_dir='.', keep_browser_open=False):
    """Scrape EE520 外銷訂單 and save as 外銷訂單_YYYYMMDD.pickle under ./YYYYMMDD/"""
    URL = 'https://service.moea.gov.tw/EE520/investigate/InvestigateBA.aspx'
    date_tag = datetime.now().strftime('%Y%m%d')
    out_folder = os.path.join(output_dir, date_tag)
    os.makedirs(out_folder, exist_ok=True)
    target_name = f'外銷訂單_{date_tag}'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not keep_browser_open)
        context = browser.new_context()
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # step 3.1 checkbox in div#ContentPlaceHolder1_tvItem1 name=ContentPlaceHolder1_tvItem1n0CheckBox
        try:
            cb1 = page.query_selector('div#ContentPlaceHolder1_tvItem1 input[type="checkbox"][name="ContentPlaceHolder1_tvItem1n0CheckBox"]')
            if cb1 and not cb1.is_checked():
                cb1.check()
        except Exception:
            pass

        # step 3.2 checkbox in div#ContentPlaceHolder1_tvItem2 name=ContentPlaceHolder1_tvItem2n1CheckBox
        try:
            cb2 = page.query_selector('div#ContentPlaceHolder1_tvItem2 input[type="checkbox"][name="ContentPlaceHolder1_tvItem2n1CheckBox"]')
            if cb2 and not cb2.is_checked():
                cb2.check()
        except Exception:
            pass

        # step 3.3 checkbox in div#ContentPlaceHolder1_divItem3 name=ContentPlaceHolder1_tvItem3n0CheckBox
        try:
            cb3 = page.query_selector('div#ContentPlaceHolder1_divItem3 input[type="checkbox"][name="ContentPlaceHolder1_tvItem3n0CheckBox"]')
            if cb3 and not cb3.is_checked():
                cb3.check()
        except Exception:
            pass

        # step 3.4 click submit input[type=submit]
        try:
            submit = page.query_selector('input[type="submit"]')
            if submit:
                submit.click()
            else:
                # try button element
                btn = page.query_selector('button[type="submit"]')
                if btn:
                    btn.click()
        except Exception:
            # fallback: evaluate a click on first submit
            page.evaluate("() => { const e=document.querySelector('input[type=\\\"submit\\\"]'); if(e) e.click(); }")

        # wait for results area
        try:
            page.wait_for_selector('#divTableReport #ContentPlaceHolder1_tabResult', timeout=DEFAULT_WAIT*1000)
        except Exception:
            time.sleep(3)

        parsed = parse_table(page)
        if parsed is None:
            print('table not found')
        else:
            dest = os.path.join(out_folder, sanitize(target_name) + '.pickle')
            with open(dest, 'wb') as f:
                pickle.dump(parsed, f)
            print('saved', dest)

        if not keep_browser_open:
            context.close()
            browser.close()


if __name__ == '__main__':
    run('.', keep_browser_open=False)
