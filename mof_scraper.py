from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import re
import time
import logging

# module logger
logger = logging.getLogger(__name__)

URL = 'https://web02.mof.gov.tw/njswww/WebMain.aspx?sys=100&funid=defjsptgl'
DEFAULT_WAIT = 60


def run(output_dir='.', keep_browser_open=True):
    # ensure dated output folder exists under provided output_dir
    date_tag = datetime.now().strftime('%Y%m%d')
    # if the provided output_dir already ends with the date_tag, use it directly
    if os.path.basename(os.path.abspath(output_dir)) == date_tag:
        out_folder = output_dir
    else:
        out_folder = os.path.join(output_dir, date_tag)
    os.makedirs(out_folder, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(URL)
        page.wait_for_load_state('networkidle')

        # helper: try to find a selector in page or any child frame, return (element, owner)
        def find_element(root, selector, timeout=DEFAULT_WAIT):
            # try root (Page or Frame) first
            try:
                el = root.wait_for_selector(selector, timeout=timeout)
                if el:
                    return el, root
            except Exception:
                pass

            # then try child frames of root
            try:
                frames = root.frames
            except Exception:
                frames = []
            for fr in frames:
                try:
                    el = fr.wait_for_selector(selector, timeout=timeout)
                    if el:
                        return el, fr
                except Exception:
                    continue
            return None, None

        # helper: get labels from page or frames
        def get_labels():
            labs = page.query_selector_all('label') or []
            if labs:
                return labs
            for fr in page.frames:
                try:
                    labs = fr.query_selector_all('label')
                    if labs:
                        return labs
                except Exception:
                    continue
            return []

        # helper: find a frame by document.title within a timeout
        def get_frame_by_title(page_obj, expected_title, timeout=2.0):
            end = time.time() + timeout
            while time.time() < end:
                for fr in page_obj.frames:
                    try:
                        t = fr.evaluate('() => document.title')
                        if t and expected_title in t:
                            return fr
                    except Exception:
                        continue
                time.sleep(0.1)
            return None

        # click the folder9 '出口' link inside frame title='功能清單', then switch to frame title='查詢內容' for subsequent actions
        try:
            start_t = time.time()
            clicked = False
            func_frame = None
            for fr in page.frames:
                try:
                    t = fr.evaluate('() => document.title')
                    if t and '貿易統計資料查詢' in t:
                        func_frame = fr
                        break
                except Exception:
                    continue

            if not func_frame:
                for fr in page.frames:
                    try:
                        if 'defjsp7' in fr.url:
                            func_frame = fr
                            break
                    except Exception:
                        continue

            if func_frame:
                try:
                    loc = func_frame.locator('#folder9 >> text=出口')
                    if loc.count() > 0:
                        loc.first.click()
                        clicked = True
                except Exception:
                    pass

            if not clicked:
                try:
                    loc2 = page.locator('text=出口')
                    if loc2.count() > 0:
                        loc2.first.click()
                        clicked = True
                except Exception:
                    pass

            elapsed = time.time() - start_t
            logger.debug('folder9 click elapsed=%.2fs, clicked=%s', elapsed, clicked)

            if not clicked:
                logger.debug('folder9/出口 not found or not clickable')
            else:
                page.wait_for_load_state('networkidle')

            query_frame = get_frame_by_title(page, '查詢內容', timeout=5.0)
            if query_frame:
                operational_frame = query_frame
            else:
                operational_frame = None
                deadline = time.time() + (DEFAULT_WAIT / 1000.0)
                while time.time() < deadline:
                    try:
                        for fr in page.frames:
                            try:
                                if getattr(fr, 'name', '') == 'qry2' or ('i7000' in (fr.url or '')):
                                    operational_frame = fr
                                    break
                            except Exception:
                                continue
                        if operational_frame:
                            break
                    except Exception:
                        pass
                    time.sleep(0.2)
                if not operational_frame:
                    operational_frame = page
        except Exception:
            logger.exception('folder9 click error')

        # wait a short while for the operational_frame to populate selectors we need
        try:
            found_cycle = False
            found_table = False
            deadline = time.time() + (DEFAULT_WAIT / 1000.0)
            while time.time() < deadline:
                try:
                    if operational_frame.locator('select[name="cycle"]').count() > 0:
                        found_cycle = True
                except Exception:
                    found_cycle = False
                try:
                    if operational_frame.locator('table#item1').count() > 0:
                        found_table = True
                except Exception:
                    found_table = False
                if found_cycle or found_table:
                    break
                time.sleep(0.5)
                if not (found_cycle or found_table):
                    try:
                        snippet = operational_frame.content()[:10000]
                        logger.debug('operational_frame content snippet:\n%s', snippet)
                    except Exception:
                        logger.exception('could not read operational_frame content')
        except Exception:
            logger.exception('operational_frame wait error')

        # wait for content to load, then select USD checkbox (按美元計算(百萬美元))
        try:
            labels = []
            try:
                labels = operational_frame.query_selector_all('label') or []
            except Exception:
                labels = get_labels()
            usd_checkbox = None
            for lab in labels:
                try:
                    txt = lab.inner_text().strip()
                except Exception:
                    txt = ''
                if '按美元計算' in txt:
                    try:
                        cb = lab.query_selector('input[type=checkbox]')
                        if cb:
                            usd_checkbox = cb
                            break
                    except Exception:
                        pass
            if usd_checkbox:
                try:
                    checked = usd_checkbox.evaluate('el => el.checked')
                except Exception:
                    checked = False
                if not checked:
                    try:
                        usd_checkbox.click()
                    except Exception:
                        pass
            else:
                logger.debug('usd checkbox not found')
        except Exception:
            logger.exception('usd checkbox error')

        # select cycle name=cycle value=1 (January) with retries inside operational_frame
        try:
            sel_found = False
            deadline = time.time() + (DEFAULT_WAIT / 1000.0)
            while time.time() < deadline:
                try:
                    try:
                        operational_frame.select_option('select[name="cycle"]', '1')
                        sel_found = True
                        break
                    except Exception:
                        pass

                    try:
                        sel_loc = operational_frame.locator('select[name="cycle"]')
                        if sel_loc.count() > 0:
                            sel_loc.first.select_option(value='1')
                            sel_found = True
                            break
                    except Exception:
                        pass
                except Exception:
                    pass
                time.sleep(0.5)

            if not sel_found:
                logger.debug('cycle select not found in operational frame')
        except Exception:
            logger.exception('cycle select error')

        # ensure nodeIcon34 expanded, click if necessary
        try:
            node, node_owner = find_element(page, 'img[name="nodeIcon34"]', timeout=DEFAULT_WAIT)
            if node:
                try:
                    node.click()
                except Exception:
                    pass
            else:
                logger.debug('nodeIcon34 not found')
        except Exception:
            logger.exception('nodeIcon34 error')

        # after expanding nodeIcon34, find tables with id=35..39 and check all checkboxes under them
        try:
            for tid in range(35, 40):
                selector = f'table#item{tid}'
                table_found = False
                deadline = time.time() + (DEFAULT_WAIT / 1000.0)
                while time.time() < deadline:
                    try:
                        tbl_loc = operational_frame.locator(selector)
                        if tbl_loc.count() > 0:
                            table_found = True
                            checks = tbl_loc.locator('input[type=checkbox]')
                            for i in range(checks.count()):
                                try:
                                    cb = checks.nth(i)
                                    checked = False
                                    try:
                                        checked = cb.evaluate('el => el.checked')
                                    except Exception:
                                        pass
                                    if not checked:
                                        try:
                                            cb.click()
                                        except Exception:
                                            pass
                                except Exception:
                                    continue
                            break
                    except Exception:
                        pass

                    try:
                        tbl = operational_frame.query_selector(selector)
                        if tbl:
                            table_found = True
                            inputs = tbl.query_selector_all('input[type=checkbox]')
                            for inp in inputs:
                                try:
                                    checked = False
                                    try:
                                        checked = inp.evaluate('el => el.checked')
                                    except Exception:
                                        pass
                                    if not checked:
                                        try:
                                            inp.click()
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                            break
                    except Exception:
                        pass

                    time.sleep(0.2)

                if not table_found:
                    logger.debug('table #item%s not found under operational_frame', tid)
        except Exception:
            logger.exception('error checking tables 35-39')

        # select all checkboxes under table id=item1 inside operational_frame with retries
        try:
            table_found = False
            deadline = time.time() + (DEFAULT_WAIT / 1000.0)
            while time.time() < deadline:
                try:
                    tbl_loc = operational_frame.locator('table#item1')
                    if tbl_loc.count() > 0:
                        table_found = True
                        checks = tbl_loc.locator('input[type=checkbox]')
                        for i in range(checks.count()):
                            cb = checks.nth(i)
                            try:
                                checked = cb.evaluate('el => el.checked')
                            except Exception:
                                checked = False
                            if not checked:
                                try:
                                    cb.click()
                                except Exception:
                                    pass
                        break
                except Exception:
                    pass
                try:
                    tbl = operational_frame.query_selector('table#item1')
                    if tbl:
                        table_found = True
                        inputs = tbl.query_selector_all('input[type=checkbox]')
                        for inp in inputs:
                            try:
                                checked = inp.evaluate('el => el.checked')
                                if not checked:
                                    inp.click()
                            except Exception:
                                pass
                        break
                except Exception:
                    pass
                time.sleep(0.5)

            if not table_found:
                logger.debug('table#item1 not found in operational frame')
        except Exception:
            logger.exception('item1 checkbox error')

        # select output mode outmode value=2 (EXCEL)
        try:
            out_el, out_owner = find_element(operational_frame, 'select[name="outmode"]', timeout=DEFAULT_WAIT)
            if out_el and out_owner:
                try:
                    out_owner.select_option('select[name="outmode"]', value='1', timeout=DEFAULT_WAIT)
                except Exception:
                    logger.exception('outmode select error')
            else:
                logger.debug('outmode select not found')
        except Exception:
            logger.exception('outmode select error')

        # click the input.button (class=stybtn)
        try:
            btn, btn_owner = find_element(operational_frame, 'input.stybtn', timeout=DEFAULT_WAIT)
            if btn:
                with page.expect_download(timeout=20000) as dl:
                    btn.click()
                download = dl.value
                orig = download.suggested_filename or 'download'
                _, ext = os.path.splitext(orig)
                name = f"機械貨品別出口值_{datetime.now().strftime('%Y%m%d')}{ext}"
                path = os.path.join(out_folder, name)
                download.save_as(path)
                logger.info('downloaded=%s', path)
            else:
                logger.debug('submit button not found')
        except Exception:
            logger.exception('submit/download error')

        try:
            if keep_browser_open:
                logger.info('browser left open for inspection. Close manually when done.')
            else:
                browser.close()
        except Exception:
            pass


if __name__ == '__main__':
    today = datetime.now().strftime('%Y%m%d')
    outdir = os.path.join(os.getcwd(), today)
    run(outdir)
