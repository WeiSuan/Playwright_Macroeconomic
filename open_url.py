from playwright.sync_api import sync_playwright
import time
import pandas as pd
import io
import re
from datetime import datetime

URL = 'https://service.moea.gov.tw/EE520/investigate/InvestigateBA.aspx'

def select_latest_option_id(page, select_selector):
    # 優先選擇帶有 selected 屬性的 option，否則選最後一個 option（通常為最新）
    options = page.query_selector_all(f"{select_selector} option")
    if not options:
        return None
    selected = None
    for opt in options:
        if opt.get_attribute('selected') is not None:
            selected = opt
            break
    target = selected or options[-1]
    value = target.get_attribute('value')
    page.select_option(select_selector, value)
    # 選項可能會觸發 postback，稍微等待網路閒置
    try:
        page.wait_for_load_state('networkidle', timeout=3000)
    except Exception:
        pass
    return value

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto(URL)
    page.wait_for_load_state('networkidle')
    print(f"已開啟 {URL}")

    # 1) 設定週期為「月」(value = 'M')
    try:
        page.select_option('#ContentPlaceHolder1_ddlPeriod', 'M')
    except Exception:
        pass

    # 2) 查詢日期使用民國（頁面預設為民國），選擇最新可用月份為起訖
    try:
        # 先確保日期種類為民國
        page.select_option('#ContentPlaceHolder1_ddlDateKind', '民國')
    except Exception:
        # 有些頁面使用文字 value，若失敗則忽略
        pass

    # 列出並選擇最接近現在的日期（以 option 的 value 數字大小判斷）
    try:
        def list_options(sel):
            opts = page.query_selector_all(f"{sel} option")
            items = []
            for o in opts:
                v = o.get_attribute('value')
                t = o.inner_text().strip()
                items.append((v, t))
            return items

        beg_items = list_options('#ContentPlaceHolder1_ddlDateBeg')
        end_items = list_options('#ContentPlaceHolder1_ddlDateEnd')
        print('可選起始日期數量:', len(beg_items))
        print('起始選項範例:', beg_items[:3])
        print('可選結束日期數量:', len(end_items))
        print('結束選項範例:', end_items[:3])

        def pick_latest(items):
            # 選 value 最大的（數字）
            nums = []
            for v, t in items:
                try:
                    nums.append((int(v), v, t))
                except Exception:
                    # fallback: try to strip non-digits
                    digits = ''.join(ch for ch in v if ch.isdigit())
                    if digits:
                        nums.append((int(digits), v, t))
            if not nums:
                return None
            nums.sort()
            return nums[-1][1]

        latest_beg = pick_latest(beg_items)
        latest_end = pick_latest(end_items)
        print('選擇的最新起始 value:', latest_beg)
        print('選擇的最新結束 value:', latest_end)

        if latest_beg:
            page.select_option('#ContentPlaceHolder1_ddlDateBeg', latest_beg)
            try:
                page.wait_for_load_state('networkidle', timeout=3000)
            except Exception:
                pass
        if latest_end:
            page.select_option('#ContentPlaceHolder1_ddlDateEnd', latest_end)
            try:
                page.wait_for_load_state('networkidle', timeout=3000)
            except Exception:
                pass
    except Exception:
        # fallback to previous behavior
        try:
            opts_beg = page.query_selector_all('#ContentPlaceHolder1_ddlDateBeg option')
            if opts_beg:
                last_val = opts_beg[-1].get_attribute('value')
                page.select_option('#ContentPlaceHolder1_ddlDateBeg', last_val)
        except Exception:
            pass
        try:
            opts_end = page.query_selector_all('#ContentPlaceHolder1_ddlDateEnd option')
            if opts_end:
                last_val_e = opts_end[-1].get_attribute('value')
                page.select_option('#ContentPlaceHolder1_ddlDateEnd', last_val_e)
        except Exception:
            pass

    # 3) 勾選「外銷訂單金額_美元」
    try:
        cb_usd = '#ContentPlaceHolder1_tvItem1n0CheckBox'
        # 使用 click 以觸發頁面上的事件處理器，更新已選數量
        try:
            page.click(cb_usd)
        except Exception:
            # fallback to check
            if not page.is_checked(cb_usd):
                page.check(cb_usd)
    except Exception:
        pass

    # 4) 在按貨品類別分 (tvItem2) 點選 全選 (id: ContentPlaceHolder1_tvItem2n1CheckBox)
    try:
        cb_tv2_all = '#ContentPlaceHolder1_tvItem2n1CheckBox'
        try:
            page.click(cb_tv2_all)
        except Exception:
            if not page.is_checked(cb_tv2_all):
                page.check(cb_tv2_all)
    except Exception:
        pass

    # 5) 在地區別只選取「地區別總計」(id: ContentPlaceHolder1_tvItem3n0CheckBox)
    try:
        cb_tv3_total = '#ContentPlaceHolder1_tvItem3n0CheckBox'
        try:
            page.click(cb_tv3_total)
        except Exception:
            if not page.is_checked(cb_tv3_total):
                page.check(cb_tv3_total)
    except Exception:
        pass

    # 6) 點擊查詢按鈕
    try:
        page.click('#ContentPlaceHolder1_btnQuery')
    except Exception:
        # fallback: submit the form
        try:
            page.evaluate("document.getElementById('form1').submit();")
        except Exception:
            pass

    # 等待結果載入並截圖
    page.wait_for_load_state('networkidle')
    time.sleep(2)
    page.screenshot(path='query_result.png', full_page=True)

    # 擷取 div id="divTableReport" 的純文字並儲存
    try:
        div_report = page.query_selector('#divTableReport')
        if div_report:
            report_text = div_report.inner_text()
            date_stamp = datetime.now().strftime('%Y%m%d')
            div_filename = f"divTableReport_{date_stamp}.txt"
            with open(div_filename, 'w', encoding='utf-8') as dfh:
                dfh.write(report_text)
            print('已將 divTableReport 文字儲存為:', div_filename)
        else:
            print('找不到 #divTableReport 元素')
    except Exception as e:
        print('擷取 divTableReport 發生錯誤：', e)

    # 擷取查詢日期（起訖）與已選數量
    def safe_text(selector):
        try:
            el = page.query_selector(selector)
            return el.inner_text().strip() if el else ''
        except Exception:
            return ''

    # 使用 eval_on_selector 取得 select 的選中文字（更可靠）
    try:
        start_date = page.eval_on_selector('#ContentPlaceHolder1_ddlDateBeg', "el => (el.selectedOptions && el.selectedOptions.length)? el.selectedOptions[0].textContent.trim() : (el.options.length? el.options[el.options.length-1].textContent.trim() : '')")
    except Exception:
        start_date = ''

    try:
        end_date = page.eval_on_selector('#ContentPlaceHolder1_ddlDateEnd', "el => (el.selectedOptions && el.selectedOptions.length)? el.selectedOptions[0].textContent.trim() : (el.options.length? el.options[el.options.length-1].textContent.trim() : '')")
    except Exception:
        end_date = ''

    # 等待短暫時間讓頁面更新已選數字
    try:
        page.wait_for_timeout(500)
    except Exception:
        pass

    selected1 = safe_text('#ContentPlaceHolder1_lblSelected1')
    selected2 = safe_text('#ContentPlaceHolder1_lblSelected2')
    selected3 = safe_text('#ContentPlaceHolder1_lblSelected3')

    # 另存結果 HTML 以供檢查
    try:
        html = page.content()
        with open('result_dump.html', 'w', encoding='utf-8') as fh:
            fh.write(html)
    except Exception:
        html = ''

    # 嘗試從 HTML 中抽取民國年+月樣式，例如 114年10月
    import re
    date_matches = re.findall(r"\d{2,3}年\d{1,2}月", html)
    found_date = date_matches[0] if date_matches else ''

    print('查詢起始日期:', start_date)
    print('查詢結束日期:', end_date)
    print('擷取到 HTML 中的日期樣式:', found_date)
    print('已選 統計項目:', selected1)
    print('已選 按貨品類別分:', selected2)
    print('已選 按地區別分:', selected3)
    print('完成：已執行勾選並查詢，結果已保存為 query_result.png 及 result_dump.html')

    # 嘗試用 pandas 解析 HTML 中的表格，尋找包含「外銷」或「百萬」關鍵字的表格
    excel_saved = False
    try:
        # 先嘗試直接解析 id=ContentPlaceHolder1_tabResult 的子 HTML
        fragment = ''
        try:
            tab_el = page.query_selector('#ContentPlaceHolder1_tabResult')
            if tab_el:
                fragment = tab_el.inner_html()
        except Exception:
            fragment = ''

        # pandas.read_html 需要 file-like 或字符串
        if fragment:
            from io import StringIO
            dfs = pd.read_html(StringIO(fragment))
        else:
            dfs = pd.read_html(html)
        target_df = None
        for df in dfs:
            s = df.to_string()
            if re.search(r'外銷|百萬|美元|外銷訂單', s):
                target_df = df
                break

        if target_df is None and dfs:
            # fallback: choose the largest table
            target_df = max(dfs, key=lambda d: (d.shape[0]*d.shape[1]))

        if target_df is not None:
            # 列印 DataFrame 的摘要並儲存為文字檔
            print('擷取到目標表格，DataFrame shape =', target_df.shape)
            print(target_df.head(10).to_string(index=False))
            date_stamp = datetime.now().strftime('%Y%m%d')
            txt_filename = f"銷訂單金額_美元 (百萬美元)_{date_stamp}.txt"
            try:
                target_df.to_csv(txt_filename, sep='\t', index=False)
                print('已將表格儲存為文字檔:', txt_filename)
                excel_saved = True
            except Exception as e:
                print('儲存文字檔發生錯誤：', e)
    except Exception as e:
        print('解析 HTML 表格失敗：', e)

    if not excel_saved:
        print('未能自動產生 Excel，請檢查 result_dump.html 或手動處理。')

    # 保持瀏覽器開啟：等待使用者按 Enter 後再關閉
    try:
        input('執行完成，頁面已停留。按 Enter 鍵以關閉瀏覽器並結束腳本...')
    except Exception:
        # 若無法等待輸入，則等待較長時間
        time.sleep(600)

    browser.close()
