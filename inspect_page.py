from playwright.sync_api import sync_playwright

URL = 'https://service.moea.gov.tw/EE520/investigate/InvestigateBA.aspx'

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(URL)
    page.wait_for_load_state('networkidle')
    html = page.content()
    with open('page_dump.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print('wrote page_dump.html')
    browser.close()
