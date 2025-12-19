Minimal EE520 scraper

Requirements:
- Node.js / Playwright browsers installed (we used `npx playwright install` earlier)
- Python 3.9+ and `playwright` Python package

Usage:

1. Install Playwright (if not already):
   npx playwright install
2. Install Python dependency for Playwright:
   /usr/bin/python3 -m pip install --user playwright
3. Run the scraper:
   /usr/bin/python3 run.py

Outputs:
- divTableReport_YYYYMMDD.txt   (text extracted from divTableReport)
- 銷訂單金額_美元 (百萬美元)_YYYYMMDD.txt   (TSV extracted from result table)

Notes:
- This package intentionally avoids heavy third-party libs like pandas or bs4 to stay minimal.
- The included SimpleTableParser is simplistic and may not handle complex table headers (MultiIndex) perfectly.
# Playwright_Macroeconomic
