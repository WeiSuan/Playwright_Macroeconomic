const { chromium } = require('playwright');

(async () => {
  // 以可視化模式啟動瀏覽器
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto('https://www.google.com');

  console.log('已開啟 https://www.google.com');

  // 等待 10 秒讓使用者看見瀏覽器
  await new Promise((resolve) => setTimeout(resolve, 10000));

  await browser.close();
})();
