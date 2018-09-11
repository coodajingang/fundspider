import pyppeteer

import asyncio

import os

os.environ['PYPPETEER_CHROMIUM_REVISION'] ='588429'
pyppeteer.DEBUG = True

async def main():
    print("in main ")
    print(os.environ.get('PYPPETEER_CHROMIUM_REVISION'))
    browser = await pyppeteer.launch()
    page = await browser.newPage()
    await page.goto('http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=161607&page=1&per=3247')
    
    content = await page.J('body > table')
    cookies = await page.cookies()
    # await page.screenshot({'path': 'example.png'})
    await browser.close()
    return {'content':content, 'cookies':cookies}

loop = asyncio.get_event_loop()
task = asyncio.ensure_future(main())
loop.run_until_complete(task)

print(task.result())

async def main2():
    browser = await pyppeteer.launch()
    page = await browser.newPage()
    await page.goto("http://www.baidu.com")
    await page.screenshot({'path': 'baidu.png'})
    await browser.close()

# asyncio.get_event_loop().run_until_complete(main())