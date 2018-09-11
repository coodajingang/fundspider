# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import random
import pyppeteer
import asyncio
import os
from scrapy.http import HtmlResponse

pyppeteer.DEBUG = False 

class FundscrapySpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FundscrapyDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.
    def __init__(self) :
        print("Init downloaderMiddleware use pypputeer.")
        os.environ['PYPPETEER_CHROMIUM_REVISION'] ='588429'
        # pyppeteer.DEBUG = False
        print(os.environ.get('PYPPETEER_CHROMIUM_REVISION'))
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(self.getbrowser())
        loop.run_until_complete(task)

        #self.browser = task.result()
        print(self.browser)
        print(self.page)
        # self.page = await browser.newPage()
    async def getbrowser(self):
        self.browser = await pyppeteer.launch()
        self.page = await self.browser.newPage()
        # return await pyppeteer.launch()
    async def getnewpage(self): 
        return  await self.browser.newPage()

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(self.usePypuppeteer(request))
        loop.run_until_complete(task)
        # return task.result()
        return HtmlResponse(url=request.url, body=task.result(), encoding="utf-8",request=request)

    async def usePypuppeteer(self, request):
        print(request.url)
        # page = await self.browser.newPage()
        await self.page.goto(request.url)
        content = await self.page.content()
        return content 

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

# 自定义user-agent  ， 在下载时 使用随机的user-aget  
# 待选择的浏览器在settings.py文件中定义  
# MY_USER_AGENT 


class MyUserAgentDownloadMiddleWare(UserAgentMiddleware):

    def __init__(self, user_agent):
        self.user_agent = user_agent

    @classmethod
    def from_crawler(cls, crawler):
        return cls(user_agent=crawler.settings.get('MY_USER_AGENT'))

    def process_request(self, request, spider):
        agent = random.choice(self.user_agent)
        print("使用USER_AGET：", agent)
        request.headers['User-Agent'] = agent

### 代理地址端口设置    MY_PROXY_IPS 
class MyProxyAgent(object):

    def __init__(self, proxys):
        self.proxys = proxys

    @classmethod
    def from_crawler(cls, crawler):
        return cls(proxys = crawler.settings.get('MY_PROXY_IPS'))

    def process_request(self, request, spider):
        #暂时不设置
        print("TODO 设置代理。。。")
        pass