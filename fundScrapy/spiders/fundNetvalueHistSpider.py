#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-03-02 11:13:18
# @Author  : dutengxiao ()
# @Link    : ${link}
# @Version : $Id$ 
# Get Fundation Net Value History  
# http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
# 

import scrapy
from fundScrapy.items import FundscrapyItem
from fundScrapy.dbUtils import DbUtils
from fundScrapy.parseFunHisJs import FunHisParse 
import traceback

class FundNetvalHistSpider(scrapy.Spider):
	name = 'fundNetvalHist'

	#start_urls = ['http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1']
	
	dbUtil = DbUtils()
	
	# 标志 表示是否清空原来已有数据， 重新下载 ， 适用于非增量情况；
	# 当增量下载时， 设置为Flase
	delFirstThenInsert = False
	
	def start_requests(self):
		curent = 1
		size = 500
		total = 0 



		if (self.delFirstThenInsert):
			print("清空所有的FundHist历史记录 ！")
			self.dbUtil.dropFundHists()

		test = False
		onecode = False

		if (onecode):
			# 进行一个代码的抓取  
			code1 = '161607'

			codelist = self.dbUtil.getFunListByCode(code1)

			for r in codelist:
				total += 1
				code = r.get('code')
				print("开始处理code：", code)
				url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + code + '&page=1&per=1' 
				yield scrapy.Request(url=url, callback=self.parse, meta={'code':code,'fundType':r.get('xtype')})

		elif (test):
			# for test 
			curent = 101
			size = 101
			codelist = self.dbUtil.getFunListByPage(curent, size )

			while codelist is not None and len(codelist) > 0 :
				for r in codelist:
					total += 1
					code = r.get('code')
					print("开始处理code：", code)
					url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + code + '&page=1&per=1' 
					yield scrapy.Request(url=url, callback=self.parse, meta={'code':code,'fundType':r.get('xtype')})

				curent += size
				#codelist = self.dbUtil.getFunListByPage(curent, curent + size - 1)
				codelist = None
		else:
			codelist = self.dbUtil.getFunListByPage(curent, size )

			while codelist is not None and len(codelist) > 0 :
				for r in codelist:
					total += 1
					code = r.get('code')
					print("开始处理code：", code)
					
					url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + code + '&page=1&per=1' 
					yield scrapy.Request(url=url, callback=self.parse, meta={'code':code,'fundType':r.get('xtype')})

				curent += size
				codelist = self.dbUtil.getFunListByPage(curent, curent + size - 1)


		print("总计fundation 个数：", total)

	


	def parse(self, response):
		code = response.meta['code']
		fundType = response.meta['fundType']

		text = response.text

		if (text is None or len(text) == 0 ):
			print("响应结果text内容为空！")
			self.dbUtil.saveCrawLog(code, fundType, 'error', '取总条数response空')
			return

		print(code , ":响应内容changdu为：", len(text))

		try:
			funParse = FunHisParse(text, code)
			res = funParse.parse2()
		except Exception as e:
			traceback.print_exc()
			excstr = traceback.format_exc()
			self.dbUtil.saveCrawLog(code, fundType, 'error', '取总条数response解析异常, ' + excstr)
			raise e


		#print("解析结果为：" , res)

		# 触发新的请求   total records
		records  = res.get('records') 

		print("总条数：", records)

		# 查询历史记录的已有总条数  
		currentcount = self.dbUtil.countFundHist(code, fundType)

		# count neet to get  
		getcount = records - currentcount 

		# update 历史数据更新状态表 
		self.dbUtil.updateFundHistStatus(code, fundType, records, getcount)

		if (getcount == 0):
			print("当前历史数据已经最新，无需下载！", code, currentcount, records)
			return 

		url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + code + '&page=1&per=' + str(getcount)

		print(url)
		yield scrapy.Request(url=url, callback=self.parseAllHist, meta={'code':code, 'records':getcount, 
			'totalcount':records, 'currentcount':currentcount, 'fundType':fundType})

	def parseAllHist(self, response):
		code = response.meta['code']
		records = response.meta['records']
		fundType = response.meta['fundType']
		currentcount = response.meta['currentcount']
		totalcount = response.meta['totalcount']

		text = response.text

		try:
			funParse = FunHisParse(text, code)
			res = funParse.parse2()
		except Exception as e:
			traceback.print_exc()
			excstr = traceback.format_exc()
			self.dbUtil.saveCrawLog(code, fundType, 'error', '取总记录response解析异常, ' + excstr)
			raise e

		# bug001 fix： 这里res中的records是总记录条数 ， records 是差异记录条数 ， 增量下载时两值不同 。 
		# 有后续的入库总记录条数比较足够了！
		#if (res.get('records') != records):
		#	print(code, "解析结果数据总条数不一致 ，请检查！", res.get('records') , records )
		#	self.dbUtil.saveCrawLog(code, fundType, 'error', '取总记录返回条数不一致' + str(records) + "," + str(res.get('records')) )
		#	return

		contentList = res.get('content')

		print(code, "解析结果数据为：", len(contentList) , records )

		item = FundscrapyItem()
		item['dataType'] = 'fundHist'
		item['fundHist'] = contentList 
		item['fundCode'] = code
		item['fundType'] = fundType
		item['insertMany'] = self.delFirstThenInsert

		yield item

		# check total records 
		total = self.dbUtil.countFundHist(code, fundType) 
		if (total != totalcount):
			print("解析入库后总条数与响应总条数不一致！",  total, totalcount, currentcount, records)
			self.dbUtil.saveCrawLog(code, fundType, 'count', '解析入库后总条数与响应总条数不一致！ ' + str(totalcount) + " " + str(total))
			return

		print("解析入库后总条数一致",   total, totalcount, currentcount, records)



####
if __name__ == '__main__':
	# do somethings
	pass
