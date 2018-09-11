#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-03-02 11:37:24
# @Author  : dutengxiao ()
# @Link    : ${link}
# @Version : $Id$

import pymongo
import datetime
import fundScrapy.settings


class DbUtils(object):

	def __init__(self):
		print("in DbUtils __init__ ")
		self.mongourl = fundScrapy.settings.MONGO_URL
		self.mongodb = fundScrapy.settings.MONGO_DB
		self.mongouser = fundScrapy.settings.MONGO_USER
		self.mongopwd = fundScrapy.settings.MONGO_PWD
		print(self.mongourl, self.mongodb, self.mongouser, self.mongopwd)
		self.open_spider()
		

	@classmethod
	def from_crawler(cls, crawler):
		print("in DbUtils from_crawler ")
		# return cls(mongurl=crawler.settings.get('MONGO_URL'), mongdb=crawler.settings.get('MONGO_DB'))
		return cls()

	def open_spider(self):
		print("in DbUtils open_spider ")
		self.client = pymongo.MongoClient(self.mongourl)
		self.db = self.client[self.mongodb]
		self.db.authenticate(self.mongouser, self.mongopwd)

	def close_spider(self):
		self.client.close()

	def process_item(self, item, spider):
		return item


	def getFunListByPage(self, start, end, page=200, filters=None):
		#查询基金信息列表 FundList ， 分页查询  ， 根据查询条件查询  ， 返回list  
		fundCol = self.db['FundList']
		res = fundCol.find(filters).limit(end-start+1).skip(start - 1)
		reslist = []

		for r in res:
			reslist.append(r)
		print("本次获取列表个数为，", len(reslist) , " 分页参数：", start, end, page, filters)
		return reslist


	def getFunCodeByPage(self, start, end, page=200, filters=None):
		#查询基金代码列表 FundList， 分页查询  ， 根据查询条件查询  ， 返回list  
		fundCol = self.db['FundList']
		res = fundCol.find(filters).limit(end-start+1).skip(start - 1)
		reslist = []

		for r in res:
			reslist.append(r.get('code'))
		print("本次获取列表个数为，", len(reslist) , " 分页参数：", start, end, page, filters)
		return reslist

	def getFunListByCode(self, code):
		fundCol = self.db['FundList'] 
		reslist = []

		for x in fundCol.find({'code':code}):
			reslist.append(x)
		print("本次获取列表个数为，", len(reslist) , " 参数：", code)
		return reslist
		
	def saveCrawLog(self, code, typestr, status, records):
		# 只记录 error 日志  
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 

		fundCol = self.db['CrawlerLog']
		fundCol.insert_one({'code':code, 'status':status, 'type':typestr , 'records':records, 'timestamp':timestamp})

	def countFundHist(self, code, fundType):
		fundCol = self.db['FundHist' + fundType]

		filterstr = {'code':code , 'fundType':fundType}

		return fundCol.find(filterstr).count() 

	def updateFundHistStatus(self, code, fundType, total, incr ):
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 
		date = datetime.datetime.now().strftime('%Y-%m-%d')

		data = {'date':date , 'code':code , 'fundType':fundType , 'totoal':total, 'incr':incr, 'timestamp':timestamp}

		fundCol = self.db['FundHistStatus']

		print("更新Fundation 历史数据状态表：", data)
		fundCol.update_one({'code':code} ,{'$set':data}, upsert = True)

	def dropFundHists(self):
		colNames = self.db.collection_names() 

		for name in colNames:
			if (name.startswith('FundHist')):
				col = self.db[name]
				print("清空表：", name)
				col.drop()

		print("清空错误日志表：CrawlerLog")
		col = self.db['CrawlerLog']
		col.drop()



####
if __name__ == '__main__':
	# do somethings
	dbUtil = DbUtils()

	curent = 1
	size = 500

	total = 0 

	codelist = dbUtil.getFunListByPage(curent, size )

	while codelist is not None and len(codelist) > 0 :
		for r in codelist:
			total += 1
			print(r.get('code'))

		curent += size
		codelist =dbUtil.getFunListByPage(curent, curent + size -1 )
	
	print("总计fundation 个数：", total)