# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import datetime

class MongoPipeline(object):

	def __init__(self, mongurl, mongdb, monguser, mongpwd):
		self.mongourl = mongurl
		self.mongodb = mongdb
		self.mongouser = monguser
		self.mongopwd = mongpwd

	@classmethod
	def from_crawler(cls, crawler):
		return cls(mongurl=crawler.settings.get('MONGO_URL'), mongdb=crawler.settings.get('MONGO_DB'), \
		monguser=crawler.settings.get("MONGO_USER"), mongpwd=crawler.settings.get('MONGO_PWD'))

	def open_spider(self, spider):
		print("In Mongo pipeline:", self.mongourl, self.mongodb, self.mongouser, self.mongopwd)
		self.client = pymongo.MongoClient(self.mongourl)
		
		self.db = self.client[self.mongodb]
		self.db.authenticate(self.mongouser , self.mongopwd)

	def close_spider(self, spider):
		self.client.close()

	def process_item(self, item, spider):
		datatype = item['dataType']

		if datatype == 'fundList':
			self.saveFundList2Db(item['fundList'])
		elif (datatype == 'fundHist'):
			# 保存历史数据  
			code = item['fundCode']
			contentList = item['fundHist']
			fundType = item['fundType']
			insertMany = item['insertMany']

			self.saveFundHist2Db(code, fundType, contentList, insertMany)



		return item

	def saveFundHist2Db(self, code, fundType, contentList, insertMany):
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

		print("开始进行保存Fundation Hist 到数据库，条数为：", len(contentList), " 时间戳：", timestamp)
		
		fundCol = self.db['FundHist' + fundType] 

		title = contentList[0]

		titlelen = len(title)

		titledict = {}

		for i in range(titlelen):
			titledict.update({i:self.getTitleName(title,i)})

		contentList = contentList[1:]

		total = 0 

		if (insertMany):
			datalist = []

			for content in contentList:
				data = {}
				total += 1
				for i in range(titlelen):
					tt = titledict.get(i)
					data.update({tt:content[i]}) 
				data['code'] = code
				data['fundType'] = fundType
				data['timestamp'] = timestamp
				datalist.append(data)

			print("准备保存数据insertmany：", code, len(datalist) )
			fundCol.insert_many(datalist)

		else:
			for content in contentList:
				data = {}
				total += 1
				for i in range(titlelen):
					tt = titledict.get(i)
					data.update({tt:content[i]}) 
				data['code'] = code
				data['fundType'] = fundType
				data['timestamp'] = timestamp

				ddlres = fundCol.replace_one({'code':code , 'date':data.get('date')}, data, upsert=True)
				print(code , ddlres.raw_result)

		print("保存历史数据完成：FundHist", fundType, total)

	def getTitleName(self, title, i):
		tt = title[i]
		if (tt == '净值日期'): 
			return 'date'
		if (tt == '单位净值'): 
			return 'pernet'
		if (tt == '累计净值'): 
			return 'addnet'
		if (tt == '日增长率'): 
			return 'daypercent'
		if (tt == '申购状态'): 
			return 'purchase'
		if (tt == '赎回状态'): 
			return 'redeem'
		if (tt == '分红送配'): 
			return 'dividend'
		if (tt == '每万份收益'):
			return 'rateofwan'
		if (tt.startswith('7日年化收益')):
			return 'rateof7day'
		if (tt == '最近运作期年化收益率'):
			return 'rateofyear'
		return 'other' + str(i)

	@DeprecationWarning
	def getCollation(self, fundType):
		if (fundType == ''):
			return self.db['']

	def saveFundList2Db(self, fundStr):
		if (fundStr is None or len(fundStr) == 0):
			print("读取到的 fundation list 为空！") 
			return 

		ll = eval(fundStr)

		print(len(ll)) 

		fundCol = self.db['FundList']

		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

		totoal = 0 

		print("开始进行保存Fundation List 到数据库，条数为：", len(ll), " 时间戳：", timestamp)
		for x in ll:
			# 使用 replace_one 方法， 查询到则替换 ，查询不到则插入新的 
			data = {
				'code':x[0],
				'abbr':x[1],
				'name':x[2],
				'type':x[3],
				'ename':x[4],
				'xtype': self.getXtype(x[3]),
				'tm':timestamp
			}
			totoal += 1
			res = fundCol.replace_one({'code':x[0]}, data, upsert=True)
			print(x[0], res.raw_result)

		print("保存Fundation list 到数据库完成！", len(ll), totoal)

	def getXtype(self, fundType):
		if ( fundType == '混合型'):
			return 'hhx'
		if ( fundType == '债券型'):
			return 'zqx'
		if ( fundType == '定开债券'):
			return 'zkx'
		if ( fundType == '联接基金'):
			return 'ljx'
		if ( fundType == '货币型'):
			return 'hbx'
		if ( fundType == '债券指数'):
			return 'zqzs'
		if ( fundType == '保本型'):
			return 'bbx'
		if ( fundType == '理财型'):
			return 'lcx'
		if ( fundType == 'QDII'):
			return 'qdii'
		if ( fundType == '股票指数'):
			return 'gpzs'
		if ( fundType == 'QDII-指数'):
			return 'qdiizs'
		if ( fundType == '股票型'):
			return 'gpx'
		if ( fundType == '固定收益'):
			return 'gdsy'
		if ( fundType == '分级杠杆'):
			return 'fjgg'
		if ( fundType == '其他创新'):
			return 'qtcx'
		if ( fundType == '混合-FOF'):
			return 'hhfof'
		if ( fundType == 'ETF-场内'):
			return 'etfcn'
		if ( fundType == 'QDII-ETF'):
			return 'qdiietf'
		if ( fundType == '债券创新-场内'):
			return 'zqcxcn'
		return 'other'


		

class FundscrapyPipeline(object):
    def process_item(self, item, spider):
        return item

