# -*- coding: utf-8 -*-
# @Date    : 2018-03-02 11:13:18
# @Author  : dutengxiao ()
# @Link    : ${link}
# @Version : $Id$ 
# 目前url返回的fundation 历史行情都是js 格式的  
# 形如： var apidata={ content:"
# 净值日期	单位净值	累计净值	日增长率	申购状态	赎回状态	分红送配
# 2018-03-02	1.1210	3.5320	-0.88%	开放申购	开放赎回	
# 2018-03-01	1.1310	3.5420	0.89%	开放申购	开放赎回	
# ",records:3927,pages:1964,curpage:1};
# 
#  提供解析器 ， 结息数据后 返回 字典格式  

import re
import demjson
from bs4 import BeautifulSoup

class FunHisParse(object):
	def __init__(self, jsstr, code):
		self.jsstr = jsstr
		self.code = code

	def parse(self):
		resdict = {}

		if (self.jsstr is None or len(self.jsstr) == 0):
			return resdict

		try:
			self.format2json2()
			djson = demjson.decode(self.jsstr)

		except Exception as e:
			print("解析jsstr 到 json异常  ！",self.code, e)
			raise e
			

		content = djson.get('content')

		djson['content'] = self.parseContentHtml(content)

		return djson
	def parse2(self):
		# print("parse jsstr")
		boup = BeautifulSoup(self.jsstr, 'html.parser')
		bodys = boup.findAll('body')
		body = bodys[0]
		# print(body) 
		# 解析字符串 
		try:
			content = self.format2json(body.getText())
			djson = demjson.decode(content)
		except Exception as e:
			print("解析jsstr 到 json异常  ！",self.code, e)
			raise e

		# 解析table 
		tabs = boup.findAll('table')
		tab = tabs[0]

		trs = tab.findAll('tr')
		tr0 = trs[0]

		ths = tr0.findAll('th')
		res = []
		head = [] 

		for th in ths:
			head.append(th.getText()) 

		res.append(head)

		trss = trs[1:]

		for tr in trss:
			trlist = [] 
			tds = tr.findAll('td')
			for td in tds:
				trlist.append(td.getText())
			res.append(trlist)
		
		djson['content'] = res

		return djson		

	def format2json2(self):
		if (self.jsstr is None or len(self.jsstr) == 0):
			return 
		self.jsstr  = self.jsstr.strip()
		# print(self.jsstr)
		ind = self.jsstr.index('=')
		if (ind > 0) :
			self.jsstr = self.jsstr[ind + 1:-1]
			self.jsstr = self.jsstr.strip()
			self.jsstr = self.jsstr.replace('\n','\\n')
			print("开头字符串：", self.jsstr[:5] , "结尾字符串：", self.jsstr[-5:])
		else:
			print("未找到=号，无法解析！")
			raise Exception("非jsstr 没找到=号，无法解析！")

	def format2json(self, content):
		if (content is None or len(content) == 0):
			return content
		content  = content.strip()
		# print(self.jsstr)
		ind = content.index('=')
		if (ind > 0) :
			content = content[ind + 1:-1]
			content = content.strip()
			content = content.replace('\n','\\n')
			print("开头字符串：", content[:15] , "结尾字符串：", content[-15:])
		else:
			print("未找到=号，无法解析！")
			raise Exception("非jsstr 没找到=号，无法解析！")
		return content
	
	def parseContentHtml(self, content):
		if (content is None or len(content) == 0):
			return []

		boup = BeautifulSoup(content, 'html.parser')
		tabs = boup.findAll('table')

		tab = tabs[0]

		trs = tab.findAll('tr')

		tr0 = trs[0]

		ths = tr0.findAll('th')

		res = []
		head = [] 

		for th in ths:
			head.append(th.getText()) 

		res.append(head)

		trss = trs[1:]

		for tr in trss:
			trlist = [] 
			tds = tr.findAll('td')
			for td in tds:
				trlist.append(td.getText())
			res.append(trlist)
		return res



	def parseContent(self, content):
		if (content is None or len(content) == 0):
			return []
		splits = content.split("\n")
		res = []

		for line in splits:
			if (len(line) > 0):
				linesplits =  line.split("\t")
				#print(linesplits)
				res.append(linesplits)
				#print("res:", res)
		return res


if __name__ == '__main__':
	jsstr = '''
	var apidata={ content:"
净值日期	单位净值	累计净值	日增长率	申购状态	赎回状态	分红送配
2018-03-02	1.1210	3.5320	-0.88%	开放申购	开放赎回	
2018-03-01	1.1310	3.5420	0.89%	开放申购	开放赎回	
",records:3927,pages:1964,curpage:1};
	'''
	jsstr2 = '''var apidata={ content:"<table class='w782 comm lsjz'><thead><tr><th class='first'>净值日期</th><th>单位净值</th><th>累计净值</th><th>日增长率</th><th>申购状态</th><th>赎回状态</th><th class='tor last'>分红送配</th></tr></thead><tbody><tr><td>2018-03-05</td><td class='tor bold'>1.1170</td><td class='tor bold'>3.5280</td><td class='tor bold grn'>-0.36%</td><td>开放申购</td><td>开放赎回</td><td class='red unbold'></td></tr></tbody></table>",records:3928,pages:3928,curpage:1};
'''
	jsstr3 = '''<html><head></head><body>var apidata={ content:"<table class="w782 comm lsjz"><thead><tr><th class=
"first">净值日期</th><th>单位净值</th><th>累计净值</th><th>日增长率</th><th>申购状态</th><th>赎回状态</th><th class="tor
 last">分红送配</th></tr></thead><tbody><tr><td>2018-09-10</td><td class="tor bold">1.0080</td><td class="tor bold">2.60
10</td><td class="tor bold grn">-1.18%</td><td>开放申购</td><td>开放赎回</td><td class="red unbold"></td></tr></tbody></
table>",records:3250,pages:3250,curpage:1};</body></html>'''
	fun = FunHisParse(jsstr3, '123456789')

	print(fun.parse2())
