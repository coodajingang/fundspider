# -*- coding: utf-8 -*-

# fundation list spider  
# spider  ： 获取基金列表 
# url： 'http://fund.eastmoney.com/js/fundcode_search.js' 
# 
# 获取全量的基金列表  
# 返回的是 文本文件 ， 需要进行解析   
# 
# 


import scrapy
from fundScrapy.items import FundscrapyItem

class Funlist(scrapy.Spider):
	name='funlist'
	start_urls =['http://fund.eastmoney.com/js/fundcode_search.js'] 

	def parse(self, response):
		text =  response.text

		if (text is None or len(text) == 0 ):
			print("响应结果text内容为空！")
			return

		# 检查返回结果 是否包含 [[ ]]; 
		try:
		 	start = text.index('[[')
		except Exception as e:
		 	print('响应结果text不包含[[ ，响应结果异常！' , text[:10])
		 	raise e 
		
		try:
			end = text.rindex(']];')
		except Exception as e:
			print('响应结果text不包含]]; ，响应结果异常！' , text[-10:])
			raise e

		print(start , end )

		str2 = text[start : end + 2]
		str2 = str2.strip()

		print(str2[:10]) 
		print(str2[-10:])

		print(len(str2))

		try:
			ll = eval(str2)
		except Exception as e:
			print('转换字符串 为 list 异常 ！' ,e)
			raise e

		
		item = FundscrapyItem()
		item['fundList'] = str2
		item['dataType'] = 'fundList'
		yield item