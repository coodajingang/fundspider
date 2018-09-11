# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FundscrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    dataType = scrapy.Field()
    fundList = scrapy.Field()
    fundHist = scrapy.Field()
    fundCode = scrapy.Field()
    fundType = scrapy.Field()
    insertMany = scrapy.Field()
    
