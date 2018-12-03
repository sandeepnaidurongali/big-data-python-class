# -*- coding: utf-8 -*-
import scrapy

class nbaItem(scrapy.Item):
    title = scrapy.Field()
    link = scrapy.Field()
    resp = scrapy.Field()
class NbaSpider(scrapy.Spider):
    name = 'ba'
    allowed_domains = ['amazon.com']
    start_urls = ['http://www.nba.com/scores#/','https://stats.nba.com/schedule/','http://www.nba.com/news','https://stats.nba.com/']
    
    def parse(self, response):
        res = scrapy.Selector(response)
        titles = res.xpath('//ul/li')
        items = []
        for title in titles:
            item = nflItem()
            item["title"] = title.xpath("a/text()").extract()
            item["link"] = title.xpath("a/@href").extract()
            item["resp"] = response
            if item["title"] != []:
                items.append(item)

        return items
