from scrapy.crawler import CrawlerProcess
from scrapy import spiderloader
from scrapy.utils import project

settings = project.get_project_settings()
spider_loader = spiderloader.SpiderLoader.from_settings(settings)
spiders = spider_loader.list()
classes = [spider_loader.load(name) for name in spiders]

process = CrawlerProcess(settings)
for cls in classes:
    if cls.name is not None:  # == "carrefour_babyproducts":
        # print(cls.name)
        process.crawl(cls)
process.start()
