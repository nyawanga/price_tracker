from scrapy.crawler import CrawlerProcess
from scrapy import spiderloader
from scrapy.utils import project
import argparse

PARSER = argparse.ArgumentParser(
    description="A crawler for ecommerce website to get data for household item price tracking"
)
PARSER.add_argument(
    "--crawlers",
    type=str,
    action="append",
    default=None,
    help="add the cralwers to run default is all of them",
)

ARGS, _ = PARSER.parse_known_args()
crawlers = ARGS.crawlers


def main():
    settings = project.get_project_settings()
    spider_loader = spiderloader.SpiderLoader.from_settings(settings)
    spiders = spider_loader.list()
    classes = [spider_loader.load(name) for name in spiders]

    process = CrawlerProcess(settings)
    for cls in classes:
        if crawlers:
            if cls.name in crawlers:
                process.crawl(cls)
        else:
            process.crawl(cls)
    process.start()


if __name__ == "__main__":
    main()
