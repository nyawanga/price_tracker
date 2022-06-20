import time
import random
import re
import datetime
from bs4 import BeautifulSoup as bs
import html5lib

import scrapy

# from mamababyshop.items import MamababyshopItem
from scrapy.utils.project import get_project_settings


class MamaBabyShop(scrapy.Spider):
    name = "mamababyshop"
    download_delay = 2
    allowed_domains = ["motherbabyshop.co.ke"]
    count = 1
    custom_settings = {
        # "LOG_FILE": "logs/carrefour.log",
        "LOG_LEVEL": "DEBUG",
        "FEED_FORMAT": "json",
        "FEED_URI": "./dataset/mamababyshop.json",
    }

    start_urls = [
        "https://motherbabyshop.co.ke/collections",
    ]

    def parse(self, response):
        settings = get_project_settings()
        urls = response.xpath(
            "//ul[contains(@class,'menu center')]/li/a/@href"
        ).extract()
        count = 0
        for url in urls[0:2]:
            if count < 3:
                count += 1
                url = f"{self.start_urls[0]}{url.replace('/collections','')}"
                time.sleep(random.randint(3, 8))
                yield scrapy.Request(url=url, callback=self.parse_products)

    def parse_products(self, response):
        # items = MamababyshopItem()
        items = {}
        listings = response.selector.xpath("//*[contains(@class, 'product-details')]")
        items["rating"] = None
        items["votes"] = None
        items["category"] = None
        items["stock_status"] = "In store"
        category_regex = re.search(
            r"(collections\/)(?P<category>[\w+\-]+)", response.url
        )
        for listing in listings:
            try:
                items["created_at"] = datetime.datetime.strftime(
                    datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
                )
                if category_regex:
                    items["category"] = category_regex.group("category")
                items["name"] = listing.css("span.title::text").extract_first().strip()
                items["sku"] = listing.css(
                    "div.shopify-reviews span::attr(data-id)"
                ).extract_first()
                items["price"] = self.int_regex(
                    listing.css("span.money::text").extract_first().strip(),
                    int_regex="\d*",
                )
                if listing.css("div.sold_out"):
                    stock_status = listing.css("div.sold_out::text").extract_first()
                    if stock_status:
                        items["stock_status"] = stock_status.strip()
                if listing.css(
                    "div.jdgm-prev-badge::attr(data-average-rating)"
                ).extract_first():
                    items["rating"] = listing.css(
                        "div.jdgm-prev-badge::attr(data-average-rating)"
                    ).extract_first()
                if listing.css(
                    "div.jdgm-prev-badge::attr(data-number-of-reviews)"
                ).extract_first():
                    items["votes"] = listing.css(
                        "div.jdgm-prev-badge::attr(data-number-of-reviews)"
                    ).extract_first()
                item_url = listing.xpath(
                    "//a[contains(@class, 'product-info__caption')]/@href"
                ).extract_first()
                items[
                    "item_url"
                ] = f"{self.start_urls[0]}{item_url.replace('/collections','')}"

                yield items

            except AttributeError as err:
                print(f"Attribute error at url {err}")
                pass
            except Exception as err:
                print(err)
                pass

        prev_xpath = "//link[contains(@rel,'prev')]"
        next_page = response.xpath(
            "//link[contains(@rel,'next')]/@href"
        ).extract_first()

        if next_page and self.count < 50:
            self.count += 1
            url = f"{response.urljoin(next_page)}"
            print(url)
            yield scrapy.Request(url=url, callback=self.parse_products)

    def int_regex(self, value, int_regex="\d*\.?\d{0,2}"):
        if value is None:
            return None
        else:
            if isinstance(value, int) or isinstance(value, float):
                return value
            try:
                replace_pattern = re.compile(r"[^\d|\.]+")
                # digit_regex = re.compile(r".+?(?=\.)")
                digit_regex = re.compile(rf"{int_regex}")
                clean_price = replace_pattern.sub("", str(value))
                clean_price = digit_regex.findall(str(clean_price))
                if clean_price:
                    return clean_price[0]
                elif re.search(r"[\d]+", str(value)):
                    return re.search(r"[\d]+", str(value)).group()
            except Exception as err:
                return None
