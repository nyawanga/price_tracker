from email import header
import time
from bs4 import BeautifulSoup as bs
import datetime
import html5lib
import json
import ast
import re
from pathlib import Path
import os

import scrapy

# from carrefour.items import CarrefourBabyProducts


# from scrapy.utils.project import get_project_settings

# from scrapy.http import Request, FormRequest
# from scrapy.linkextractors import LinkExtractor
# from scrapy.spiders import CrawlSpider, Rule

# https://www.youtube.com/watch?v=2LwrUu9yTAo
# https://www.youtube.com/watch?v=Pu3gmdWsLYc


class BabyProducts(scrapy.Spider):
    # handle_httpstatus_list = [403, 404]
    name = "carrefour"
    download_delay = 2
    allowed_domains = ["carrefour.ke"]
    base_url = "https://www.carrefour.ke"
    start_urls = ["https://www.carrefour.ke/mafken/en/c/FKEN1000000"]
    execution_dir = Path(os.path.abspath(__file__)).parents[0]
    scrapping_date = datetime.datetime.strftime(
        datetime.datetime.now().date(), "%Y%m%d"
    )
    custom_settings = {
        # "LOG_FILE": "logs/carrefour.log",
        "LOG_LEVEL": "INFO",
        "FEED_FORMAT": "json",
        "FEED_URI": f"./datasets/base/{name}/{scrapping_date}-%(batch_id)03d.json",
        "FEED_EXPORT_BATCH_ITEM_COUNT": 1000,
        # "FEED_URI_PARAMS": "myproject.utils.uri_params",
    }

    headers = {
        "Host": "www.carrefour.ke",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "env": "prod",
        "appId": "Reactweb",
        "userid": "undefined",
        "token": "undefined",
        "deviceId": "904460405.1655922412",
        "credentials": "include",
        "storeId": "mafken",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Referer": "https://www.carrefour.ke/mafken/en/c/FKEN1700000",
        "Connection": "keep-alive",
    }

    def parse(self, reponse):
        url = "https://www.carrefour.ke/api/v7/categories/FKEN1700000?filter=&sortBy=relevance&currentPage=1&pageSize=60&maxPrice=&minPrice=&areaCode=Westlands%20-%20Nairobi&lang=en&displayCurr=KES&latitude=-1.2672236834605626&longitude=36.810586556760555&nextOffset=0&responseWithCatTree=true&depth=3 HTTP/2"
        yield scrapy.Request(
            url=url, callback=self.parse_products, headers=self.headers
        )

    def parse_products(self, response):
        # items = CarrefourBabyProducts()
        items = {}
        page = response.body
        data = json.loads(page)
        for idx, item in enumerate(data.get("products")):
            items["created_at"] = datetime.datetime.strftime(
                datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
            )
            items["source_id"] = item.get("id")
            items["sku"] = item.get("ean")
            items["category"] = item.get("category", [{"name": None}])[0].get("name")
            items["name"] = item.get("name")
            items["brand"] = item.get("brand", {"name": None}).get("name", None)
            items["item_size"] = item.get("size")
            price = item.get("price", {"formattedValue": "KES00.00"}).get(
                "formattedValue"
            )
            if price:
                items["price"] = str(self.int_regex(price, int_regex="\d*"))
            items["item_type"] = item.get("type", None)
            items["stock_status"] = item.get(
                "stock", {"stockLevelStatus": "outOfStock", "value": 2}
            ).get("stockLevelStatus")
            items["no_of_stock"] = str(
                item.get("stock", {"stockLevelStatus": "outOfStock", "value": 0}).get(
                    "value"
                )
            )
            items["category_hierarchy"] = item.get("productCategoriesHearchi", None)
            item_url = (
                item.get("links", {"productUrl": {"href": None}})
                .get("productUrl", {"href": None})
                .get("href", None)
            )
            if item_url:
                items["item_url"] = f"{self.base_url}{item_url}"
            items["origin"] = item.get("productOrigin")
            items["supplier"] = item.get("supplier")
            items["type"] = item.get("type")
            items["availability_status"] = items.get(
                "availability", {"isAvailable": False, "max": 0}
            ).get("isAvailable")
            items["availability_max"] = items.get(
                "availability", {"isAvailable": False, "max": 0}
            ).get("max")

            yield items

        pagination = data.get("pagination", {"totalPages": 1, "currentPages": 1})
        if pagination.get("totalPages") > pagination.get("currentPage"):
            next_page = pagination.get("currentPage") + 1
            print(f"\nMoving to next page {next_page} \n")
            url = f"https://www.carrefour.ke/api/v7/categories/FKEN1700000?filter=&sortBy=relevance&currentPage={next_page}&pageSize=60&maxPrice=&minPrice=&areaCode=Westlands%20-%20Nairobi&lang=en&displayCurr=KES&latitude=-1.2672236834605626&longitude=36.810586556760555&nextOffset=0&responseWithCatTree=true&depth=3 HTTP/2"
            yield scrapy.Request(
                url=url, callback=self.parse_products, headers=self.headers
            )

    def int_regex(self, value, int_regex="\d*\.?\d{0,2}"):
        if value is None:
            return None
        else:
            if isinstance(value, int) or isinstance(value, float):
                return value
            try:
                replace_pattern = re.compile(r"[^\d|\.]+")
                # digit_regex = re.compile(r".+?(?=\.)")
                digit_regex = re.compile(fr"{int_regex}")
                clean_price = replace_pattern.sub("", str(value))
                clean_price = digit_regex.findall(str(clean_price))
                if clean_price:
                    return clean_price[0]
                elif re.search(r"[\d]+", str(value)):
                    return re.search(r"[\d]+", str(value)).group()
            except Exception as err:
                return None
