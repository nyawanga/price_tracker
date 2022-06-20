import time
from bs4 import BeautifulSoup as bs
import datetime
import html5lib
import json
import ast
import re
import copy
import random
from fuzzywuzzy import process

import scrapy
from scrapy.http.request import Request

# from mydawa.items import PharmacyItem

# https://www.youtube.com/watch?v=ThKiZjLNN8Y&list=PLj4hN6FewnwrQ3Soq1e2MKISnoWyPkWfB&index=5
# https://www.youtube.com/watch?v=2LwrUu9yTAo
# https://www.youtube.com/watch?v=Pu3gmdWsLYc


class BabyProducts(scrapy.Spider):
    name = "mydawa_pharmacy"
    download_delay = 2
    allowed_domains = ["mydawa.com"]
    start_urls = ["https://mydawa.com/products"]
    main_url = "https://mydawa.com/_local/ajax.aspx/GetProductsApi"
    start_page_no = 0
    brand_list = {}
    current_brand = None
    custom_settings = {
        # "LOG_FILE": "logs/carrefour.log",
        "LOG_LEVEL": "DEBUG",
        "FEED_FORMAT": "json",
        "FEED_URI": "./dataset/mydawa.json",
    }

    # headers = {
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
    #     "Accept": "*/*",
    #     "Accept-Language": "en-US,en;q=0.5",
    #     "Content-Type": "application/json; charset=utf-8",
    #     "X-Requested-With": "XMLHttpRequest",
    #     "Origin": "https://mydawa.com",
    #     "Connection": "keep-alive",
    #     "Referer": "https://mydawa.com/products",
    #     # "Cookie": "ASP.NET_SessionId=bgmqdpxnjglxzwtyzwgzh3ji; GtaIDx=d2965d47-4a7c-4959-9a3f-d8040d983967; currentCountry=KE; __kla_id=eyIkcmVmZXJyZXIiOnsidHMiOjE2Mzg1NzA0MDIsInZhbHVlIjoiaHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS8iLCJmaXJzdF9wYWdlIjoiaHR0cHM6Ly9teWRhd2EuY29tLyJ9LCIkbGFzdF9yZWZlcnJlciI6eyJ0cyI6MTYzODU3MDQ2NCwidmFsdWUiOiJodHRwczovL3d3dy5nb29nbGUuY29tLyIsImZpcnN0X3BhZ2UiOiJodHRwczovL215ZGF3YS5jb20vIn19; editmode=; _clck=emxzcg|1|ewy|0; _clsk=cu7jpv|1638570466144|2|1|f.clarity.ms/collect",
    #     "Sec-Fetch-Dest": "empty",
    #     "Sec-Fetch-Mode": "cors",
    #     "Sec-Fetch-Site": "same-origin",
    #     "Cache-Control": "max-age=0",
    #     "TE": "trailers",
    # }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json; charset=utf-8",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        # "Origin": "https://mydawa.com",
        # "Connection": "keep-alive",
    }

    payload = {
        "ItemsPerPage": "600",
        "PageNumber": "",
        "Keyword": "",
        "BrandId": "",
        "CategoryId": "",
        "MinPrice": "0",
        "MaxPrice": "0",
    }

    def parse(self, response):
        category_ids = ["8223", "8225"]
        for category_id in category_ids:
            payload = copy.deepcopy(self.payload)
            payload["PageNumber"] = str(self.start_page_no)

            payload["CategoryId"] = category_id

            yield scrapy.Request(
                url=self.main_url,
                callback=self.parse_brands,
                headers=self.headers,
                body=json.dumps(payload),
                method="POST",
            )

    def parse_brands(self, response):
        data = json.loads(response.body)
        root_data = data.get("d")
        if root_data:
            root_data = json.loads(root_data)
            status = root_data.get("status")
            brands = root_data.get("brands")

            if status == "success" and not self.brand_list:
                brands_soup = bs(brands, "html5lib")
                brands_items = brands_soup.find_all("li")
                [
                    self.brand_list.update(
                        {brand.get("data-brandid"): brand.get("data-name")}
                    )
                    for brand in brands_items
                ]

        payload = copy.deepcopy(self.payload)
        # self.start_page_no += 1
        payload["PageNumber"] = str(self.start_page_no)
        # next_brand_id = list(self.brand_list.keys())[0]
        # payload["BrandId"] = next_brand_id
        # self.current_brand = self.brand_list.get(next_brand_id)
        # self.brand_list.pop(next_brand_id)
        yield scrapy.Request(
            url=self.main_url,
            callback=self.parse_products,
            headers=self.headers,
            body=json.dumps(payload),
            method="POST",
        )

    def parse_products(self, response):
        # items = PharmacyItem()
        items = {}
        data = json.loads(response.body)

        root_data = data.get("d")
        if root_data:
            root_data = json.loads(root_data)

        status = root_data.get("status")
        products = root_data.get("products")
        brands = root_data.get("brands")
        brand_list = [i.lower() for i in set(self.brand_list.values())]

        if status == "success":
            soup = bs(products, "html5lib")
            products = soup.find_all("div", {"class": "col-sm-4"})
            print(f"Length of results : {len(products)}")
            for product in products:
                prod_item = product.find("div", class_=re.compile("^product-item"))
                items["created_at"] = datetime.datetime.strftime(
                    datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
                )
                items["name"] = (
                    prod_item.find("h2", {"class": "prd-titlel"}).find("a").get("title")
                )
                items["brand"] = process.extract(items["name"].lower(), brand_list)[0][
                    0
                ]
                items["brand_match_ratio"] = str(
                    process.extract(items["name"].lower(), brand_list)[0][1]
                )
                items["category"] = prod_item.get("data-category")
                items["link"] = (
                    prod_item.find("h2", {"class": "prd-titlel"}).find("a").get("href")
                )
                prev_price = prod_item.find("div", {"class": "prd-price"}).find(
                    "div", {"class": "price pricesale"}
                )
                if prev_price:
                    items["prev_price"] = str(self.int_regex(prev_price.text))
                price = (
                    prod_item.find("div", {"class": "prd-price"})
                    .find("div", {"class": "price"})
                    .text
                )
                if price:
                    items["price"] = str(self.int_regex(price))
                items["item_url"] = prod_item.find(
                    "a", class_=re.compile("prd-description")
                ).get("href")

                yield items

            if len(products) >= 300:
                payload = copy.deepcopy(self.payload)
                self.start_page_no += 1
                payload["PageNumber"] = str(self.start_page_no)
                time.sleep(random.randint(3, 8))

                yield scrapy.Request(
                    url=self.main_url,
                    callback=self.parse_products,
                    headers=self.headers,
                    body=json.dumps(payload),
                    method="POST",
                )

        else:
            exit(0)

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

    # def parse_products(self, response):
    #     items = PharmacyItem()
    #     data = json.loads(response.body)

    #     root_data = data.get("d")
    #     if root_data:
    #         root_data = json.loads(root_data)

    #     status = root_data.get("status")
    #     products = root_data.get("products")
    #     brands = root_data.get("brands")

    #     if status == "success":
    #         soup = bs(products, "html5lib")
    #         products = soup.find_all("div", {"class": "col-sm-4"})
    #         print(f"Length of results : {len(products)}")
    #         for product in products:
    #             prod_item = product.find("div", class_=re.compile("^product-item"))
    #             items["created_at"] = datetime.datetime.strftime(
    #                 datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
    #             )
    #             items["brand"] = self.current_brand
    #             items["name"] = (
    #                 prod_item.find("h2", {"class": "prd-titlel"}).find("a").get("title")
    #             )
    #             items["data_category"] = prod_item.get("data-category")
    #             items["link"] = (
    #                 prod_item.find("h2", {"class": "prd-titlel"}).find("a").get("href")
    #             )
    #             prev_price = prod_item.find("div", {"class": "prd-price"}).find(
    #                 "div", {"class": "price pricesale"}
    #             )
    #             if prev_price:
    #                 items["prev_price"] = prev_price.text
    #             items["price"] = (
    #                 prod_item.find("div", {"class": "prd-price"})
    #                 .find("div", {"class": "price"})
    #                 .text
    #             )
    #             items["desc_url"] = prod_item.find(
    #                 "a", class_=re.compile("prd-description")
    #             ).get("href")

    #             yield items

    #         payload = copy.deepcopy(self.payload)
    #         self.start_page_no += 1
    #         payload["PageNumber"] = str(self.start_page_no)
    #         if len(products) < 300:
    #             next_brand_id = list(self.brand_list.keys())[0]
    #             payload["BrandId"] = next_brand_id
    #             self.current_brand = self.brand_list.get(next_brand_id)
    #             self.brand_list.pop(next_brand_id)
    #             self.start_page_no = 0

    #         payload["PageNumber"] = str(self.start_page_no)
    #         time.sleep(random.randint(3, 8))

    #         yield scrapy.Request(
    #             url=self.main_url,
    #             callback=self.parse_products,
    #             headers=self.headers,
    #             body=json.dumps(payload),
    #             method="POST",
    #         )

    #     else:
    #         exit(0)
