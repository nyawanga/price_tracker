# https://stackoverflow.com/questions/55501524/how-does-recaptcha-3-know-im-using-selenium-chromedriver/55502835#55502835

import time
import datetime
from bs4 import BeautifulSoup as bs, Tag
import html5lib
import re

import scrapy

# from jumia.items import GroceryItem
from scrapy.utils.project import get_project_settings


class JumiaGroceries(scrapy.Spider):
    name = "jumia_groceries"
    download_delay = 2
    start_urls = ["https://www.jumia.co.ke"]
    count = 1
    allowed_domains = ["jumia.co.ke"]
    custom_settings = {
        # "LOG_FILE": "logs/carrefour.log",
        "LOG_LEVEL": "DEBUG",
        "FEED_FORMAT": "json",
        "FEED_URI": "./dataset/jumia.json",
    }

    def parse(self, response):
        url = f"{self.start_urls[0]}/groceries"
        settings = get_project_settings()
        yield scrapy.Request(url=url, callback=self.parse_groceries)

    def parse_groceries(self, response):
        # items = GroceryItem()
        items = {}
        sources = response.selector.xpath(
            "//section[@class='card -fh']//div[@class='-paxs row _no-g _4cl-3cm-shs']"
        )
        soup = bs(sources.extract_first(), "html5lib")
        articles = soup.find_all("article", {"class": "prd _fb col c-prd"})
        for idx, article in enumerate(articles):
            try:
                items["created_at"] = datetime.datetime.strftime(
                    datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
                )
                items["sku"] = article.find("a", {"class": "core"}).get("data-id")
                items["brand"] = article.find("a", {"class": "core"}).get("data-brand")
                items["category"] = article.find("a", {"class": "core"}).get(
                    "data-category"
                )

                info = article.find("a", {"class": "core"}).find(
                    "div", {"class": "info"}
                )
                items["name"] = self.__safe_parsing(info.find("h3", {"class": "name"}))
                items["price"] = self.int_regex(info.find("div", {"class": "prc"}).text)
                items["rating"] = self.__safe_parsing(
                    info.find("div", {"class": "rev"}),
                    sibling_search={
                        "tag": "div",
                        "params": {"attr": "class", "value": "stars _s"},
                    },
                )
                items["voters"] = self.__safe_parsing(
                    info.find("div", {"class": "rev"}),
                )
                items["prev_price"] = self.__safe_parsing(
                    info.find("div", {"class": "s-prc-w"}),
                    sibling_search={
                        "tag": "div",
                        "params": {"attr": "class", "value": "old"},
                    },
                )
                items["discount"] = self.__safe_parsing(
                    info.find("div", {"class": "s-prc-w"}),
                    sibling_search={
                        "tag": "div",
                        "params": {"attr": "class", "value": "bdg _dsct _sm"},
                    },
                )
                item_url = soup.find("a", {"class": "core"}).get("href")
                items["item_url"] = f"{self.start_urls[0]}{item_url}"

                yield items

            except AttributeError as err:
                # raise
                print(f"Attribute error at url {response.url} item number {idx}")
                pass
            except Exception as err:
                print(err)
                pass

        next_page = response.selector.xpath(
            "//*[contains(@class, 'pg-w -ptm -pbxl')]//a[@aria-label='Next Page']/@href"
        ).extract_first()

        if next_page and self.count < 51:
            url = f"{self.start_urls[0]}{next_page}"
            self.count += 1
            print(f"\nGoing to next page {self.count}\n")
            yield scrapy.Request(url=url, callback=self.parse_groceries)

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

    def __safe_parsing(self, parsing, sibling_search=None) -> str:
        """
        This allows me to parse the HTML safely without having a breaking on my cralwer.

        :param parsing: either a beautifulsoup Tag or text
        :param sibling_search: a dictionary to allow for the next sibling search avoid attribute error on find method

        returns: string or None if e do not find what we are looking for
        """
        try:
            if sibling_search and parsing is not None:
                return self.__safe_parsing(
                    parsing.find(
                        f'{sibling_search.get("tag")}',
                        {
                            f'{sibling_search.get("params").get("attr")}': f'{sibling_search.get("params").get("value")}'
                        },
                    )
                )
            elif isinstance(parsing, str):
                return parsing
            elif isinstance(parsing, Tag):
                return parsing.text
        except AttributeError:
            return None
