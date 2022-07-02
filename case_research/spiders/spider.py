from string import ascii_uppercase
from urllib.parse import urljoin

import scrapy


INITIAL_URL = "https://casesearch.courts.state.md.us/casesearch/"
SEARCH_PAGE_URL = "https://casesearch.courts.state.md.us/casesearch/processDisclaimer.jis"
SEARCH_POST_URL = "https://casesearch.courts.state.md.us/casesearch/inquirySearch.jis"


# Helper exception
class LettersExhausted(BaseException):
    pass


class CaseSpider(scrapy.Spider):
    first: str = None  # First Name Letter
    last: str = None  # Last Name Letter

    def parse(self, response, **kwargs):
        pass

    name = "case_research"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/102.0.0.0 Safari/537.36',
        'sec-ch-device-memory': '8',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-full-version-list': '" Not A;Brand";v="99.0.0.0", "Chromium";v="102.0.5005.115", "Google '
                                       'Chrome";v="102.0.5005.115"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
    }

    def get_next_letters(self):
        """
        Helper method to iterate over uppercase and lowercase letters
        """
        if not self.first or not self.last:
            self.first = ascii_uppercase[0]
            self.last = ascii_uppercase[0]
            return
        idx = ascii_uppercase.find(self.first)
        if idx + 1 >= len(ascii_uppercase):
            idx = ascii_uppercase.find(self.last)
            if idx + 1 >= len(ascii_uppercase):
                raise LettersExhausted()
            self.first = ascii_uppercase[0]
            self.last = ascii_uppercase[idx + 1]
        else:
            self.first = ascii_uppercase[idx + 1]

        self.logger.debug(f"First Name set to: {self.first}%")
        self.logger.debug(f"Last Name set to: {self.last}%")

    def start_requests(self):
        yield self.next_request()

    def next_request(self):
        # Load the next letters to search. If we already tested everything, we just skip
        try:
            self.get_next_letters()
        except LettersExhausted:
            return

        return scrapy.Request(
            url=INITIAL_URL,
            callback=self.parse_disclaimer_page,
            headers=self.headers,
            dont_filter=True,
        )

    def extract_element(self, response, field_names):
        # convert to list if one input is given
        if not isinstance(field_names, list):
            if isinstance(field_names, str):
                field_names = [field_names]
        # check for field names and try matching
        for field_name in field_names:
            element = self.extract_field(response, field_name)
            if element:
                return element
            element = self.extract_sub_field(response, field_name)
            if element:
                return element

        # no matches, return none
        return None

    def extract_field(self, response, field_name):
        if "State" in field_name:
            return
        return response.xpath(
            f"//span[contains(text(),'{field_name}')]/ancestor-or-self::td/following-sibling::td//text()"
        ).get()

    def extract_sub_field(self, response, field_name):
        return response.xpath(
            f"//span[contains(text(),'{field_name}')]/following-sibling::span//text()"
        ).get()

    def parse_disclaimer_page(self, response):
        self.logger.info(f"In disclaimer page {response.url}")

        # Obtaining the disclaimer key from the site
        disclaimer_key = response.selector.xpath("//input[contains(@name,'disclaimer')]/@value").get(default=None)
        if disclaimer_key is None:
            self.logger.critical("Cannot get the disclaimer key to post the form")
            return

        # Adding obtained key to the form
        form_data = {
            "disclaimer": disclaimer_key,
        }

        yield scrapy.FormRequest(
            url=SEARCH_PAGE_URL,
            method="POST",
            dont_filter=True,
            headers=self.headers,
            formdata=form_data,
            callback=self.parse_search_page,
        )

    def parse_search_page(self, response):
        self.logger.info(f"In search page {response.url}")
        self.logger.info(f"Searching: First {self.first}% | Last {self.last}%")

        # Obtaining search key from the page
        search_key = response.selector.xpath("//input[contains(@name,'searchtype')]/@value").get(default=None)
        if search_key is None:
            self.logger.critical("Cannot get Search Key to post the form")
            return

        form_data = {
            "lastName": f"{self.last}%",
            "firstName": f"{self.first}%",
            "middleName": "",
            # * select party type as defendant with "DEF"
            "partyType": "DEF",
            # * select traffic violations with "TRAFFIC"
            "site": "TRAFFIC",
            # * select district court with "D"
            "courtSystem": "B",
            "countyName": "",
            "filingStart": "6/29/2022",
            "filingEnd": "6/30/2022",
            "filingDate": "",
            "company": "N",
            "courttype": "N",
            "searchtype": search_key,
            "searchTrialPersonAction": "Search",
        }

        yield scrapy.FormRequest(
            url=SEARCH_POST_URL,
            method="POST",
            dont_filter=True,
            headers=self.headers,
            formdata=form_data,
            callback=self.parse_results_page,
        )

    def parse_results_page(self, response):
        self.logger.info(f"In results page {response.url}")
        item_count_msg = response.xpath("//span[contains(text(),'items')]/text()").get()
        self.logger.info(item_count_msg)

        # Prepare a list of link cases
        case_links = response.css("tfoot+tbody td a::attr(href)").getall()
        case_links = [urljoin(response.url, link) for link in case_links]

        # Get the next_page link if it exists
        next_link = response.xpath("//a[contains(text(),'Next')]/@href").get()
        if next_link:
            next_link = urljoin(response.url, next_link)

        yield self.process_links(case_links, next_link)

    def process_links(self, case_links: list, next_link: str = None):
        # If the list has been exhausted or is empty we try to move to the next page
        if not case_links and next_link:
            return scrapy.Request(
                url=next_link,
                callback=self.parse_results_page,
                headers=self.headers,
                dont_filter=True,
            )
        # Otherwise if the list is empty, and we don't have next page link, we move to the next letter
        elif not case_links:
            return self.next_request()

        # Get the first link of the list
        link = case_links.pop(0)
        # Process this link and send the remaining links and next page to the callback.
        return scrapy.Request(
            url=link,
            callback=self.parse_traffic_page,
            headers=self.headers,
            cb_kwargs={
                'case_links': case_links,
                'next_link': next_link,
            },
        )

    def extract_case_page(self, response):
        self.logger.info(f"Found regular page {response.url}")
        item = {}
        # gather information from case information section
        item["citation_number"] = self.extract_element(response, "Citation Number")
        item["filling_date"] = self.extract_element(response, "Filing Date")
        item["violation_county"] = self.extract_element(response, "Violation County")
        item["case_status"] = self.extract_element(response, "Case Status")

        # get name
        item["name"] = self.extract_element(response, ["Defendant Name", "Name"])
        item["address"] = self.extract_element(response, "Address")
        item["city"] = self.extract_element(response, "City")
        item["state"] = self.extract_element(response, "State")
        item["zip_code"] = self.extract_element(response, "Zip Code")

        # get charge and disposition info
        item["charge_description"] = self.extract_element(
            response, ["Charge Description", "Description"]
        )
        item["fine_amount_owed"] = self.extract_element(
            response, ["Fine Amount Owed", "Fine"]
        )
        item["link"] = response.url
        
        return item

    def parse_traffic_page(self, response, case_links: list = None, next_link: str = None):
        self.logger.info(f"In case page {response.url}")
        try:
            item = self.extract_case_page(response)
            yield item
        except Exception as error:
            self.logger.error(f"Error occured in {response.url}: {error}")
        yield self.process_links(case_links, next_link)
