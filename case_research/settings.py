BOT_NAME = "case_research"

SPIDER_MODULES = ["case_research.spiders"]
NEWSPIDER_MODULE = "case_research.spiders"

# my sql data
MYSQL_DB = "case_research_traffic"
MYSQL_USERNAME = "admin"
MYSQL_PASSWORD = "admin"
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'case_research (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

ITEM_PIPELINES = {
    "case_research.pipelines.CaseResearchPipeline": 300,
    # "case_research.pipelines.MySQLPipeline": 400,
}

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Adding Aut-throttle to avoid getting 403's
AUTOTHROTTLE_ENABLE = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
DOWNLOAD_DELAY = 1
