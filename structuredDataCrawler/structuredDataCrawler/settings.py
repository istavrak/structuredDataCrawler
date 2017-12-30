# Scrapy settings for structuredDataCrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#
from scrapy.settings.default_settings import REDIRECT_ENABLED

BOT_NAME = 'structuredDataCrawler'

SPIDER_MODULES = ['structuredDataCrawler.spiders']
NEWSPIDER_MODULE = 'structuredDataCrawler.spiders'

ITEM_PIPELINES = [
    'structuredDataCrawler.pipelines.CrawlSeedPipeline',
    'structuredDataCrawler.pipelines.MongoDBItemsPipeline',
    # Aggregation is not possible within the spiders. It has been moved to the business logic in MongoDB
    # 'structuredDataCrawler.pipelines.AggregationPipeline',
    # To be removed in the production
    # 'structuredDataCrawler.pipelines.JsonWriterPipeline',
]
DOWNLOADER_MIDDLEWARES = {
    'structuredDataCrawler.middlewares.CustomDownloaderMiddleware': 355,
}
COOKIES_ENABLED = False
DEPTH_LIMIT = 3
LOG_LEVEL = 'INFO'
LOG_ENABLED = True
LOG_FILE = '../logging_structured_data_crawler.log'

REDIRECT_ENABLED = False
# DEFAULT_REQUEST_HEADERS = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#     'Accept-Language': 'de', 
# }

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'structuredDataCrawler - research'
