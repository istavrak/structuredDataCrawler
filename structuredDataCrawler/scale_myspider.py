from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from structuredDataCrawler.items import SeedItem
import MySQLdb as mdb
import ConfigParser
from Queue import *
from scrapy import log, signals
from scrapy.resolver import CachingThreadedResolver
from scrapy.conf import settings
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from structuredDataCrawler.spiders.structured_data_spider import MyScaledSpider
from scrapy.xlib.pydispatch import dispatcher
from twisted.internet import reactor
from aggregation import aggregate_seed_items

# Loading the db credentials from the settings file
Config = ConfigParser.ConfigParser()
Config.read("..//settings.cfg")
db_user = Config.get("MYSQL", "db_user")
db_pass = Config.get("MYSQL", "db_pass")
db_location = Config.get("MYSQL", "db_location")
db_name = Config.get("MYSQL", "db_name")

# Loading the scrapy parameters from the settings file
limit_seeds = Config.get("scrapy", "limit_seeds")
scraping_frequency = Config.get("scrapy", "scraping_frequency")

# Create the queue with the seeds
seeds_queue = Queue()
scraped_seeds_queue = Queue()

con = mdb.connect(db_location, db_user, db_pass, db_name)
log.start("../logging_crawler.log", loglevel=log.INFO)
# Feed the crawler with the seeds from the mysql db
with con:
    cur = con.cursor()

    if limit_seeds == 0:
        cur.execute(
            'SELECT seed_url FROM seeds WHERE status IS NULL and (DATE(last_check) <= CURRENT_DATE-%s or last_check IS NULL)',
            (int(scraping_frequency)))
    else:
        cur.execute(
            'SELECT seed_url FROM seeds WHERE (DATE(last_check) <= CURRENT_DATE-%s or last_check IS NULL) LIMIT %s',
            (int(scraping_frequency), int(limit_seeds)))

    numrows = int(cur.rowcount)
    if numrows > 0:
        rows = cur.fetchall()

        counter = 0
        for i in rows:
            seeds_queue.put(i[0].lower().replace(" ", ""))
            counter += 1
            log.msg("Queueing seed_" + str(counter) + ": " + i[0].lower().replace(" ", ""), level=log.INFO)

if con:
    con.close()


def update_aggregation_coll():
    while (True):
        try:
            # we don't want to wait in the queue until there is an available item
            seed_url = scraped_seeds_queue.get(block=False)
            if not seed_url.startswith('http://') and not seed_url.startswith('https://'):
                seed_url = 'http://%s' % seed_url
            log.msg("Ready to aggregate the scraped items for seed: " + seed_url, level=log.INFO)
            aggregate_seed_items(seed_url)
        except Empty:
            log.msg(
                "Queue with crawled seeds is empty. The seed items have been aggregated. Ready for the analysis!",
                level=log.INFO)
            break


def start_crawler():
    crawler = Crawler(settings)
    crawler.configure()

    try:
        # we don't want to wait in the queue until there is an available item
        url = seeds_queue.get(block=False)
        log.msg("Crawler for " + url + " is ready to start", level=log.INFO)

        crawler.crawl(MyScaledSpider(url=url))

        # add the url to the queue that will be used in the aggregation
        scraped_seeds_queue.put(url)

        crawler.start()
    except Empty:
        log.msg("Queue with seeds is empty. That's all folks!", level=log.INFO)
        # it's out of the main thread:
        reactor.callFromThread(reactor.stop)
        update_aggregation_coll()
    else:
        # in case of some other problem you stop the job with task_done.
        seeds_queue.task_done()

        # Get a signal that a crawler has finished its job, start another crawler


def spider_closed(spider, reason):
    # if there is no such value is None
    response_200 = spider.crawler.stats.get_value('downloader/response_status_count/200')
    response_404 = spider.crawler.stats.get_value('downloader/response_status_count/404')

    # ----------------------------------------------
    # Update the status of the seed in the MySQL db
    # ----------------------------------------------

    # Loading the db credentials from the settings file
    Config = ConfigParser.ConfigParser()
    Config.read("..//settings.cfg")
    db_user = Config.get("MYSQL", "db_user")
    db_pass = Config.get("MYSQL", "db_pass")
    db_location = Config.get("MYSQL", "db_location")
    db_name = Config.get("MYSQL", "db_name")

    con = mdb.connect(db_location, db_user, db_pass, db_name)

    with con:
        cur = con.cursor()

        # At the starting urls we add the scheme. So, now we don't know if it exists or not. We remove and check without it.
        strip_url = spider.start_urls[0].replace("http://", "").replace("https://", "")
        if response_200 != None:
            operation = "UPDATE seeds SET last_check=NOW(),pages_scraped=%d WHERE seed_url LIKE %s" % (
                response_200, "'%" + strip_url + "%'")
            result = cur.execute(operation)

            if result > 0:
                log.msg(
                    "Updated seed entry in MySQL for " + strip_url + " with " + str(response_200) + " scraped pages",
                    level=log.INFO)
            else:
                # this failure could happen in case we try to save information about a subdomain of the seed
                log.msg("Update of seed entry in MySQL for " + strip_url + " FAILED", level=log.INFO)

            con.commit()
        elif response_404 != None and response_200 == 0:
            operation = "UPDATE seeds SET last_check=NOW(),pages_scraped=0,status=404 WHERE seed_url LIKE %s" % (
                "'%" + strip_url + "%'")
            result = cur.execute(operation)

            if result > 0:
                log.msg("Updated seed entry in MySQL for " + strip_url + " with status 404", level=log.INFO)
            else:
                # this failure could happen in case we try to save information about a subdomain of the seed
                log.msg("Update of seed entry in MySQL for " + strip_url + " FAILED", level=log.INFO)

            con.commit()

    if con:
        con.close()

    # Start a new crawler
    reactor.callLater(1, start_crawler)  # after 1 second


def main():
    # Subscribe to the "spider_closed" signals
    dispatcher.connect(spider_closed, signals.spider_closed)

    # Start a required number of concurrent crawlers

    number_of_concurrent_crawlers = 1
    for i in range(number_of_concurrent_crawlers):
        reactor.callLater(1, start_crawler)  # after 1 second

    reactor.installResolver(CachingThreadedResolver(reactor))

    # Run twisted reactor. This call is blocking.
    log.msg('Running reactor...')

    reactor.run(installSignalHandlers=False)  # here the script blocks. When queue is empty we stop it.


if __name__ == "__main__":
    main()
