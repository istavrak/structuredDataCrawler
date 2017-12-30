# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from structuredDataCrawler.items import PipeSeedItem
from structuredDataCrawler.items import SeedItem
from collections import defaultdict
from pymongo import Connection
import pymongo
import MySQLdb as mdb
import json
import ConfigParser
from scrapy import log
from spider_utilities import domain_extractor, tld_extractor


class CrawlSeedPipeline(object):
    def process_item(self, item, spider):
        # we make the assumption that we have only one url in the list of start_urls due to the usage of our spiders
        # for iurl in spider.start_urls:
        #    log.msg("In CrawlSeedPipeline pipeline for: "+iurl,log.INFO)

        new_item = PipeSeedItem()

        new_item['main_url'] = item['main_url']
        new_item['seed_url'] = item['seed_url']
        new_item['seed_response_url'] = item['seed_response_url']
        new_item['RDFa_namespaces'] = sorted(set(item['RDFa_namespaces']))
        new_item['microformats'] = sorted(set(item['microformats']))
        new_item['microformats2'] = sorted(set(item['microformats2']))
        new_item['rss'] = sorted(set(item['rss']))
        new_item['atom'] = sorted(set(item['atom']))
        new_item['sitemap'] = item['sitemap']
        new_item['headRDFLinks'] = sorted(set(item['headRDFLinks']))
        new_item['metatags'] = sorted(set(item['metatags']))
        new_item['RDFa_typeOf'] = sorted(set(item['RDFa_typeOf']))

        # We remove the duplicates
        external_links = sorted(set(item['all_external_links']))
        new_item['microdata_types'] = sorted(set(item['microdata_types']))
        new_item['iframes'] = sorted(set(item['iframes']))

        new_item['CMS_type'] = item['CMS_type']
        new_item['server_type'] = item['server_type']

        # Populate the new item
        new_item['schema_types'] = []
        new_item['twitter'] = []
        new_item['facebook'] = []
        new_item['gplus'] = []
        new_item['tripadvisor'] = []
        new_item['review_widgets_links'] = []
        new_item['other_external_links'] = []

        for itype in new_item['microdata_types']:
            if "schema.org" in itype:
                new_item['schema_types'].append(itype)

        # Cluster the extracted links in categories
        for ilink in external_links:
            if "plus.google.com" in ilink:
                new_item['gplus'].append(ilink)

            elif "facebook.com" in ilink:
                new_item['facebook'].append(ilink)

            elif "twitter.com" in ilink:
                new_item['twitter'].append(ilink)

            elif ("tripadvisor" in ilink) or ("trustyou" in ilink) or ("hotelnavigators" in ilink) or (
                        "customer-alliance" in ilink):
                new_item['review_widgets_links'].append(ilink)
                if ("tripadvisor" in ilink):
                    new_item['tripadvisor'].append(ilink)
            else:
                new_item['other_external_links'].append(ilink)

        return new_item


class AggregationPipeline(object):
    # This pipeline aggregates the scraped data per seed.
    def __init__(self):
        # we save the scraped items per seed in a list
        self.dictSeed_items = defaultdict(list)
        self.file_aggr = open('items_aggr.json', 'a')

    def process_item(self, item, spider):

        h_url = item['main_url']

        # add the scraped item to the respective seed
        if not self.dictSeed_items[h_url]:
            self.dictSeed_items[h_url] = item
        else:
            aggr_item = self.dictSeed_items[h_url]

            for ilink in item['RDFa_namespaces']:
                if not ilink in aggr_item['RDFa_namespaces']:
                    aggr_item['RDFa_namespaces'].append(ilink)

            for ilink in item['microformats']:
                if not ilink in aggr_item['microformats']:
                    aggr_item['microformats'].append(ilink)

            for ilink in item['microformats2']:
                if not ilink in aggr_item['microformats2']:
                    aggr_item['microformats2'].append(ilink)

            for ilink in item['rss']:
                if not ilink in aggr_item['rss']:
                    aggr_item['rss'].append(ilink)

            for ilink in item['microdata_types']:
                if not ilink in aggr_item['microdata_types']:
                    aggr_item['microdata_types'].append(ilink)

            for ilink in item['iframes']:
                if not ilink in aggr_item['iframes']:
                    aggr_item['iframes'].append(ilink)

            for ilink in item['CMS_type']:
                if not ilink in aggr_item['CMS_type']:
                    aggr_item['CMS_type'].append(ilink)

            for ilink in item['server_type']:
                if not ilink in aggr_item['server_type']:
                    aggr_item['server_type'].append(ilink)

            for ilink in item['schema_types']:
                if not ilink in aggr_item['schema_types']:
                    aggr_item['schema_types'].append(ilink)

            for ilink in item['twitter']:
                if not ilink in aggr_item['twitter']:
                    aggr_item['twitter'].append(ilink)

            for ilink in item['facebook']:
                if not ilink in aggr_item['facebook']:
                    aggr_item['facebook'].append(ilink)

            for ilink in item['gplus']:
                if not ilink in aggr_item['gplus']:
                    aggr_item['gplus'].append(ilink)

            for ilink in item['tripadvisor']:
                if not ilink in aggr_item['tripadvisor']:
                    aggr_item['tripadvisor'].append(ilink)

            for ilink in item['review_widgets_links']:
                if not ilink in aggr_item['review_widgets_links']:
                    aggr_item['review_widgets_links'].append(ilink)

            for ilink in item['other_external_links']:
                if not ilink in aggr_item['other_external_links']:
                    aggr_item['other_external_links'].append(ilink)

            self.dictSeed_items[h_url] = aggr_item

        return item

    def close_spider(self, spider):

        for k, v in self.dictSeed_items.iteritems():
            line = json.dumps(dict(v)) + "\n"
            self.file_aggr.write(line)

        self.file_aggr.close()


class MongoDBItemsPipeline(object):
    # This pipeline inserts the items in MongoDB.
    def __init__(self):

        # We save the number of scraped items per seed
        self.dictSeed = defaultdict(int)

    def process_item(self, item, spider):

        # log.msg("In MongoDBItemsPipeline pipeline for: "+spider.start_urls[0]+", "+item['main_url'],log.INFO)
        h_url = item['main_url']
        h_response = item['seed_response_url']

        h_response_domain = domain_extractor(h_response)
        if (h_response_domain in h_url):
            # increase the number of scraped items for the respective seed
            if not self.dictSeed[h_url]:
                self.dictSeed[h_url] = 1
            else:
                self.dictSeed[h_url] += 1

            line = json.dumps(dict(item))
            # Save the scrapped items in MongoDB
            # Loading the MongoDB credentials from the settings file
            Config = ConfigParser.ConfigParser()
            Config.read("..//settings.cfg")
            db_hostname = Config.get("MongoDB", "db_hostname")
            db_port = Config.get("MongoDB", "db_port")
            db_database = Config.get("MongoDB", "db_database")
            db_collection = Config.get("MongoDB", "db_collection_items")

            conn = Connection(db_hostname, int(db_port))
            db = conn[db_database]
            coll = db[db_collection]
            coll.insert(json.loads(line))

            return item
        else:
            raise DropItem("The spider brought irrelevant item: " + h_response_domain + " for seed:" + h_url)


class JsonWriterPipeline(object):
    def __init__(self):
        self.file = open('items.json', 'a')

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item
