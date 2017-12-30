'''
Created on Jul 09, 2013

@author: istavrak
'''
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector, XmlXPathSelector
from scrapy.http import Request
from structuredDataCrawler.items import SeedItem
import MySQLdb as mdb
import ConfigParser
from urlparse import urlunparse, urlparse
from scrapy import log
from spider_utilities import domain_extractor, tld_extractor


class MyScaledSpider(CrawlSpider):
    # the name of the crawler
    name = "structured_data_crawler"

    # For scalability reasons we prevent the spider to crawl the multiple language versions of a website
    # In a typical hotel website we have about 80% improvement (100 results instead of 500) 
    # The rule could be without the last slash (i.e. /en, /it, etc.) but we would risk to miss links that start with
    # the locale as subdomain.
    # Media types are excluded by default.
    allowed_domains = []
    start_urls = []

    rules = (
        Rule(SgmlLinkExtractor(deny=("/en/", "/it/", "/fr/", "/es/")), callback="parse_items", follow=True),
    )

    def __init__(self, **kw):

        url = kw.get('url')
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://%s' % url
        self.url = url
        self.start_urls = []
        self.start_urls.append(url)
        self.allowed_domains = []
        self.allowed_domains.append(domain_extractor(url))
        super(MyScaledSpider, self).__init__(**kw)
        self._compile_rules()
        for u_temp in self.start_urls:
            log.msg(
                "Initialising the spider with start_urls: " + u_temp + ", allowed_domains: " + self.allowed_domains[0],
                log.INFO)

    def parse_items(self, response):
        current_url = response.url.lower()
        current_url_domain = domain_extractor(current_url)

        item = SeedItem()

        if tld_extractor(current_url) == "xml":
            xxs = XmlXPathSelector(response)

            # ---------------------------------
            # Sitemaps
            # ---------------------------------
            xxs.remove_namespaces()
            temp_sitemap_content = ""
            temp_sitemap_content = xxs.select("//urlset").extract()

            if temp_sitemap_content != "":
                item['sitemap'] = current_url
            else:
                item['sitemap'] = ""
        else:
            item['sitemap'] = ""
            hxs = HtmlXPathSelector(response)

            # We assume there is only one start_url
            item['main_url'] = self.start_urls[0]

            item['seed_url'] = current_url_domain
            item['seed_response_url'] = current_url

            # ---------------------------------
            # Links in the Head section
            # ---------------------------------

            item['headRDFLinks'] = hxs.select(
                "//link[contains(translate(@rel,'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'schema')]").extract()

            # ---------------------------------
            # Meta-tags
            # ---------------------------------

            item['metatags'] = hxs.select("//meta/@name").extract()

            # ---------------------------------
            # RDFa
            # ---------------------------------

            # item['RDFa_namespaces']=hxs.select("//html").select("@*[not(contains(name(.),'lang'))]").extract()

            # extended support of RDFa namespaces extraction
            item['RDFa_namespaces'] = hxs.select(
                "//*/@*[contains(name(.),'xmlns') or contains(name(.),'prefix') or contains(name(.),'vocab') and not(contains(name(.),'lang'))]").extract()

            # collect the types of entities that are mentioned in the website
            item['RDFa_typeOf'] = hxs.select("//*/@*[contains(name(.),'typeof')]").extract()

            # ---------------------------------
            # Microformats v1 & v2
            # ---------------------------------

            microformats_classes = ['hreview', 'rating', 'hproduct', 'vcard', 'vevent', 'geo']
            microformats2_classes = ['h-adr', 'h-card', 'h-entry', 'h-event', 'h-geo', 'h-item', 'h-product',
                                     'h-recipe', 'h-resume', 'h-review']
            temp_classes = hxs.select("//*[contains(@class,*)]").select("@class").extract()

            item['microformats'] = []
            item['microformats2'] = []

            for temp_class in temp_classes:
                if temp_class != "":
                    inner_classes = temp_class.split(" ")
                    for i_micro in microformats_classes:
                        for tmp_inner in inner_classes:
                            if i_micro == tmp_inner:
                                item['microformats'].append(tmp_inner)
                                break

                    else:
                        for i_micro in microformats2_classes:
                            for tmp_inner in inner_classes:
                                if i_micro == tmp_inner:
                                    item['microformats2'].append(tmp_inner)
                                    break

            # ---------------------------------
            # Microdata
            # ---------------------------------

            # Extracts the type of entities that are used in the website
            item['microdata_types'] = hxs.select("//*[contains(@itemtype,*)]").select("@itemtype").extract()

            # ---------------------------------
            # External links
            # ---------------------------------

            # Extracts all the external links. In the pipeline phase we will put them in clusters.
            # We check the netloc. If doesn't exist then it's an internal link.
            # If it exists but contains the seed domain then it's an internal link too.
            temp_links = hxs.select("//*[contains(@href,*)]").select("@href").extract()

            item['all_external_links'] = []

            for temp_link in temp_links:
                # netloc_link=urlparse(temp_link)[1]
                netloc_link = domain_extractor(temp_link.lower())
                if netloc_link != "":
                    # print netloc_link
                    tld_link = tld_extractor(temp_link.lower())
                    if tld_link != "":
                        if not (item['seed_url'] in temp_link.lower()):
                            item['all_external_links'].append(temp_link.lower())

            # ---------------------------------
            # RSS & Atom feeds 
            # ---------------------------------
            # <link type="application/rss+xml" 
            # <link type="application/atom+xml"

            item['rss'] = hxs.select("//link[contains(@type,'application/rss+xml')]").select("@href").extract()
            item['atom'] = hxs.select("//link[contains(@type,'application/atom+xml')]").select("@href").extract()

            # ---------------------------------
            # CMS type
            # ---------------------------------

            # Extracts the type of the CMS that is used by searching for the meta element Generator (could appear in various typecases).
            item['CMS_type'] = hxs.select(
                "//meta[contains(translate(@name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'generator')]").select(
                "@content").extract()

            # ---------------------------------
            # Server type
            # ---------------------------------
            item['server_type'] = []
            if 'server' in response.headers:
                item['server_type'].append(response.headers['server'])

            # ---------------------------------
            # iframes
            # ---------------------------------
            item['iframes'] = hxs.select("//iframe/@src").extract()

        return item

    # we override the parse_start_url in order to parse the homepage too. Otherwise, the crawler ignores it.
    def parse_start_url(self, response):
        log.msg("Starting scraping for " + response.url.lower(), log.INFO)
        return self.parse_items(response)
