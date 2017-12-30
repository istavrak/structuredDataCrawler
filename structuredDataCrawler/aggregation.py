from pymongo import Connection
import pymongo
import ConfigParser
import logging
from Queue import *
import MySQLdb as mdb


def aggregate_seed_items(seed_url):
    # Loading the MongoDB credentials from the settings file
    Config = ConfigParser.ConfigParser()
    Config.read("..//settings.cfg")
    db_hostname = Config.get("MongoDB", "db_hostname")
    db_port = Config.get("MongoDB", "db_port")
    db_database = Config.get("MongoDB", "db_database")
    db_collection_items = Config.get("MongoDB", "db_collection_items")
    db_collection_aggr_items = Config.get("MongoDB", "db_collection_aggr_items")

    conn = Connection(db_hostname, int(db_port))
    db = conn[db_database]
    coll_items = db[db_collection_items]
    coll_aggr_items = db[db_collection_aggr_items]

    items = coll_items.find({'$or': [{'seed_url': seed_url}, {'main_url': seed_url}]})

    records_to_process = items.count(with_limit_and_skip=True)
    logging.basicConfig(filename='../logging_aggregation.log', level=logging.INFO)
    logging.info("The aggregator has retrieved " + str(records_to_process) + " items for seed: " + seed_url)

    if records_to_process != 0:
        exist_aggr_items = coll_aggr_items.find({'$or': [{'seed_url': seed_url}, {'main_url': seed_url}]})
        records_aggregated = exist_aggr_items.count(with_limit_and_skip=True)

        if records_aggregated == 0:
            # We do the aggregation in any way, because even if the object exists in the aggregations collection
            # we have to compare with it in order not to loose any piece of information.
            # Initialize with the first item
            new_aggr_item = items[0]

            for item in items:
                # add the scraped item to the respective seed
                for ilink in item['RDFa_namespaces']:
                    if not ilink in new_aggr_item['RDFa_namespaces']:
                        new_aggr_item['RDFa_namespaces'].append(ilink)

                for ilink in item['microformats']:
                    if not ilink in new_aggr_item['microformats']:
                        new_aggr_item['microformats'].append(ilink)

                for ilink in item['microformats2']:
                    if not ilink in new_aggr_item['microformats2']:
                        new_aggr_item['microformats2'].append(ilink)

                for ilink in item['rss']:
                    if not ilink in new_aggr_item['rss']:
                        new_aggr_item['rss'].append(ilink)

                for ilink in item['microdata_types']:
                    if not ilink in new_aggr_item['microdata_types']:
                        new_aggr_item['microdata_types'].append(ilink)

                for ilink in item['iframes']:
                    if not ilink in new_aggr_item['iframes']:
                        new_aggr_item['iframes'].append(ilink)

                for ilink in item['CMS_type']:
                    if not ilink in new_aggr_item['CMS_type']:
                        new_aggr_item['CMS_type'].append(ilink)

                for ilink in item['server_type']:
                    if not ilink in new_aggr_item['server_type']:
                        new_aggr_item['server_type'].append(ilink)

                for ilink in item['schema_types']:
                    if not ilink in new_aggr_item['schema_types']:
                        new_aggr_item['schema_types'].append(ilink)

                for ilink in item['twitter']:
                    if not ilink in new_aggr_item['twitter']:
                        new_aggr_item['twitter'].append(ilink)

                for ilink in item['facebook']:
                    if not ilink in new_aggr_item['facebook']:
                        new_aggr_item['facebook'].append(ilink)

                for ilink in item['gplus']:
                    if not ilink in new_aggr_item['gplus']:
                        new_aggr_item['gplus'].append(ilink)

                for ilink in item['tripadvisor']:
                    if not ilink in new_aggr_item['tripadvisor']:
                        new_aggr_item['tripadvisor'].append(ilink)

                for ilink in item['review_widgets_links']:
                    if not ilink in new_aggr_item['review_widgets_links']:
                        new_aggr_item['review_widgets_links'].append(ilink)

                for ilink in item['other_external_links']:
                    if not ilink in new_aggr_item['other_external_links']:
                        new_aggr_item['other_external_links'].append(ilink)

                for ilink in item['atom']:
                    if not ilink in new_aggr_item['atom']:
                        new_aggr_item['atom'].append(ilink)

                for ilink in item['sitemap']:
                    if not ilink in new_aggr_item['sitemap']:
                        new_aggr_item['sitemap'].append(ilink)

                for ilink in item['metatags']:
                    if not ilink in new_aggr_item['metatags']:
                        new_aggr_item['metatags'].append(ilink)

                for ilink in item['headRDFLinks']:
                    if not ilink in new_aggr_item['headRDFLinks']:
                        new_aggr_item['headRDFLinks'].append(ilink)

                for ilink in item['RDFa_typeOf']:
                    if not ilink in new_aggr_item['RDFa_typeOf']:
                        new_aggr_item['RDFa_typeOf'].append(ilink)

            # The aggregated item has been created. Save it in the new collection
            coll_aggr_items.insert(new_aggr_item)
            # log.msg("The aggregated item is inserted for seed: "+seed_url,log.INFO)
            logging.info("The aggregated item is inserted for seed: " + seed_url)
        else:
            # log.msg("No item is inserted for seed: "+seed_url+" as it already exists",log.INFO)
            logging.info("No item is inserted for seed: " + seed_url + " as it already exists")
            pass
            # in this case there are results in the aggregations collection
            # TODO but not urgent


def main():
    logging.basicConfig(filename='../logging_aggregation_manual.log', level=logging.INFO)

    # Loading the MySQL credentials

    Config = ConfigParser.ConfigParser()
    Config.read("..//settings.cfg")
    db_user = Config.get("MYSQL", "db_user")
    db_pass = Config.get("MYSQL", "db_pass")
    db_location = Config.get("MYSQL", "db_location")
    db_name = Config.get("MYSQL", "db_name")

    logging.info('Feeding the Queue with seeds for aggregation...')

    scraped_seeds_queue = Queue()
    con = mdb.connect(db_location, db_user, db_pass, db_name)

    # Feed a Queue with the crawled seeds from the MySQL DB
    with con:
        cur = con.cursor()

        cur.execute('SELECT seed_url FROM seeds WHERE (last_check IS NOT NULL OR status IS NOT NULL)')

        numrows = int(cur.rowcount)
        if numrows > 0:
            rows = cur.fetchall()

            counter = 0
            for i in rows:
                scraped_seeds_queue.put(i[0].lower().replace(" ", ""))
                counter += 1
                logging.info("Queueing seed_" + str(counter) + ": " + i[0].lower().replace(" ", ""))

    if con:
        con.close()

    # Start the aggregation
    while (True):
        try:
            seed_url = scraped_seeds_queue.get(
                block=False)  # we don't want to wait in the queue until there is an available item
            if not seed_url.startswith('http://') and not seed_url.startswith('https://'):
                seed_url = 'http://%s' % seed_url
            logging.info("Ready to aggregate the scraped items for seed: " + seed_url)
            aggregate_seed_items(seed_url)
        except Empty:
            logging.info(
                "Queue with crawled seeds is empty. The seed items have been aggregated. Ready for the analysis!")
            break


if __name__ == "__main__":
    main()
