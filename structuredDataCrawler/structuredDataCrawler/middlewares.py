import MySQLdb as mdb
import ConfigParser
from scrapy import log


class CustomDownloaderMiddleware(object):
    def process_response(self, request, response, spider):
        Config = ConfigParser.ConfigParser()
        Config.read("..//settings.cfg")
        db_user = Config.get("MYSQL", "db_user")
        db_pass = Config.get("MYSQL", "db_pass")
        db_location = Config.get("MYSQL", "db_location")
        db_name = Config.get("MYSQL", "db_name")

        # Check if the status is 404 to update the database
        if response.status == 404:
            con = mdb.connect(db_location, db_user, db_pass, db_name)

            # Update the seeds in the db
            with con:
                cur = con.cursor()

                operation = "UPDATE seed SET last_check=NOW(),pages_scraped=0,status=404 WHERE seed_url LIKE %s" % (
                "'%" + response.url.replace("www.", "") + "%'")
                result = cur.execute(operation)

                log.msg(result, level=log.INFO)
                if cur.rowcount > 0:
                    log.msg("Updated seed entry in MySQL for " + response.url.replace("www.", "") + " with status 404. The URL does not exist",
                            level=log.INFO)
            if con:
                con.close()

        return response
