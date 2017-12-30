'''
Created on Jul 10, 2013

@author: istavrak
'''

from scrapy.item import Item, Field

class SeedItem(Item):
    # !IMPORTANT!
    # we assume that any of those could be an array

    main_url = Field()                  # the url of the seed
    seed_url = Field()                  # the domain of the seed
    seed_response_url = Field()         # we keep the response url of the item

    all_external_links = Field()        # the links that are pointing outside of the domain of the seed
    microdata_types = Field()           # all the itemtype properties that were identified in the website excluding the schema.org types
    microformats = Field()              # all the microformats types that were identified in the website
    microformats2 = Field()             # all the microformats2 types that were identified in the website
    CMS_type = Field()                  # the CMS that is used at the backend based on the meta generator property
    RDFa_namespaces = Field()           # all the namespaces that appear with: xmlns, prefix, vocab
    server_type = Field()               # the server that is used to host the website
    iframes = Field()                   # the iframes that were found in the website
    rss = Field()                       # RSS that was found in the website
    atom = Field()                      # Atom that was found in the website
    sitemap = Field()                   # If a link is an xml
    metatags = Field()                  # Store all the meta-tags that occur in the website
    headRDFLinks = Field()              # Store all the links that are in the head section of the website
    RDFa_typeOf = Field()               # The types of entities that are annotated with RDFa, RDFa Lite

class PipeSeedItem(SeedItem):
    # this class is used at the pipeline phase 
    schema_types = Field()              # schema.org types that are used in the website
    gplus = Field()
    twitter = Field()
    facebook = Field()
    tripadvisor = Field()
    social_web_links = Field()
    review_widgets_links = Field()      # widgets' links that have been recognized
    booking_links = Field()             # booking links that have been recognized
    other_external_links = Field()      # the links that are pointing outside of the domain of the seed