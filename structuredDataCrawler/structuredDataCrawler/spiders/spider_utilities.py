import tldextract


def domain_extractor(url):
    no_fetch_extract = tldextract.TLDExtract(fetch=False)
    result = no_fetch_extract(url)
    return result.domain + "." + result.tld


def tld_extractor(url):
    no_fetch_extract = tldextract.TLDExtract(fetch=False)
    result = no_fetch_extract(url)
    return result.tld
