# Structured Data Crawler
Crawler based on Scrapy.org that aims to extract various types of data and semantic annotations in order to construct a website profile.

The crawler consumes a predefined list of seed URLs and looks for specific data within the website related to social network links, semantic annotations, media types and external links in general.

It was used for the research conducted in the publication listed below. If you use it in your initiatives, please cite the following publication:
```
@Inbook{Stavrakantonakis2013,
author="Stavrakantonakis, Ioannis and Toma, Ioan and Fensel, Anna and Fensel, Dieter",
editor="Xiang, Zheng and Tussyadiah, Iis",
title="Hotel Websites, Web 2.0, Web 3.0 and Online Direct Marketing: The Case of Austria",
bookTitle="Information and Communication Technologies in Tourism 2014: Proceedings of the International Conference in Dublin, Ireland, January 21-24, 2014",
year="2013",
publisher="Springer International Publishing",
pages="665--677",
isbn="978-3-319-03973-2",
doi="10.1007/978-3-319-03973-2_48",
url="https://doi.org/10.1007/978-3-319-03973-2_48"
}
```


Implementation details

MySQL

The seeds table can be created using the following CREATE statement:
```
CREATE TABLE `seed` (
  `seed_url` varchar(400) NOT NULL,
  `last_check` datetime DEFAULT NULL,
  `pages_scraped` int(11) DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  PRIMARY KEY (`seed_url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

|---------------|--------------|------|-----|---------|-------|
| Field         | Type         | Null | Key | Default | Extra |
| seed_url      | varchar(400) | NO   | PRI |         |       |
| last_check    | datetime     | YES  |     | NULL    |       |
| pages_scraped | int(11)      | YES  |     | NULL    |       |
| status        | int(11)      | YES  |     | NULL    |       |