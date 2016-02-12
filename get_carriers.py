from requests import get
from bs4 import BeautifulSoup
from re import compile
from codecs import open
from pymongo import MongoClient
from datetime import datetime

fetch_carrier_urls = False
fetch_carrier_json = True
fetch_additional_urls = True

additional_urls = []

db = MongoClient('mongodb://<user>:<pass>@ds<id>.mongolab.com:<port>/<db>')
collection = db["muziekcentrum"]["muziekcentrum"]

if fetch_carrier_urls:
  carrier_urls = set()
  for i in range(1, 4, 1):
    url = "http://www.muziekcentrum.be/genrelist.php?Page=" + str(i) + "&List=4&Year=2015&Genre=195"
    print url
    r = get(url)
    html = BeautifulSoup(r.text, "lxml")
    carrier_regex = compile('<a href=["\']carrier\.php\?ID=(\d+)["\']>(.+?)</a>')
    tds = html.findAll("td")
    for td in tds:
       carrier = carrier_regex.findall(unicode(td))[0]
       if len(carrier) == 2:
         carrier_urls.add("http://www.muziekcentrum.be/carrier.php?ID=" + carrier[0] + "\t" + carrier[1] + "\n")

if fetch_carrier_urls:
  with open("carrier_urls.txt", "w", "utf-8") as f:
    f.writelines(carrier_urls)

if fetch_carrier_json:
  with open("carrier_urls.txt", "r", "utf-8") as f:
    for carrier in f.readlines():
      url, carrier_title = carrier.split("\t")
      doc = {"Titel": carrier_title.strip()}
      r = get(url)
      html = BeautifulSoup(r.text, "lxml")
      for pair in html.find("table", {"class": "database-info"}).findAll("tr"):
        key = unicode(pair.th.contents[0]).strip(":")
        value = pair.td.contents
        if key == "Releasedatum":
          if len(value[0].split(".")) == 3:
            value = datetime(int(value[0].split(".")[2]), int(value[0].split(".")[1]), int(value[0].split(".")[0]))
          elif len(value[0].split(".")) == 2:
            value = datetime(int(value[0].split(".")[1]), int(value[0].split(".")[0]), 1)
        elif key in ["Uitvoerder(s)", "Genre(s)", "Label(s)"]:
          uitvoerders = []
          for v in value:
            if unicode(v).startswith("<a"):
              regex = compile('<a href="(.+?)">(.+?)</a>')
              a = regex.findall(unicode(v))[0]
              uitvoerders.append(a[1])
              if fetch_additional_urls:
                additional_urls.append("http://www.muziekcentrum.be/" + a[0] + "\n")
            else:
              uitvoerder = unicode(v).strip(", ")
              if uitvoerder:
                uitvoerders.append(uitvoerder)
          value = uitvoerders
        else:
          value = "".join([unicode(c) for c in pair.td.contents])
        doc[key] = value
      print "writing", doc["Titel"]
      collection.update_one(filter={"Uitvoerder(s)": doc["Uitvoerder(s)"], "Titel": doc["Titel"]}, update={"$set": doc}, upsert=True)

if fetch_additional_urls:
  with open("additional_urls.txt", "w", "utf-8") as f:
    f.writelines(additional_urls)

