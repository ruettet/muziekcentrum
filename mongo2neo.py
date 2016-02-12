from pymongo import MongoClient
from py2neo import Graph, Path, Node, Relationship

db = MongoClient('mongodb://<user>:<pass>@ds<id>.mongolab.com:<port>/<db>')
collection = db["muziekcentrum"]["muziekcentrum"]

graph = Graph("http://<user>:<apikey>@<db>.sb02.stations.graphenedb.com:<port>/db/data/")

graph.cypher.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE r,n")

graph.schema.drop_uniqueness_constraint("Album", "name")
graph.schema.drop_uniqueness_constraint("Uitvoerder", "name")
graph.schema.drop_uniqueness_constraint("Label", "name")

graph.schema.create_uniqueness_constraint("Album", "name")
graph.schema.create_uniqueness_constraint("Uitvoerder", "name")
graph.schema.create_uniqueness_constraint("Label", "name")


for doc in collection.find({"Type": "album"}):
  for uitvoerder in doc["Uitvoerder(s)"]:
    uitvoerder_node = graph.merge_one("Uitvoerder", "name", uitvoerder)
    album_node = graph.merge_one("Album", "name", doc["Titel"])
    uitvoerder_makes_album = Relationship(uitvoerder_node, "MADE", album_node)
    graph.create_unique(uitvoerder_makes_album)

  for label in doc["Label(s)"]:
    label_node = graph.merge_one("Label", "name", label)
    album_node = graph.merge_one("Album", "name", doc["Titel"])
    label_releases_album = Relationship(label_node, "RELEASED", album_node)
    graph.create_unique(label_releases_album)
