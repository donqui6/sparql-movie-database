from MongoDbConnection import *
from SPARQLWrapper import SPARQLWrapper, JSON


sparql = SPARQLWrapper(
    "https://query.wikidata.org/sparql"
)
sparql.setReturnFormat(JSON)

sparql.setQuery(
"""

PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
SELECT DISTINCT ?movie ?movieLabel ?trophy ?trophyLabel ?awardScope WHERE {
  VALUES ?actor { wd:Q2263 wd:Q4547 }   # exemple : Tom Hanks et Daniel Craig

  {
    # 1) Prix reçu directement par le film
    ?movie wdt:P31/wdt:P279* wd:Q11424 ;
           wdt:P161 ?actor ;
           p:P166 ?awardStmt .

    ?awardStmt ps:P166 ?trophy .

    BIND("film" AS ?awardScope)
  }
  UNION
  {
    # 2) Prix reçu par l'acteur pour ce film
    ?actor p:P166 ?awardStmt .
    ?awardStmt ps:P166 ?trophy ;
               pq:P1686 ?movie .

    ?movie wdt:P31/wdt:P279* wd:Q11424 ;
           wdt:P161 ?actor .

    BIND("acteur" AS ?awardScope)
  }
  MINUS{?movie wdt:P31/wdt:P279? wd:Q24856} # n'est pas un feuilleton

  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}
LIMIT 3

"""
)

sparql2 = SPARQLWrapper(
    "https://query.wikidata.org/sparql"
)
sparql2.setReturnFormat(JSON)

sparql2.setQuery(
"""
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX org:  <http://www.w3.org/ns/org#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?name
WHERE {
  ?p a foaf:Person ;
     rdfs:label ?name ;
     org:memberOf ?org .

  FILTER(lang(?name) = "en" || lang(?name) = "")
}
ORDER BY ?name
LIMIT 5
"""
)

try:
    Database = MongoDbConnection()

    Database.setUri()
    Database.setClient()
    Database.setDatabase()
    Database.setCollection(True)
    Database.dbSendQuery(sparql, True, True)
    Database.JsonPrint(True)
    Database.client.close()

except Exception as e:
    print(e)

