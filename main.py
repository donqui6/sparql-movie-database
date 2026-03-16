import json
from mongodb_connection import get_database
from SPARQLWrapper import SPARQLWrapper, JSON
import os
from bson.json_util import dumps
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

try:
    ret = sparql.queryAndConvert()

    dbname = get_database()
    collection_name = dbname[os.getenv("COLLECTION_MOVIE_NAME")]

    documents = collection_name.find()

    with open("person.json", "w") as f:
        f.write(dumps(documents))

    with open('person.json', 'w') as file:
        json.dump(ret, file, indent=4)
    for r in ret["results"]["bindings"]:
        print(r)


except Exception as e:
    print(e)

