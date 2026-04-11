import requests
import json
import time
from MongoDbConnection import MongoDbConnection
from SPARQLWrapper import SPARQLWrapper, JSON

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sparql_query(query_string: str) -> list[dict]:
    """Exécute une requête SPARQL et retourne les bindings."""
    sparql = SPARQLWrapper(WIKIDATA_SPARQL)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "TP2-RDF-Bot/1.0")
    sparql.setQuery(query_string)
    raw = sparql.queryAndConvert()
    return raw.get("results", {}).get("bindings", [])


def qid(uri: str) -> str:
    """Extrait le QID (ex: Q42) depuis une URI Wikidata complète."""
    return uri.split("/")[-1] if uri else ""


def val(row: dict, key: str) -> str:
    return row.get(key, {}).get("value", "")


# ---------------------------------------------------------------------------
# 1. Résolution nom → QID
# ---------------------------------------------------------------------------

def _is_actor(cqid: str) -> bool:
    sparql = SPARQLWrapper(WIKIDATA_SPARQL)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(f"""
        ASK {{
            wd:{cqid} wdt:P31  wd:Q5 ;
                      wdt:P106/wdt:P279* wd:Q33999 .
        }}
    """)
    try:
        return sparql.queryAndConvert().get("boolean", False)
    except Exception:
        return False


def search_actor_qid(name: str) -> str | None:
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "type": "item",
        "limit": 10,
        "format": "json",
    }
    resp = requests.get(WIKIDATA_API, params=params,
                        headers={"User-Agent": "TP2-RDF-Bot/1.0"})
    resp.raise_for_status()

    for candidate in resp.json().get("search", []):
        cqid = candidate["id"]
        if _is_actor(cqid):
            print(f"  ✓ '{name}' → {cqid} ({candidate.get('label', '')})")
            return cqid
        time.sleep(0.2)

    print(f"  ✗ Aucun acteur trouvé pour '{name}'")
    return None


def resolve_actors(names: list[str]) -> dict[str, str]:
    if not (3 <= len(names) <= 5):
        raise ValueError(f"Fournissez entre 3 et 5 acteurs (reçu : {len(names)}).")
    print("Résolution des noms d'acteurs…")
    result = {}
    for name in names:
        cqid = search_actor_qid(name)
        if cqid:
            result[name] = cqid
        time.sleep(0.5)
    if not result:
        raise RuntimeError("Aucun acteur résolu.")
    return result


# ---------------------------------------------------------------------------
# 2a. Requête 1 — films primés d'un acteur (minimaliste, ~1 s)
# ---------------------------------------------------------------------------

def fetch_awarded_films(actor_qid: str, max_films: int = 3) -> list[str]:
    """
    Retourne jusqu'à `max_films` QIDs de films ayant reçu un prix
    pour l'acteur donné.
    """
    rows = sparql_query(f"""
SELECT DISTINCT ?movie WHERE {{
  {{
    ?movie wdt:P31/wdt:P279* wd:Q11424 ;
           wdt:P161 wd:{actor_qid} ;
           wdt:P166 [] .
  }} UNION {{
    wd:{actor_qid} p:P166 ?awardStmt .
    ?awardStmt pq:P1686 ?movie .
    ?movie wdt:P31/wdt:P279* wd:Q11424 ;
           wdt:P161 wd:{actor_qid} .
  }}
  MINUS {{ ?movie wdt:P31/wdt:P279? wd:Q24856 }}
}}
LIMIT {max_films}
""")
    return [qid(val(r, "movie")) for r in rows if val(r, "movie")]


# ---------------------------------------------------------------------------
# 2b. Requête 2 — métadonnées d'un film (titre, année, genres, réalisateur, prix)
# ---------------------------------------------------------------------------

def fetch_film_metadata(movie_qid: str) -> dict:
    rows = sparql_query(f"""
    SELECT DISTINCT
      ?movieLabel ?releaseDate
      ?genre ?genreLabel
      ?director ?directorLabel ?directorBirth
      ?trophy ?trophyLabel
    WHERE {{
      # Ajouter cette ligne :
      wd:{movie_qid} rdfs:label ?movieLabel .
      FILTER(LANG(?movieLabel) = "fr" || LANG(?movieLabel) = "en")

      OPTIONAL {{ wd:{movie_qid} wdt:P577 ?releaseDate }}
      OPTIONAL {{ wd:{movie_qid} wdt:P136 ?genre }}
      OPTIONAL {{
        wd:{movie_qid} wdt:P57 ?director .
        OPTIONAL {{ ?director wdt:P569 ?directorBirth }}
      }}
      OPTIONAL {{ wd:{movie_qid} wdt:P166 ?trophy }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en". }}
    }}
    LIMIT 50
    """)

    film = {
        "qid": movie_qid,
        "title": "",
        "release_dates": set(),
        "genres": {},
        "directors": {},
        "awards": [],
        "cast": [],
    }

    for r in rows:
        if not film["title"]:
            film["title"] = val(r, "movieLabel")
        if rd := val(r, "releaseDate"):
            film["release_dates"].add(rd[:4])
        if g := qid(val(r, "genre")):
            film["genres"][g] = val(r, "genreLabel") or g
        if d := qid(val(r, "director")):
            film["directors"][d] = {
                "qid": d,
                "name": val(r, "directorLabel"),
                "birth_date": val(r, "directorBirth")[:10] if val(r, "directorBirth") else "",
            }
        if t := qid(val(r, "trophy")):
            entry = {"qid": t, "label": val(r, "trophyLabel")}
            if entry not in film["awards"]:
                film["awards"].append(entry)

    return film


# ---------------------------------------------------------------------------
# 2c. Requête 3 — casting d'un film (séparée pour rester légère)
# ---------------------------------------------------------------------------

def fetch_film_cast(movie_qid: str) -> list[dict]:
    """
    Retourne le casting du film (max 20 membres).
    Séparé de fetch_film_metadata pour éviter l'explosion combinatoire.
    """
    rows = sparql_query(f"""
    SELECT DISTINCT ?member ?memberLabel ?memberBirth ?roleLabel WHERE {{
        wd:{movie_qid} p:P161 ?castStmt .
        ?castStmt ps:P161 ?member .
        OPTIONAL {{ ?member wdt:P569 ?memberBirth }}
        OPTIONAL {{
            ?castStmt pq:P453 ?role .
            ?role rdfs:label ?roleLabel .
            FILTER(lang(?roleLabel) = "fr" || lang(?roleLabel) = "en")
        }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en". }}
    }}
    LIMIT 20
    """)

    cast: dict[str, dict] = {}
    for r in rows:
        m = qid(val(r, "member"))
        if not m:
            continue
        entry = cast.setdefault(m, {
            "qid": m,
            "name": val(r, "memberLabel"),
            "birth_date": val(r, "memberBirth")[:10] if val(r, "memberBirth") else "",
            "roles": set(),
        })
        if role := val(r, "roleLabel"):
            entry["roles"].add(role)

    return [
        {**c, "roles": sorted(c["roles"])}
        for c in cast.values()
    ]


# ---------------------------------------------------------------------------
# 3. Orchestration : acteur → films → détails
# ---------------------------------------------------------------------------

def collect_data(actor_qids: dict[str, str], max_films: int = 3) -> list[dict]:
    documents = []

    for actor_name, actor_qid in actor_qids.items():
        print(f"\n── {actor_name} ({actor_qid})")

        # Étape A : films primés (requête légère)
        movie_qids = fetch_awarded_films(actor_qid, max_films)
        print(f"   {len(movie_qids)} film(s) primé(s) : {movie_qids}")
        time.sleep(1)

        films = []
        for mq in movie_qids:
            print(f"   ├─ {mq} métadonnées…", end=" ", flush=True)

            # Étape B : métadonnées (requête légère)
            film = fetch_film_metadata(mq)
            time.sleep(1)

            # Étape C : casting (requête légère)
            film["cast"] = fetch_film_cast(mq)
            time.sleep(1)

            # Sérialisation des sets Python → listes JSON
            film["release_dates"] = sorted(film["release_dates"])
            film["genres"] = list(film["genres"].values())
            film["directors"] = list(film["directors"].values())

            films.append(film)
            print(f"✓ '{film['title']}'")

        documents.append({
            "actor_name": actor_name,
            "actor_qid": actor_qid,
            "films": films,
        })

    return documents


# ---------------------------------------------------------------------------
# 4. Point d'entrée
# ---------------------------------------------------------------------------

def main():
    # ── Modifier cette liste (3 à 5 acteurs) ────────────────────────────────
    actor_names = [
        "Tom Hanks",
        "Meryl Streep",
        "Leonardo DiCaprio",
    ]

    # 1) Résolution noms → QIDs Wikidata
    actor_qids = resolve_actors(actor_names)

    # 2) Collecte film par film (3 requêtes légères par film)
    documents = collect_data(actor_qids, max_films=3)

    # 3) Sauvegarde JSON locale
    with open("movies.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)
    print("\n✓ movies.json sauvegardé")

    # 4) Insertion MongoDB
    db = MongoDbConnection()
    db.setUri()
    db.setClient()
    db.setDatabase()
    db.setCollection(keepMongoCollection=True)

    for doc in documents:
        db.mongodb_collection.insert_one(doc)
        print(f"✓ MongoDB ← {doc['actor_name']} ({len(doc['films'])} film(s))")

    db.client.close()
    print("\nTerminé.")


if __name__ == "__main__":
    main()
