import os

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
        "language": "fr",  # priorité au français
        "language_fallback": 1,  # fallback vers l'anglais si absent
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
# 2a. Requête 1 — films primés d'un acteur
# ---------------------------------------------------------------------------

def fetch_awarded_films(actor_qid: str, max_films: int = 3) -> list[str]:
    """
    Retourne jusqu'à `max_films` QIDs de films ayant reçu un prix
    pour l'acteur donné. ORDER BY pour des résultats déterministes.
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
ORDER BY ?movie
LIMIT {max_films}
""")
    return [qid(val(r, "movie")) for r in rows if val(r, "movie")]


# ---------------------------------------------------------------------------
# 2b. Requête 2 — métadonnées d'un film
# ---------------------------------------------------------------------------

def fetch_film_metadata(movie_qid: str) -> dict:
    rows = sparql_query(f"""
SELECT DISTINCT
  ?movieLabel ?releaseDate
  ?genre ?genreLabel
  ?director ?directorLabel ?directorBirth
  ?trophy ?trophyLabel
WHERE {{
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

    # Accumulation temporaire pour dédoublonner
    _years = set()
    _genres = {}  # wikidata_id → label
    _directors = {}  # wikidata_id → dict (on garde le premier réalisateur)
    _awards = []

    title = ""

    for r in rows:
        if not title:
            title = val(r, "movieLabel")
        if rd := val(r, "releaseDate"):
            _years.add(rd[:4])
        if g := qid(val(r, "genre")):
            _genres[g] = val(r, "genreLabel") or g
        if d := qid(val(r, "director")):
            _directors[d] = {
                "wikidata_id": d,
                "name": val(r, "directorLabel"),
                "birth_date": val(r, "directorBirth")[:10] if val(r, "directorBirth") else "",
            }
        if t := qid(val(r, "trophy")):
            entry = {"wikidata_id": t, "label": val(r, "trophyLabel")}
            if entry not in _awards:
                _awards.append(entry)

    # On retient l'année de sortie la plus ancienne (première sortie)
    year = int(min(_years)) if _years else None

    # Le réalisateur principal est le premier trouvé
    director = list(_directors.values())[0] if _directors else None

    return {
        "wikidata_id": movie_qid,
        "title": title,
        "year": year,
        "genres": list(_genres.values()),
        "director": director,
        "awards": _awards,
        "cast": [],  # rempli juste après par fetch_film_cast
    }


# ---------------------------------------------------------------------------
# 2c. Requête 3 — casting d'un film
# ---------------------------------------------------------------------------

def fetch_film_cast(movie_qid: str) -> list[dict]:
    """
    Retourne le casting du film (max 20 membres).
    Chaque membre possède : wikidata_id, name, birth_date, role.
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
        # setdefault : on n'écrase pas un rôle déjà trouvé pour ce membre
        entry = cast.setdefault(m, {
            "wikidata_id": m,
            "name": val(r, "memberLabel"),
            "birth_date": val(r, "memberBirth")[:10] if val(r, "memberBirth") else "",
            "role": val(r, "roleLabel"),  # chaîne, pas un set
        })
        # Si le rôle était vide et qu'on en trouve un maintenant, on le complète
        if not entry["role"] and val(r, "roleLabel"):
            entry["role"] = val(r, "roleLabel")

    return list(cast.values())


# ---------------------------------------------------------------------------
# 3. Orchestration : acteur → films → 1 document par film
# ---------------------------------------------------------------------------

def collect_data(actor_qids: dict[str, str], max_films: int = 3) -> list[dict]:
    """
    Retourne une liste plate de documents films.
    Chaque document correspond à UN film (structure attendue par MongoDB).
    Si deux acteurs partagent un film, le film n'est inséré qu'une seule fois.
    """
    # wikidata_id → document film  (pour éviter les doublons)
    films_by_id: dict[str, dict] = {}

    for actor_name, actor_qid in actor_qids.items():
        print(f"\n── {actor_name} ({actor_qid})")

        try:
            movie_qids = fetch_awarded_films(actor_qid, max_films)
        except Exception as e:
            print(f"   ✗ Erreur fetch_awarded_films : {e}")
            continue

        print(f"   {len(movie_qids)} film(s) primé(s) : {movie_qids}")
        time.sleep(1)

        for mq in movie_qids:
            # Film déjà traité via un autre acteur → on saute
            if mq in films_by_id:
                print(f"   ├─ {mq} déjà traité, ignoré.")
                continue

            print(f"   ├─ {mq} métadonnées…", end=" ", flush=True)

            try:
                film = fetch_film_metadata(mq)
                time.sleep(1)
                film["cast"] = fetch_film_cast(mq)
                time.sleep(1)
            except Exception as e:
                print(f"\n   ✗ Erreur pour {mq} : {e}")
                continue

            films_by_id[mq] = film
            print(f"✓ '{film['title']}' ({film['year']})")

    return list(films_by_id.values())
# ---------------------------------------------------------------------------
# 4. constructeur de actors
# ---------------------------------------------------------------------------
def build_actor_documents(actor_qids: dict[str, str], film_documents: list[dict]) -> list[dict]:
    """
    Construit 1 document par acteur à partir des films déjà collectés.
    Chaque document contient au minimum : wikidata_id, name, et la liste
    des films (titre + réalisateur) dans lesquels l'acteur a joué.
    """
    actor_docs = []

    for actor_name, actor_qid in actor_qids.items():
        # Trouve tous les films où cet acteur apparaît dans le cast
        actor_films = []
        for film in film_documents:
            cast_ids = [member["wikidata_id"] for member in film.get("cast", [])]
            if actor_qid in cast_ids:
                actor_films.append({
                    "wikidata_id": film["wikidata_id"],
                    "title": film["title"],
                    "year": film.get("year"),
                    "director": film.get("director"),  # déjà un dict {wikidata_id, name, birth_date}
                })

        actor_docs.append({
            "wikidata_id": actor_qid,
            "name": actor_name,
            "films": actor_films,
        })

    return actor_docs

# ---------------------------------------------------------------------------
# 4. Point d'entrée
# ---------------------------------------------------------------------------

def main():
    actor_names = ["Tom Cruise", "Brad Pitt", "Johnny Depp", "Will Smith", "Harrison Ford"]

    actor_qids = resolve_actors(actor_names)
    film_documents = collect_data(actor_qids, max_films=3)
    print(f"\n{len(film_documents)} film(s) collecté(s) au total.")

    with open("movies.json", "w", encoding="utf-8") as f:
        json.dump(film_documents, f, ensure_ascii=False, indent=2)
    print("✓ movies.json sauvegardé")

    db = MongoDbConnection()
    db.setUri()
    db.setClient()
    db.setDatabase()

    # ── Collection movies ──────────────────────────────────────────────
    db.setCollection(keepMongoCollection=True)  # utilise COLLECTION_MOVIE_NAME_1
    for film in film_documents:
        try:
            db.mongodb_collection.insert_one(film)
            print(f"✓ movies ← '{film['title']}' ({film['wikidata_id']})")
        except Exception as e:
            print(f"✗ Erreur insertion film '{film.get('title', '?')}' : {e}")

    # ── Collection actors ──────────────────────────────────────────────
    actor_documents = build_actor_documents(actor_qids, film_documents)

    db.setCollection(
        keepMongoCollection=True,
        collection_name=os.getenv("COLLECTION_MOVIE_NAME_2")
    )
    for actor in actor_documents:
        try:
            db.mongodb_collection.insert_one(actor)
            print(f"✓ actors ← '{actor['name']}' ({actor['wikidata_id']})")
        except Exception as e:
            print(f"✗ Erreur insertion acteur '{actor.get('name', '?')}' : {e}")

    db.client.close()
    print("\nTerminé.")


if __name__ == "__main__":
    main()
