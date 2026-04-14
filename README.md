# Tutoriel


## Etape 1 : créer un projet
Créer le projet.
Soit vous utilisez pycharm et vous pouvez créer un projet python,
soit vous faites "à la main" (avec visual studio code par exemple).

Si vous décidez de faire à la main, pensez à créer un .venv (environnement virtuel) pour ne pas avoir à installer les librairies directement sur votre ordinateur : 

Dans un terminal qui pointe vers le dossier du projet créer : 

> python -m venv .venv

Cela va créer un .venv (environnement virtuel).

Ensuite, toujours depuis le terminal, activez le : 

> .venv\Scripts\activate


Maintenant, votre terminal utilise l’environnement, on peut installer tout pour que le projet fonctionne grâce à une seule commande :

> pip install -r requirements.txt


requirements.txt continent toutes les librairies (et le “pymongo[srv]”) dont mongodb à besoin.

## Etape 2 : configuer le .env

Créer un .env (c’est à dire, créer un fichier qui s’appelle juste “.env”)
un .env est un fichier fait pour stocker des informations importantes (le lien générer par mongodb)

*Ce fichier est lu par le script et utilise
les données dedans pour fonctionner.*


Il a besoin de trois choses : 
- DATABASE_NAME : le nom de la database dans le cluster mongodb.
- COLLECTION_MOVIE_NAME_(½) : le nom de la collection dans la database.
- DB : le lien généré par mongodb.

*COLLECTION_MOVIE_NAME_1 et COLLECTION_MOVIE_NAME_2
doivent être nommés “movies” et “actors” comme demandé
dans le cahier des charges.*


Voici un exemple, mettez votre lien mongodb,
celui-ci ne fonctionne pas.
```
DATABASE_NAME = "database"
COLLECTION_MOVIE_NAME_1 = "movies"
COLLECTION_MOVIE_NAME_2 = "actors"
DB= mongodb+srv://<DB USER>:<MOT DE PASSE>@sparql-db....
```
