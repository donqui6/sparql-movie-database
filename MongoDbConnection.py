import json


from pymongo.server_api import ServerApi
import os
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv, dotenv_values
from bson import json_util

class MongoDbConnection:
    def __init__(self):
        load_dotenv()
        self.uri = None
        self.client = None
        self.databaseName = None
        self.database = None
        self.collectionName = None
        self.mongodb_collection = None
        self.last_query = None

    def setUri(self, uri=None):
        self.uri = os.getenv("DB") or uri
        if not self.uri:
            raise ValueError("MongoDB URI is not set. Please provide a valid URI.")

    def setClient(self, uri=None):
        if self.uri is None and uri is None:
            raise ValueError("MongoDB URI is not set. Please provide a valid URI.")

        connection_uri = self.uri if self.uri else uri
        self.client = MongoClient(connection_uri, server_api=ServerApi('1'))

        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")

    def setDatabase(self, db_name=None):
        self.databaseName = os.getenv("DATABASE_NAME") or db_name
        self.database = self.client[self.databaseName]


    def setCollection(self,keepMongoCollection=False, collection_name=None):
        self.collectionName = collection_name or os.getenv("COLLECTION_MOVIE_NAME_1")
        if keepMongoCollection:
            self.mongodb_collection = self.database[self.collectionName]

    def emergencySetup(self):
        if self.mongodb_collection is None:
            self.database = self.client[self.databaseName]
            self.mongodb_collection = self.database[self.collectionName]
            if self.last_query is None:
                return "dbSendQuery error: no database or client setup."
        return "Successfully connected to database."

    def dbSendQuery(self,sparqlQuery, debug=False, saveLastQuery=False):
        query = sparqlQuery.queryAndConvert()

        self.emergencySetup()

        if saveLastQuery:
            self.last_query = query

        with open('person.json', 'w') as file:
            json.dump(query, file, indent=4)

        if debug:
            for r in query["results"]["bindings"]:
                print(r)

        self.mongodb_collection.insert_one(query)

        if debug:
            print("")
            print(self.mongodb_collection)

    def JsonPrint(self, debug=False, query=None):
        if query is None:
            final_query = self.last_query
        else:
            final_query = query.queryAndConvert()

        with open('person.json', 'w') as file:
            file.write(json_util.dumps(query, indent=4))

        if debug:
            for r in final_query["results"]["bindings"]:
                print(r)




if __name__ == "__main__":
    print("launched: MongoDbConnection class")