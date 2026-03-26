import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv, dotenv_values
import bson


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
        self.collectionName = os.getenv("COLLECTION_MOVIE_NAME") or collection_name
        if keepMongoCollection:
            self.mongodb_collection = self.database[self.collectionName]

    def emergencySetup(self):
        if self.mongodb_collection is None:
            self.database = self.client[self.databaseName]
            self.mongodb_collection = self.database[self.collectionName]
            if self.last_query is None:
                return "dbSendQuery error: no database or client setup."
        return "dbSendQuery: setup ok."

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

        bson_data = bson.BSON.encode(query)

        self.mongodb_collection.insert_one(bson_data)

        if debug:
            print("")
            print(bson_data)

        self.client.close()


    def JsonPrint(self,debug=False, query=None):
        final_query = query.queryAndConvert()

        self.emergencySetup()

        if query is None:
            final_query = self.last_query

        with open('person.json', 'w') as file:
            json.dump(final_query, file, indent=4)

        if debug:
            for r in query["results"]["bindings"]:
                print(r)





if __name__ == "__main__":
    print("launched: MongoDbConnection")