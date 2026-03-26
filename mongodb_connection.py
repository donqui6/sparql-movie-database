from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import os
from pymongo import MongoClient

from dotenv import load_dotenv, dotenv_values

load_dotenv()







def get_database():

    uri = "your mongodb uri"

    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)


    return client[os.getenv("DATABASE_NAME")]


if __name__ == "__main__":


    print("created database :" + os.getenv("DATABASE_NAME"))
    dbname = get_database()