from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
# importing os module for environment variables
import os
from pymongo import MongoClient
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values
# loading variables from .env file
load_dotenv()







def get_database():

    uri = "your mongodb uri"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client[os.getenv("DATABASE_NAME")]


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":


    print("created database :" + os.getenv("DATABASE_NAME"))
    dbname = get_database()