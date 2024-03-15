from connectToMongoDB import connectToMongoDB
from storeDataIntoClickHouse import storeDataIntoClickHouse
from filterData import filterData
from storeIncorrectDataToMongoDB import storeIncorrectDataToMongoDB

from datetime import datetime
from dotenv import load_dotenv


import os

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")


CLICKHOUSE_HOST =  os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_DATABASE_NAME = os.getenv("CLICKHOUSE_DATABASE_NAME")

INCORRECT_MONDODB_URI =  os.getenv("INCORRECT_MONDODB_URI")
INCORRECT_DATABASE_NAME = os.getenv("INCORRECT_DATABASE_NAME")
INCORRECT_COLLECTION_NAME = os.getenv("INCORRECT_COLLECTION_NAME")


if __name__ == "__main__":

    load_dotenv()
    startTime = datetime.now()
    try:
        database = connectToMongoDB(MONGODB_URI, DATABASE_NAME)   
        print("Connected to MongoDB")

        [correctData, incorrectData] = filterData(database)

        # i=0
        # print(incorrectData[i]["event_name"])
        # print(dict.keys(incorrectData[i]))

        check2 = storeDataIntoClickHouse(correctData, CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD, CLICKHOUSE_USER, CLICKHOUSE_DATABASE_NAME)
        print("Data stored to ClickHouse")

        storeIncorrectDataToMongoDB(incorrectData, INCORRECT_MONDODB_URI, INCORRECT_DATABASE_NAME, INCORRECT_COLLECTION_NAME)
        print("Incorrect data stored to MongoDB")
        

        print("Process completed")
    except  Exception as e:
        print(f"An error occurred: {e}")
