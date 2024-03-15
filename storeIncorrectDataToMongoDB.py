from pymongo import MongoClient

# @profile
def storeIncorrectDataToMongoDB(incorrectData, uri, databaseName, collectionName):
    if(len(incorrectData) == 0):
        return
    try:
        client = MongoClient(uri)
        db = client[databaseName]
        collection = db[collectionName]
        collection.insert_many(incorrectData)
    except Exception as e:
        print(f"An error occurred while storing incorrect data to MongoDB: {e}")