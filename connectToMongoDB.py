from pymongo import MongoClient

# @profile
def connectToMongoDB(uri, db_name):
    try:
        client = MongoClient(uri)
        db = client[db_name]
        return db
    except Exception as e:
        print(f"An error occurred while connecting to MongoDB: {e}")
        return None