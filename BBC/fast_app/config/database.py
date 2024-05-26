from pymongo import MongoClient
client=MongoClient("mongodb://mongoadmin:mongoadmin@localhost:27017/")
db=client.BBC
collection_name=db["BBC_news"]