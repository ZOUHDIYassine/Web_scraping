import pymongo
from pymongo import MongoClient
import json

# Reading dictionary from a JSON file
with open('scraping_news_data.json', 'r') as json_file:
    data = json.load(json_file)


# Connect to MongoDB (please change the credentials in the url before testing)
client = MongoClient('mongodb://mongoadmin:mongoadmin@localhost:27017/')
db = client.BBC
collection = db.BBC_news
# Insert the data
# Convert the dictionary to a list of documents
#documents = [{'_id': key, **value} for key, value in data.items()]
documents = [{**value} for key, value in data.items()]

# Insert documents into MongoDB
collection.insert_many(documents)

print("Data has been inserted successfully.")