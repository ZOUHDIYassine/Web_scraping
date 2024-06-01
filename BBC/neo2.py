from neo4j import GraphDatabase
from neo4j import GraphDatabase
import json

# Function to create nodes in Neo4j
def create_nodes(uri, username, password, data):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    session = driver.session()

    # Create Article nodes
    for item in data:
        article_title = item['title']
        article_time = item['time'] if item['time'] is not None else '1970-01-01T00:00:00'
        session.run(
            "MERGE (a:Article {title: $title, text: $text, author: $author, images: $images, videos: $videos, link: $link, time: $time, subtitle: $subtitle, topics: $topics})",
            title=article_title,
            text=item['text'],
            author=item['author'],
            images=item['images'],
            videos=item['videos'],
            link=item['link'],
            time=article_time,
            subtitle=item['subtitle'],
            topics=item['topics']
        )

    session.close()

# Function to add new articles from a JSON file
def add_new_articles_from_json(uri, username, password, json_file_path):
    # Reading dictionary from a JSON file
    with open(json_file_path, 'r') as json_file:
        new_data = json.load(json_file)

    data = [{**value} for key, value in new_data.items()]

    create_nodes(uri, username, password, data)

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "#####"

i=1
while i<220:
    file='2024_data_page_'+str(i)+'.json'
    # Initial creation of nodes
    with open(file, 'r') as json_file:
        json_data = json.load(json_file)

    data = [{**value} for key, value in json_data.items()]

    create_nodes(uri, username, password, data)

    # Adding new articles from another JSON file
    add_new_articles_from_json(uri, username, password, file)
    print(i)
    i=i+1

# Initial creation of nodes
#with open('travel_data_page_11.json', 'r') as json_file:
#    json_data = json.load(json_file)

#data = [{**value} for key, value in json_data.items()]

#create_nodes(uri, username, password, data)

# Adding new articles from another JSON file
#add_new_articles_from_json(uri, username, password, 'travel_data_page_11.json')
