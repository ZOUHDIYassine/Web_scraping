from neo4j import GraphDatabase
import json

# Reading dictionary from a JSON file
with open('scraping_news_data.json', 'r') as json_file:
    json_data = json.load(json_file)

data = [{**value} for key, value in json_data.items()]

# Function to create nodes and relationships in Neo4j
def create_nodes_and_relationships(uri, username, password, data):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    session = driver.session()

    # Create Article nodes and index them by title for quick lookup
    article_nodes = {}
    for item in data:
        article_title = item['title']
        article_time = item['time'] if item['time'] is not None else '1970-01-01T00:00:00'  # Default value for time if it's None
        article_node = session.run(
            "MERGE (a:Article {title: $title, text: $text, author: $author, images: $images, videos: $videos, link: $link, time: $time, subtitle: $subtitle}) "
            "RETURN a",
            title=article_title,
            text=item['text'],
            author=item['author'],
            images=item['images'],
            videos=item['videos'],
            link=item['link'],
            time=article_time,
            subtitle=item['subtitle']
        ).single()[0]
        article_nodes[article_title] = article_node

    # Create relationships based on shared topics
    for item in data:
        article_title = item['title']
        for topic in item['topics']:
            for other_item in data:
                if other_item == item:
                    continue
                if topic in other_item['topics']:
                    other_article_title = other_item['title']
                    session.run(
                        "MATCH (a1:Article {title: $title1}), (a2:Article {title: $title2}) "
                        "MERGE (a1)-[:HAS_TOPIC {name: $topic}]->(a2)",
                        title1=article_title,
                        title2=other_article_title,
                        topic=topic
                    )

    session.close()

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "###"

# Create nodes and relationships in Neo4j
create_nodes_and_relationships(uri, username, password, data)
