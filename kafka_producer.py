from typing import List, Union
from tweety import Twitter
from kafka import KafkaProducer
import os
import json
import sys
import yaml
from dotenv import load_dotenv
load_dotenv()

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

# Function to read YAML configuration
def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")
    return config

KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_ETHER_TOPIC")
# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_URL)

# Specify the JSON filename
json_filename = 'output.json'

# Function to send tweet to Kafka
def send_to_kafka(tweet):
    tweet_json = json.dumps(tweet, ensure_ascii=False, indent=4, default=str)
    producer.send(KAFKA_TOPIC, value=tweet_json.encode('utf-8'))

# Function to crawl tweets of users
def crawl_tweet_user(app,
    users: Union[str, List[str]],
    username: Union[str, List[str]],
    pages: int = 10,
    wait_time: int = 5):
    for idx, user in enumerate(users):
        print(f"Crawling tweets of '@{user}'")
        all_tweets = app.get_tweets(username=f"{user}", pages=pages, wait_time=wait_time)
        for tweet in all_tweets:
            send_to_kafka(tweet)
            print(tweet.__dict__)

# Function to convert data to JSON and send to Kafka
def convert_to_json(data):
    for tweet in data:
        send_to_kafka(tweet)

# Main script
if __name__ == "__main__":
    # Login Twitter account
    app = Twitter("session")
    with open("account.key", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)

    # Read config file
    CONFIG_PATH = os.path.join(os.getcwd(), "config_users.yaml")
    config = read_yaml(path=CONFIG_PATH)

    # Crawl tweets and send to Kafka
    crawl_tweet_user(
        app=app,
        users=config['users'],
        username=config['username'],
        pages=config['pages'],
        wait_time=config['wait_time']
    )

    # Close Kafka producer
    producer.close()
