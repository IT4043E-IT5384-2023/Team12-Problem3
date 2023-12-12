from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
load_dotenv()

# Configure Kafka consumer
KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_ETHER_TOPIC")
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_URL, group_id='tweet_consumer')

# Function to process and handle incoming tweets
def process_tweet(tweet_json):
    try:
        tweet_data = json.loads(tweet_json)
        # Add your processing logic here
        print("Received tweet:", tweet_data)
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)

# Main script for consuming tweets
if __name__ == "__main__":
    try:
        for message in consumer:
            tweet_json = message.value.decode('utf-8')
            process_tweet(tweet_json)
    except KeyboardInterrupt:
        print("Consumer interrupted. Closing.")
    finally:
        # Close Kafka consumer
        consumer.close()
