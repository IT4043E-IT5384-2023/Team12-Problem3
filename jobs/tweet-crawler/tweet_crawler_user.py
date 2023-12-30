from typing import List, Union
from tweety import Twitter
from google.cloud import storage
import os
import json
import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
import yaml

def upload_file_to_google_cloud_storage(bucket_name, file_name, local_path):
    client = storage.Client.from_service_account_json('service-account/key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_path, content_type='application/json')

    print(f'File {local_path} uploaded to {file_name} in {bucket_name} bucket.')
def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")
    return config


# Specify the JSON filename
json_filename = 'output.json'
def convert_to_json(data, json_filename=json_filename):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        json_file.write('[')
        for idx, tweet in enumerate(data):
            json.dump(tweet, json_file, ensure_ascii=False, indent=4, default=str)
            if idx < len(data) - 1:
                json_file.write(',')  # Add a comma between objects
            json_file.write('\n')
        json_file.write(']')

def crawl_tweet_user(
    app,
    users: Union[str, List[str]],
    username: Union[str, List[str]],
    pages: int = 10,
    wait_time: int = 5
):
    for idx, user in enumerate(users):
        print(f"Crawling tweets of @'{user}'")
        all_tweets = app.get_tweets(username = f"{user}", pages = pages, wait_time = wait_time)
        convert_to_json(all_tweets, f"{username[idx]}.json")
        upload_file_to_google_cloud_storage('it4043e-it5384', f"it4043e/it4043e_group12_problem3/raw-data/data/{username[idx]}.json", f"data/{username[idx]}.json")
if __name__ == "__main__":
    # Login Twitter account
    app = Twitter("session")
    with open("account.key", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)

    # Read config file
    CONFIG_PATH = os.path.join(os. getcwd(), "jobs/config-crawler/config_users.yaml")
    config = read_yaml(path=CONFIG_PATH)

    crawl_tweet_user(app = app,
        users=config['users'],
        username=config['username'],
        pages=config['pages'],
        wait_time=config['wait_time']
    )
    print("Crawled complete!")