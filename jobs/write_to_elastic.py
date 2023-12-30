from google.cloud.storage import Client
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(['http://34.143.255.36:9200/'], http_auth=('elastic', 'elastic2023'))
# Create a client using the credentials
client = Client.from_service_account_json('service-account/key.json')
# Replace 'your_bucket_name' with the name of your Google Cloud Storage bucket
bucket_name = 'it4043e-it5384'

# Replace 'source_object_name' with the name of the object you want to download
folder = 'it4043e/it4043e_group12_problem3/transformed-data/'
files = ['user_data_cleaned/part-00000-85840b5e-8086-464f-b640-68fe070dbae2-c000.snappy.parquet', 
         'post_data_cleaned/part-00000-0cfd1295-f0b6-4c0d-8b1e-c43658b6b2e6-c000.snappy.parquet', 
         'post_data_cleaned/part-00001-0cfd1295-f0b6-4c0d-8b1e-c43658b6b2e6-c000.snappy.parquet']
destination = 'data-storage/'
destination_files = ['user_data.parquet', 
                     'post_data(1).parquet', 
                     'post_data(2).parquet']

# Get the bucket and the specific blob (object) within the bucket
bucket = client.get_bucket(bucket_name)

def get_blob(source_blob_name, destination_file_name):
  # Download the blob to a specified destination
  blob = bucket.blob(source_blob_name)
  blob.download_to_filename(destination_file_name)

  print(f'File {source_blob_name} downloaded to {destination_file_name}.')

for i in range(0, len(files)):
  source_blob_name = folder + files[i]
  destination_file_name = destination + destination_files[i]
  get_blob(source_blob_name, destination_file_name)
df_user = pd.read_parquet("data-storage/user_data.parquet")
df1 = pd.read_parquet("data-storage/post_data(1).parquet")
df2 = pd.read_parquet("data-storage/post_data(2).parquet")
df2 = df2.iloc[1:]
# Find common columns
common_columns = df1.columns.intersection(df2.columns)

# Concatenate the rows using only the common columns
df_tweet = pd.concat([df1[common_columns], df2[common_columns]], axis=0, ignore_index=True)

def index_user_data_to_elasticsearch_custom(df, es, index_name):
    actions = []
    for _, row in df.iterrows():
        action = {
            "_index": index_name,
            "_source": {
                'verified_type': row["verified_type"],
                'can_dm': row["can_dm"],
                'can_media_tag': row["can_media_tag"],
                'created_at': row["created_at"],
                'default_profile': row["default_profile"],
                'description': row["description"],
                'favourites_count': row["favourites_count"],
                'followers_count': row["followers_count"],
                'friends_count': row["friends_count"],
                'has_custom_timelines': row["has_custom_timelines"],
                'listed_count': row["listed_count"],
                'location': row["location"],
                'media_count': row["media_count"],
                'screen_name': row["screen_name"],
                'statuses_count': row["statuses_count"],
                'verified': row["verified"],
                'profile_url': row["profile_url"],
                'time_bot_count': row["time_bot_count"],
                'views_score': row["views_score"],
                'following/follower': row["following/follower"],
                'friend_score': row["friend_score"],
                'bot_score': row["bot_score"]
                # Add other fields as needed
            }
        }
        actions.append(action)

    helpers.bulk(es, actions)
def index_tweet_data_to_elasticsearch_custom(df, es, index_name):
    actions = []
    for _, row in df.iterrows():
        action = {
            "_index": index_name,
            "_source": {
                'screen_name': row["screen_name"],
                'hashtags': row["hashtags"],
                'bookmark_count': row["bookmark_count"],
                'created_at': row["created_at"],
                'favorite_count': row["favorite_count"],
                'is_quote_status': row["is_quote_status"],
                'quote_count': row["quote_count"],
                'reply_count': row["reply_count"],
                'retweet_count': row["retweet_count"],
                'user_id_str': row["user_id_str"],
                'id': row["id"],
                'is_retweet': row["is_retweet"],
                'is_quoted': row["is_quoted"],
                'is_reply': row["is_reply"],
                'views': row["views"],
                'url': row["url"],
                'time_diff': row["time_diff"],
                'average_time_diff': row["average_time_diff"],
                'time_diff_seconds': row["time_diff_seconds"],
                'average_time_diff_seconds': row["average_time_diff_seconds"],
                'time_bot': row["time_bot"],
                'views/like': row["views/like"],
                'views_score': row["views_score"]
                # Add other fields as needed
            }
        }
        actions.append(action)

    helpers.bulk(es, actions)
# index_user_data_to_elasticsearch_custom(df = df_user, es = es, index_name = "group12_user_test")
# index_tweet_data_to_elasticsearch_custom(df = df_tweet, es = es, index_name = "group12_user_tweet")