import json
import pandas as pd
from functools import reduce

with open("extract-field\tweet.txt", "r") as post:
    post_tag = [data.strip().split(" ") for data in post]
    post_data = {key : [] for key in [sub[-1] for sub in post_tag] }

with open("extract-field\user.txt", "r") as user:
    user_tag = [data.strip().split(" ") for data in user]
    user_data = {key : [] for key in [sub[-1] for sub in user_tag] }

file = open("output.json", 'r', encoding="utf-8")
data = json.load(file)
unique_user = set()

for post in data:
    #check language: if english -> continue
    if post["original_tweet"]["lang"] != "en":
        continue
    
    #check user are in set or not
    if post["author"]["original_user"]["screen_name"] not in unique_user:
        unique_user.add(post["author"]["original_user"]["screen_name"])

        #check post has "verified_type": dont have-> None
        if "verified_type" not in post['author']['original_user']:
            user_data["verified_type"].append(None)
        else:
            user_data["verified_type"].append(post['author']['original_user']['verified_type'])

        for utag in user_tag[1:]:
            user_data[utag[-1]].append(reduce(lambda d, k: d[k], utag, post))
    
    #get post data:
    #first get all tag:
    hashtagtext = ""
    for idx, hashtag in enumerate(post['hashtags']):
        if idx < len(post['hashtags']) - 1:
            hashtagtext += hashtag["text"] + ", "
        else:
            hashtagtext += hashtag["text"]
    post_data['hashtags'].append(hashtagtext)
    for ptag in post_tag[1:]:
        post_data[ptag[-1]].append(reduce(lambda d, k: d[k], ptag, post))

#convert to csv file
user_df = pd.DataFrame(user_data)
user_df.to_csv("user.csv", index=False)

post_df = pd.DataFrame(post_data)
post_df.to_csv("post.csv", index=False)

