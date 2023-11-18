import json
import os

folder_path = 'data'

# Get the list of files in the folder
files = os.listdir(folder_path)
data = []
# Print the list of files
for file in files:
    with open(os.path.join(folder_path, file), 'r', encoding='utf-8') as f:
        data += json.load(f, strict=False)
print(len(data))

unique_values = set()
result = []

with open('output_notclean.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=2)

for item in data:
    if 'tweets' in item:
        if item['all_tweets_id'][0] not in unique_values:
            unique_values.add(item['all_tweets_id'][0])
            for tweet in item['tweets']:
                result.append(tweet)
    elif item['original_tweet']['conversation_id_str'] not in unique_values:
        unique_values.add(item['original_tweet']['conversation_id_str'])
        result.append(item)
print(len(result))

#write result
with open('output.json', 'w', encoding='utf-8') as file:
    json.dump(result, file, indent=2)
