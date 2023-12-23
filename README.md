# Team12-Problem3

## Setup
1. **Python Environment**
Run install the requirements: 
```python
pip install -r requirement.txt
```
- Create a file name account.key including username, password, and bearer token to Twitter account.
- Please remember to edit the path to Google storage in each file
2. **Crawled data**
- Changed the keyword or user to crawling in config_keywords.yaml or config_users.yaml file.
- Create a folder **data** to store the data
- Running the code in tweet-crawler\tweet_crawler_search.py or tweet-crawler\tweet_crawler_user.py to start crawling
```
python tweet-crawler\tweet_crawler_search.py || python tweet-crawler\tweet_crawler_user.py
```
- Running the code in tweet_crawler_search.py or tweet_crawler_user.py to start crawling
3. **Data Preprocessing**
- Running the code in data-preprocessing\check_duplicate.py to remove the duplicate records
- Remember to edit the folder data path to exactly
```
python data-preprocessing\check_duplicate.py
```
- Running the code in data-preprocessing\clean_data.py to keep only potentially relevant fields for each entity
- Remember to edit the file data path to exactly
```
python data-preprocessing\clean_data.py
```
- Open the JupyterHub environment to run data-preprocessing\preprocessing_data.py using Spark to process the data before evaluation.
- Remember to edit the file data path to exactly
```
python data-preprocessing\preprocessing_data.py
```
4. **Data Analysis**
- Continue in the JupyterHub environment
- Running the code in write_to_elastic.py to index the data to Kibana to create Dashboards for analysts.
- Remember to edit the file data path to exactly
```
python write_to_elastic.py
```
- After that, implement for analysis in Kibana.
5. **Evaluation**
- Continue in the JupyterHub environment
- Running the code in model_evaluation.py to cluster KOL and project accounts data by KMeans
- Remember to edit the file data path to exactly
```
python model_evaluation.py
```

## Data Structures
Please visit Data documentation – Group 12.docx

## Report
Please visit our [Google Driver folder](https://drive.google.com/drive/folders/1YJIYe9-Jf7nU25gB8Ifnw5GW-s94L0EY) which contains all of our documents, presentation slides, video demonstration for this project.
