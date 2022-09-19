1. How to add a new keyword?

- Add new topic line in topics.csv. Make sure to put 'Y' in the Used column. 
You can get the topic type from google trends when searching for the keyword.

- In google_trends_topics.py, run the add_topic_ids_to_csv function. It will
generate the topics_with_topics_id.csv. This file will used to perform extractions