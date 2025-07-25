import pymongo
from pymongo import MongoClient, TEXT
from mysql_database import *

# Function to create a connection to MongoDB
def create_mongo_connection(uri):
    client = MongoClient(uri)
    return client


# Connect to MongoDB
mongo_client = MongoClient(mongo_config['uri'])

# Check if the "TwitterData" database exists and drop it if it does
if "TwitterData" in mongo_client.list_database_names():
    mongo_client.drop_database(mongo_config['db'])

twitter_db = mongo_client[mongo_config['db']]
tweets_collection = twitter_db[mongo_config['tweets_collection']]
hashtags_collection = twitter_db[mongo_config['hashtags_collection']]

# Ensure indexes
tweets_collection.create_index([("tweet_id", 1)], unique=True)
tweets_collection.create_index([("hashtags.text", 1)])
tweets_collection.create_index([("text", TEXT)])  # Text index for full-text search
tweets_collection.create_index([("created_at", 1)])


def parse_twitter_date(twitter_date):
    """Parse the Twitter date format to datetime object directly."""
    try:
        return datetime.strptime(twitter_date, '%a %b %d %H:%M:%S %z %Y')
    except ValueError as e:
        print(f"Date conversion error: {e}")
        return None
def create_tweet_document(tweet_data):
    user_details = tweet_data['user']
    is_retweet = tweet_data['text'].startswith('RT')
    created_at = parse_twitter_date(tweet_data['created_at'])
    tweet_document = {
        "tweet_id": tweet_data['id_str'],
        "user_id": user_details['id_str'],
        "name": user_details['name'],
        "screen_name": user_details['screen_name'],
        "text": tweet_data['text'],
        "created_at": created_at,
        "is_retweet": is_retweet,  # Boolean flag for retweets
        "quote_count": tweet_data.get('quote_count', 0),
        "reply_count": tweet_data.get('reply_count', 0),
        "retweet_count": tweet_data.get('retweet_count', 0),
        "favorite_count": tweet_data.get('favorite_count', 0),
        "entities": tweet_data.get('entities', {}),
        "hashtags": [{"text": tag['text']} for tag in tweet_data.get('entities', {}).get('hashtags', [])],
        "url": user_details.get('url'),
        "user_mentions": tweet_data.get('entities', {}).get('user_mentions', [])
    }
    # If the tweet is a retweet, store additional information
    if is_retweet and 'retweeted_status' in tweet_data:
        tweet_document['original_tweet_id'] = tweet_data['retweeted_status']['id']
    return tweet_document

def insert_tweet(tweet_data):
    tweet_document = create_tweet_document(tweet_data)
    try:
        tweets_collection.insert_one(tweet_document)
        # Handle hashtag indexing
        for hashtag in tweet_document["hashtags"]:
            hashtags_collection.update_one(
                {"text": hashtag["text"]},
                {"$addToSet": {"tweet_ids": tweet_data['id']}},
                upsert=True
            )
        # If it's a retweet, consider inserting the original tweet
        if tweet_document['is_retweet'] and 'retweeted_status' in tweet_data:
            original_tweet_data = tweet_data['retweeted_status']
            insert_tweet(original_tweet_data)

    except pymongo.errors.DuplicateKeyError:
        print(f"Tweet with id {tweet_document['tweet_id']} already exists.")

if __name__ == '__main__':
    # Read the tweets and insert the tweet into the MongoDB database
    with open(data_path, 'r') as file:
        line_number = 0
        for line in file:
            line_number += 1
            if line.strip():  # Skip empty lines
                try:
                    tweet = json.loads(line)
                    insert_tweet(tweet)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")
                    print(line)


