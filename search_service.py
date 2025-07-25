import time
from pymongo import MongoClient
from cache import LRUCacheWithTTL
from mysql_database import create_server_connection
from datetime import datetime
from collections import Counter
import threading
import pandas as pd

# MongoDB connection setup
mongo_client = MongoClient('...')
mongo_db = mongo_client['TwitterData']
tweets_collection = mongo_db['tweets']

# MySQL connection setup
db_connection = create_server_connection(
    "localhost", "root", "...", "TwitterData")
mysql_cursor = db_connection.cursor()

lru_cache = LRUCacheWithTTL(capacity=100, ttl=3600)


def search_tweets(query_string=None, hashtag=None, user=None, time_range=None):
    query_filter = {}

    # Add string search in text to query filter
    if query_string:
        query_filter['text'] = {'$regex': query_string,
                                '$options': 'i'}  # Case-insensitive search

    # Add hashtag search to query filter
    if hashtag:
        query_filter['entities.hashtags.text'] = hashtag

    # Add user search to query filter
    if user:
        # Retrieve user_id from MySQL using screen_name
        mysql_cursor.execute(
            "SELECT user_id FROM users WHERE screen_name = %s", (user,))
        result = mysql_cursor.fetchone()
        if result:
            query_filter['user_id'] = result['user_id']

    # Add time range to query filter
    if time_range:
        start_date, end_date = time_range
        query_filter['created_at'] = {'$gte': start_date, '$lte': end_date}

    # Execute search query
    search_results = tweets_collection.find(query_filter).limit(50)

    # Rank and return search results
    return rank_search_results(search_results)


def rank_search_results(search_results):
    # ranking based on retweet_count
    ranked_results = sorted(search_results, key=lambda x: -x['retweet_count'])

    for tweet in ranked_results:
        del tweet['_id']

    # Group by categories and select top 10 for each category
    top_by_category = {
        'top_retweeted': ranked_results[:10],
        'top_favorited': sorted(ranked_results, key=lambda x: -x['favorite_count'])[:10],
    }

    # Convert MongoDB results to list of dicts
    ranked_results_list = list(map(dict, ranked_results))

    # Return ranked results grouped by category
    return top_by_category, ranked_results_list


def tweet_metadata(tweet_id, cache=True):
    if cache:
        # check if the metadata is available in the cache
        cached_data = lru_cache.get(tweet_id)
        if cached_data:
            return cached_data

    # Fetch tweet information from MongoDB
    tweet = tweets_collection.find_one({'tweet_id': tweet_id})
    if tweet:
        # Retrieve additional user data from MySQL
        mysql_cursor.execute(
            "SELECT * FROM users WHERE user_id = %s", (tweet['user_id'],))
        user_data = mysql_cursor.fetchone()
        if user_data:
            # Prepare metadata to show
            metadata = {
                'author': user_data['name'],
                'tweeted_at': tweet['created_at'],
                'retweet_count': tweet.get('retweet_count', 0),
                # Example additional field
                'favorite_count': tweet.get('favorite_count', 0),
                # Potentially add more fields as needed
            }

            if cache:
                # Cache the retrieved metadata
                # Using put if you are using a custom LRUCacheWithTTL
                lru_cache.put(tweet_id, metadata)
                return metadata

    return None


def user_tweets(user_id):
    # Fetch tweets for the given user
    return list(tweets_collection.find({'user_id': user_id}))


def calculate_top_metrics(cursor):
    try:
        # Fetch top 10 users by followers count
        cursor.execute(
            "SELECT user_id, screen_name, followers_count FROM users ORDER BY followers_count DESC LIMIT 10")
        top_users = [{"user_id": row['user_id'], "screen_name": row['screen_name'],
                      "followers_count": row['followers_count']} for row in cursor.fetchall()]

        # Ensure index on 'retweet_count' in MongoDB for performance
        tweets_collection.create_index([('retweet_count', -1)])

        # Fetch top 10 tweets by retweet count
        top_tweets = list(tweets_collection.find().sort(
            'retweet_count', -1).limit(10))

        # Processing MongoDB results to be JSON serializable and more informative
        processed_tweets = [
            {
                # Ensure the ID is serializable
                'tweet_id': str(tweet['tweet_id']),
                'text': tweet.get('text', ''),
                'retweet_count': tweet.get('retweet_count', 0)
            }
            for tweet in top_tweets
        ]

        return {'top_users': top_users, 'top_tweets': processed_tweets}
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle the error appropriately or re-raise it
        return {'top_users': [], 'top_tweets': []}


# Define a helper function for TTL cache
def get_cached_top_metrics():
    top_metrics_key = 'top_metrics'
    top_metrics = lru_cache.get(top_metrics_key)
    if not top_metrics:
        top_metrics = calculate_top_metrics(mysql_cursor)
        lru_cache.put(top_metrics_key, top_metrics)
    return top_metrics

# Function to search tweets with ranking and drill-down features


def search_and_rank_tweets(query_params, cache=True):
    # Create a unique key based on the query parameters to cache search results
    query_key = frozenset(query_params.items())

    if cache:
        # Try to get cached results
        cached_results = lru_cache.get(query_key)
        if cached_results:
            return cached_results

    # If no cached results, perform the search
    query_string = query_params.get('query_string')
    hashtag = query_params.get('hashtag')
    user = query_params.get('user')
    time_range = query_params.get('time_range')

    top_by_category, ranked_results_list = search_tweets(
        query_string=query_string,
        hashtag=hashtag,
        user=user,
        time_range=time_range
    )

    # Enhance tweet list with metadata from cache or database
    for tweet in ranked_results_list:
        tweet_id = tweet.get('tweet_id')
        tweet['metadata'] = tweet_metadata(tweet_id)
        # del tweet['_id']

    # Package the results
    results = {
        'results': ranked_results_list,
        'top_by_category': top_by_category
    }

    if cache:
        # Cache the new results before returning
        lru_cache.put(query_key, results)

    return results


def periodic_cache_update(interval):  # 定时启动cache
    # Establish its own database connection
    local_db_connection = create_server_connection(
        "localhost", "root", "Qyf19980504", "TwitterData")
    local_mysql_cursor = local_db_connection.cursor()

    try:
        while True:
            print("Updating cache with top metrics...")
            top_metrics = calculate_top_metrics(
                local_mysql_cursor)  # Pass the local cursor
            lru_cache.put('top_metrics', top_metrics)
            time.sleep(interval)
    finally:
        local_mysql_cursor.close()
        local_db_connection.close()


# Run periodic cache update as a background thread
update_interval = 3600  # Update every hour
update_thread = threading.Thread(
    target=periodic_cache_update, args=(update_interval,))
update_thread.daemon = True  # Daemonize thread
update_thread.start()

if __name__ == '__main__':

    search_params = {
        'query_string': 'trump',
        # 'hashtag': 'Herdenimmunitaet',
        # 'user': 'chase_tim',
        # 'time_range': (datetime(2020, 1, 1), datetime(2020, 12, 31))
    }

    results = search_and_rank_tweets(search_params)
    print(results)

    # top_metrics_results = calculate_top_metrics(mysql_cursor)
    # print(top_metrics_results)
    #
    # tweet_id = 1249403767180668930
    # print(tweet_metadata(tweet_id))
