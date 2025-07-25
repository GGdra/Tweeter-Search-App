from bson import ObjectId
from flask import Flask, request, jsonify
import json
from search_service import *
import datetime
import pytz

app = Flask(__name__)

@app.route('/search', methods=['POST'])
def search():
    """
    Search tweets based on query parameters.
    Expected JSON payload: {
        "query_string": "some text",
        "hashtag": "example",
        "user": "username",
        "start_time": "YYYY-MM-DD HH:MM:SS",
        "end_time": "YYYY-MM-DD HH:MM:SS"
    }
    """
    data = request.json
    time_range = None

    if 'start_time' in data and 'end_time' in data:
        start_time = datetime.datetime.fromisoformat(data['start_time'].replace(' ', 'T')).astimezone(pytz.utc)
        end_time = datetime.datetime.fromisoformat(data['end_time'].replace(' ', 'T')).astimezone(pytz.utc)
        time_range = (start_time, end_time)

    query_params = {
        'query_string': data.get('query_string'),
        'hashtag': data.get('hashtag'),
        'user': data.get('user'),
        'time_range': time_range
    }

    results = search_and_rank_tweets(query_params)
    return jsonify(results)


@app.route('/top-metrics', methods=['GET'])
def top_metrics():
    """
    Retrieve top-level metrics.
    """
    metrics = get_cached_top_metrics()
    # print(metrics)
    return jsonify(metrics)


@app.route('/tweet/<tweet_id>', methods=['GET'])
def tweet_details(tweet_id):
    """
    Fetch details of a tweet by its ID.
    """
    details = tweet_metadata(tweet_id)
    return jsonify(details)


if __name__ == '__main__':

    app.run(debug=True, port=5000)


