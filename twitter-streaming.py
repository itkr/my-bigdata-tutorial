# -*- encoding: utf-8 -*-

import datetime
import json
import pymongo
import requests_oauthlib
import tqdm


def main():
    r = get_r()
    insert_to_mongo(r)

def get_r():
    twitter = requests_oauthlib.OAuth1Session(
        consumer_key, consumer_secret, access_token_key, access_token_secret)
    uri = 'https://stream.twitter.com/1.1/statuses/sample.json'
    r = twitter.get(uri, stream=True)
    r.raise_for_status()
    return r

def insert_to_mongo(r):
    mongo = pymongo.MongoClient()
    for line in tqdm.tqdm(r.iter_lines(), unit='tweets', mininterval=1):
        if line:
            tweet = json.loads(line)
            tweet['_timestamp'] = datetime.datetime.utcnow().isoformat()
            mongo.twitter.sample.insert_one(tweet)

if __name__ == '__main__':
    main()

