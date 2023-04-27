import os
import tweepy
import logging
import pprint
import json
import time
import boto3
import datetime as dt
import pandas as pd
import numpy as np
import botocore.session
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
print('imported modules successfully.')

""" get_followers()
    -params: user (string)

    -returns: following (list)
"""

def get_followers(user, api):
    following = []
    friends_response = api.get_friends(screen_name = user, count = 100, cursor = -1)
    following.extend([t.screen_name for t in friends_response[0]])
    next_cursor = friends_response[1][1]
    print("got most recent 100 following")

    while next_cursor != 0:
        try:
            friends_response = api.get_friends(screen_name = user, count = 100, cursor = next_cursor)
            following.extend([t.screen_name for t in friends_response[0]])
            next_cursor = friends_response[1][1]
            print("got next 100 following")
        
        except:
            time.sleep(60*15)

    return(following)

""" batch_put_response()
    -params: response (json),
             table (string)

    -returns: None
"""

def batch_put_response(response, table):
    with table.batch_writer(overwrite_by_pkeys=["user", "created_at"]) as batch:
        for r in response:
            try:
                batch.put_item(Item = {
                    "user": r.user.screen_name,
                    "created_at": r.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "tweet": r.text,
                    "id": r.id,
                    "retweets": r.retweet_count,
                    "favorites": r.favorite_count,
                    "inserted_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                })
        
            except ClientError as err:
                print("Couldn't load data into table {}. Here's why: {}: {}".format(table.name, err.response['Error']['Code'], err.response['Error']['Message']))
                raise

""" get_tweets()
    -params: user (string),
             table (string)
"""

def get_tweets(user, table, api):
    print("scraping tweets for user: {}".format(user))
    timeline_response = api.user_timeline(screen_name = user, count = 25)
    n = len(timeline_response)
    last_id = timeline_response[n-1]._json["id"]
    i = 0

    print("putting first batch")
    batch_put_response(timeline_response, table)
    print("inserted first batch to dynamodb successfully.")
    i += 1

    while i < 25:
        try:
            timeline_response = api.user_timeline(screen_name = user, count = 25, max_id = last_id)
            n = len(timeline_response)
            last_id = timeline_response[n-1]._json["id"]

            batch_put_response(timeline_response, table)
            i += 1

            if (i > 0 and (i % 5) == 0):
                print("inserted batch {} to dynamodb successfully.".format(i+1))
    
        except Exception as e:
            print(e)
            if "429" in str(e):
                print("sleeping for 5 minutes")
                time.sleep(60*5)

            else:
                print("sleeping for a minute")
                time.sleep(60)

#concatenate into master df, write to file
def main():
    print("starting now...")
    start = dt.datetime.now()
    base = os.getcwd()
    data_dir = os.path.join(base, 'data')
    log_dir = os.path.join(base, 'logs')
    pp = pprint.PrettyPrinter(indent = 1)

    os.chdir(base)

    with open('./../creds.json', 'r') as f:
        tweepy_creds = json.load(f)
    f.close()
        
    auth = tweepy.OAuthHandler(tweepy_creds['twitter-api-key'], tweepy_creds['twitter-secret-key'])
    auth.set_access_token(tweepy_creds['twitter-access-token'], tweepy_creds['twitter-secret-access'])

    api = tweepy.API(auth)
    logging.debug('authorized API w/ client id and secret.')

    client = boto3.client('dynamodb',
    endpoint_url = "http://{}:{}".format(tweepy_creds["host"], tweepy_creds["port"]),
    aws_access_key_id=tweepy_creds["aws-access-key"],
    aws_secret_access_key=tweepy_creds["aws-secret-key"],
    region_name='us-east-2')

    dynamodb = boto3.resource('dynamodb', \
                endpoint_url = "http://{}:{}".format(tweepy_creds["host"], tweepy_creds["port"]), \
                region_name='us-east-2')

    session = botocore.session.get_session()
    dynamodb_session = session.create_client('dynamodb', \
                        region_name='us-east-2',
                        endpoint_url = "http://{}:{}".format(tweepy_creds["host"], tweepy_creds["port"]),
                        aws_access_key_id=tweepy_creds["aws-access-key"],
                        aws_secret_access_key=tweepy_creds["aws-secret-key"]) # low-level client

    tweets = dynamodb.Table("tweets")

    print("created dynamo client.")

    my_following = get_followers("xanax_princess_", api)

    #set users to scrape tweets from
    # users = ['xanax_princess_', 'slpyboy', 'AristocratAlex', 'timpc9213', 'cd3lisi', 'ChriMoulto', 'uhnonimouse', 'Bean_glitch']

    print("following: {}.".format(my_following))

    tweets = dynamodb.Table("tweets")

    for f in my_following:
        try:
            output = get_tweets(f, tweets, api)
            time.sleep(5)

        except KeyboardInterrupt as k:
            print(k)

    response = tweets.scan()
    # response

if __name__ == "__main__":
    main()