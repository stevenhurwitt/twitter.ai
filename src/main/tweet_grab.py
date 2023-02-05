import os
import tweepy
import logging
import logging.handlers
import pprint
import json
import time
import datetime as dt
import pandas as pd
import numpy as np
import boto3
import botocore.session
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
print("imported modules.")

filepath = "/home/steven/Documents/twitter.ai/creds.json"

with open(filepath, "r") as f:
    creds = json.load(f)
    f.close()
    print("read creds.json")

def get_followers(user):


    with open(filepath, "r") as f:
        creds = json.load(f)
        f.close()
        print("read creds.json")

    following = []
    auth = tweepy.OAuthHandler(creds['twitter-api-key'], creds['twitter-secret-key'])
    auth.set_access_token(creds['twitter-access-token'], creds['twitter-secret-access'])

    api = tweepy.API(auth)
    friends_response = api.get_followers(screen_name = user, count = 100, cursor = -1)
    following.extend([t.screen_name for t in friends_response[0]])
    next_cursor = friends_response[1][1]
    print("got most recent 100 following")

    while next_cursor != 0:
        try:
            friends_response = api.get_followers(screen_name = user, count = 100, cursor = next_cursor)
            following.extend([t.screen_name for t in friends_response[0]])
            next_cursor = friends_response[1][1]
            print("got next 100 following")
        
        except:
            time.sleep(60*15)

    return(following)

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

def get_tweets(user, table):
    print("scraping tweets for user: {}".format(user))
    auth = tweepy.OAuthHandler(creds['twitter-api-key'], creds['twitter-secret-key'])
    auth.set_access_token(creds['twitter-access-token'], creds['twitter-secret-access'])

    api = tweepy.API(auth)
    timeline_response = api.user_timeline(screen_name = user, count = 25)
    n = int(len(timeline_response)) - 1
    try:
        last_id = timeline_response[n]._json["id"]
    
    except Exception as e:
        print(e)
        last_id = timeline_response[20]._json["id"]

    i = 0

    print("putting first batch")
    batch_put_response(timeline_response, table)
    print("inserted first batch to dynamodb successfully.")
    i += 1

    while i < 25:
        try:
            timeline_response = api.user_timeline(screen_name = user, count = 25, max_id = last_id)
            n = int(len(timeline_response)) - 1
            try:
                last_id = timeline_response[n]._json["id"]

            except Exception as e:
                print(e)
                # last_id = timeline_response[20]._json["id"]

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

#scrape pages of tweets per users
#save metadata: tweet, user, retweets, favorites, is_retweet, date
def get_tweets_per_user(user, pages):
    
    i = 0
    tweet_info = []
    for p in range(0,pages):
        try:
            auth = tweepy.OAuthHandler(creds['twitter-api-key'], creds['twitter-secret-key'])
            auth.set_access_token(creds['twitter-access-token'], creds['twitter-secret-access'])

            api = tweepy.API(auth)
            tweets = api.user_timeline(screen_name = user, page = p)
            for t in tweets:
                try:
                    tweet_dict = dict([('text', t.text), ('user', user), ('date', t.created_at), ('fav_count', t.favorite_count), ('retweet_count', t.retweet_count), ('retweet', t.retweeted)])
                    tweet_info.append((i, tweet_dict))
                    i += 1
                except:
                    print('error with tweet {}.'.format(i))
        except:
            output = 'error with page {}'.format(p)
            print(output)
            logging.error(output)

    tweet_info = dict(tweet_info)
    tweet_info_json = json.dumps(tweet_info, indent = 1, sort_keys = False, default = str)
    json_fname = '%s_tweet_dump.json' % user

    with open(json_fname, 'w') as f:
        json.dump(tweet_info_json, f)
    
    
    tweets_df = pd.read_json(tweet_info_json, orient = 'index')
    tweets_df.sort_index(axis = 0, inplace = True)
    csv_fname = '%s_tweet_dump.csv' % user
    tweets_df.to_csv(csv_fname, index = False)
    
    log_out = 'saved %d tweets for user %s.' % (i, user)
    print(log_out)
    logging.info(log_out)
    
    return(tweets_df)

#concatenate into master df, write to file
def main():
    try:
        try:
            client = boto3.client('dynamodb',
                endpoint_url = "http://{}:{}".format(creds["host"], creds["port"]),
                aws_access_key_id=creds["aws-access-key"],
                aws_secret_access_key=creds["aws-secret-access-key"],
                region_name='us-east-2')

            dynamodb = boto3.resource('dynamodb', \
                        endpoint_url = "http://{}:{}".format(creds["host"], creds["port"]), \
                        region_name='us-east-2')

            session = botocore.session.get_session()
            dynamodb_session = session.create_client('dynamodb', \
                                region_name='us-east-2',
                                endpoint_url = "http://{}:{}".format(creds["host"], creds["port"]),
                                aws_access_key_id=creds["aws-access-key"],
                                aws_secret_access_key=creds["aws-secret-access-key"]) # low-level client

            tweets = dynamodb.Table("tweets")

            print("created dynamo client.")

        except Exception as e:
            print(e)

        my_following = get_followers("xanax_princess_")
        print(len(my_following))

    except Exception as e:
        print("Exception: {}".format(e))

    results = []
    my_following = get_followers("xanax_princess_")
    for f in my_following:
        try:
            output = get_tweets(f, tweets)
            results.append(output)
            time.sleep(5)

        except KeyboardInterrupt as k:
            print(k)

    df = pd.DataFrame(results)
    df.head()
    df.shape

if __name__ == "__main__":
    main()