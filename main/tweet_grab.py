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
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from dynamodb_json import json_util
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

pp = pprint.PrettyPrinter(indent = 2)
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

    print("got {} following.".format(len(following)))

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
    if n > 0:
        try:
            last_id = timeline_response[n-1]._json["id"]
            i = 0

            print("putting first batch")
            batch_put_response(timeline_response, table)
            print("{}: inserted first batch to dynamodb successfully.".format(user))
            i += 1

            while i < 25:
                try:
                    timeline_response = api.user_timeline(screen_name = user, count = 25, max_id = last_id)
                    n = len(timeline_response)
                    last_id = timeline_response[n-1]._json["id"]

                    batch_put_response(timeline_response, table)
                    i += 1

                    if (i > 0 and (i % 5) == 0):
                        print("{}: inserted batch {} to dynamodb successfully.".format(user, i+1))
            
                except Exception as e:
                    print(e)
                    if "429" in str(e):
                        print("sleeping for 5 minutes")
                        time.sleep(60*5)

                    else:
                        print("sleeping for a minute")
                        time.sleep(60)

        except Exception as e:
            print(e)
    
    else:
        print("no response data for {}".format(user))
        pass

#concatenate into master df, write to file
def main():
    print("starting now...")
    start = dt.datetime.now()
    base = os.getcwd()
    data_dir = os.path.join(base, 'data')
    log_dir = os.path.join(base, 'logs')
    pp = pprint.PrettyPrinter(indent = 1)

    logger = logging.getLogger("tweet_grab")
    logger.setLevel(logging.INFO)

    os.chdir(base)

    with open('./../creds.json', 'r') as f:
        tweepy_creds = json.load(f)
    f.close()
        
    auth = tweepy.OAuthHandler(tweepy_creds['twitter-api-key'], tweepy_creds['twitter-secret-key'])
    auth.set_access_token(tweepy_creds['twitter-access-token'], tweepy_creds['twitter-secret-access'])

    api = tweepy.API(auth)
    logging.debug('authorized API w/ client id and secret.')

    # client = boto3.client('dynamodb',
    #     endpoint_url = "http://{}:{}".format(tweepy_creds["host"], tweepy_creds["port"]),
    #     aws_access_key_id=tweepy_creds["aws-access-key"],
    #     aws_secret_access_key=tweepy_creds["aws-secret-key"],
    #     region_name='us-east-2')

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
    print("following count: {}.".format(len(my_following)))

    initial_response = tweets.scan()
    count = initial_response["Count"]
    print("found {} existing records in dynamo table".format(count))

    # if count == 0:
        # create table...

    spark_host = tweepy_creds["spark_host"]
    spark_port = tweepy_creds["spark_port"]

    spark = SparkSession.builder \
        .appName("tweet_grab") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.cores.max", "4") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.host", spark_host) \
        .config("spark.driver.port", "4040") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.execution.arrow.fallback.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", tweepy_creds["aws-access-key"]) \
        .config["spark.hadoop.fs.s3a.secret.key", tweepy_creds["aws-secret-key"]] \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer.size", "1048576") \
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4") \
        .master(f"{spark_host}:{spark_port}") \
        .enableHiveSupport() \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    spark_df = spark.createDataFrame(pd.DataFrame(json_util.loads(initial_response["Items"])))
    spark_df.head()

    df = pd.DataFrame(json_util.loads(initial_response["Items"]))
    dynamo_users = list(np.unique(df['user']))

    subset = [a not in dynamo_users for a in my_following]
    missing_users = [x for x, y in zip(my_following, subset) if y]

    print("found {} users in dynamo, {} users left.".format(len(dynamo_users), len(missing_users)))

    for f in missing_users:
        try:
            output = get_tweets(f, tweets, api)
            time.sleep(5)

        except KeyboardInterrupt as k:
            print(k)

    response = tweets.scan()
    new_count = response["Count"]
    print("found {} existing records in dynamo table".format(new_count))

    # dynamodb scan
    tweets = dynamodb.Table("tweets")
    records = []
    year_range = {}
    year_range["first"] = 1990
    year_range["second"] = 2020
    scan_kwargs = {
        'FilterExpression': Key('year').between(year_range['first'], year_range['second']),
        'ProjectionExpression': "user, tweet, favorites, retweets, created_at, inserted_at",
        'ExpressionAttributeNames': {"user": "user"}}
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = tweets.scan(**scan_kwargs)
            records.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
    except ClientError as err:
        logger.error(
            "Couldn't scan for movies. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    pp.pprint(records)
    logger.info("Found %d records", len(records))

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.info("running main function...")
    main()
    logger.info("main function completed.")