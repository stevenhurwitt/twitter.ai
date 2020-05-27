import os
import tweepy
import logging
import pprint
import json
import datetime
import pandas as pd
import numpy as np

#generate log file, init tweepy api, init pprinter
print('working in directory: %s' % os.getcwd())
pp = pprint.PrettyPrinter(indent = 1)
logging.basicConfig(filename = 'twitter.log', level = logging.DEBUG)
auth = tweepy.OAuthHandler('vDmSiSjkot8pbnf4Z7eUmNCaW', 'dBcYnqMwWaTDx76weHrybngCsfszZQ4TinxsCjZrgo4hR2nlWH')
auth.set_access_token('258542490-FD117iVPwblz8uGppXC1M8blIOzZLPEMTw7ATIv8', 'hCbeVJA9Rlif9rYGHlFMUEbJl5nzKlZ5GjVbxQ5QEQdGX')

api = tweepy.API(auth)
logging.info('authorized API w/ client id and secret.')

#set users to scrape tweets from
users = ['xanax_princess_', 'slpyboy', 'AristocratAlex', 'timpc9213', 'cd3lisi', 'ChriMoulto', 'uhnonimouse', 'Bean_glitch']

#scrape pages of tweets per users
#save metadata: tweet, user, retweets, favorites, is_retweet, date
def get_tweets_per_user(user, pages):
    
    i = 0
    tweet_info = []
    for p in range(0,pages):
        try:
            tweets = api.user_timeline(screen_name = user, page = p)
            for t in tweets:
                tweet_dict = dict([('text', t.text), ('user', user), ('date', t.created_at), ('fav_count', t.favorite_count), ('retweet_count', t.retweet_count), ('retweet', t.retweeted)])
                tweet_info.append((i, tweet_dict))
                i += 1
        except:
            print('error with page {}'.format(p))
    tweet_info = dict(tweet_info)
    tweet_info_json = json.dumps(tweet_info, indent = 1, sort_keys = False, default = str)
    json_fname = '%s_tweet_dump.json' % user

    with open(json_fname, 'w') as f:
        json.dump(tweet_info_json, f)
    
    print('wrote %s tweets as %s' % (user, json_fname))
    
    tweets_df = pd.read_json(tweet_info_json, orient = 'index')
    tweets_df.sort_index(axis = 0, inplace = True)
    csv_fname = '%s_tweet_dump.csv' % user
    tweets_df.to_csv(csv_fname, index = False)
    
    log_out = 'saved %d tweets for user %s.' % (i, user)
    print(log_out)
    logging.info(log_out)
    
    return(tweets_df)

#concatenate into master df, write to file
master = []

for u in users:
    tweets_df = get_tweets_per_user(u, 100)
    master.append(tweets_df)

master_df = pd.concat(master)
master_fname = 'master_tweet_dump.csv'
master_df.to_csv(master_fname, index = False)
logging.info('wrote master .csv file.')
print('wrote master .csv file.')

#cleanse master data file (exclude retweets)
not_retweet = [r == False for r in master_df.retweet]
master_df = master_df[not_retweet]
master_df.reset_index(drop = True)
master_df.to_csv('master_data_clean.csv', index = False)
print('wrote cleaned master file to .csv.')