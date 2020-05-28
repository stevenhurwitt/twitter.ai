import os
import tweepy
import logging
import logging.handlers
import pprint
import json
import datetime
import pandas as pd
import numpy as np

#generate log file, init tweepy api, init pprinter
base = os.getcwd()
data_dir = os.path.join(base, 'data')
print('working in directory: %s' % base)

pp = pprint.PrettyPrinter(indent = 1)

should_roll_over = os.path.isfile('tweet_grab.log')
handler = logging.handlers.RotatingFileHandler('tweet_grab.log', mode='w', backupCount=1)
if should_roll_over: 
    handler.doRollover()
logging.basicConfig(filename = 'tweet_grab.log', level = logging.DEBUG)

with open('/media/steven/big_boi/creds_tweepy.json', 'r') as f:
    tweepy_creds = json.load(f)
f.close()
    
auth = tweepy.OAuthHandler(tweepy_creds['consumer_key'], tweepy_creds['consumer_secret'])
auth.set_access_token(tweepy_creds['access_key'], tweepy_creds['access_secret'])

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
    master = []

    for u in users:
        os.chdir(data_dir)
        tweets_df = get_tweets_per_user(u, 100)
        master.append(tweets_df)
        os.chdir(base)

    os.chdir(data_dir)
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
    os.chdir(base)

if __name__ == "__main__":
    main()