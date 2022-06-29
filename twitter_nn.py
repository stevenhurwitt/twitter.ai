import re
import os
import logging
import logging.handlers
import pprint
import math
import json
import keras
import pickle
import datetime
import pandas as pd 
import numpy as np 
import google.auth
import tensorflow as tf
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from twitter_bq_upload import bq_read_table

base = os.getcwd()
os.chdir(base)
data_dir = os.path.join(base, 'data')
log_dir = os.path.join(base, 'logs')

pp = pprint.PrettyPrinter(indent = 1)

#handle log files
def log_init():
    should_roll_over = os.path.isfile('twitter_train.log')
    handler = logging.handlers.RotatingFileHandler('twitter_train.log', mode='w', backupCount=1)
    if should_roll_over: 
        handler.doRollover()
    logging.basicConfig(filename = 'twitter_train.log', level = logging.DEBUG)

#init bq client
creds_fname = "creds_google.json"
client = bigquery.Client.from_service_account_json(creds_fname)
bqstorageclient = bigquery_storage.BigQueryStorageClient.from_service_account_json(creds_fname)
logging.info('initialized bigquery client.')

tf_dev = 'using tf with dev: {}.'.format(tf.config.list_physical_devices('GPU'))
logging.info(tf_dev)
print(tf_dev)
print('imported modules successfully.')

#remove garbage from tweets
def clean_tweets(tweet_list):
    clean_tweet_list = []
    bad_chars = ['\n', '\\', 'amp;', 'RT ']
    for t in tweet_list:
        for b in bad_chars:
            t = t.replace(b, '')
        t = re.sub(r"http\S+", '', t)
        clean_tweet_list.append(t)
    output = 'cleaned {} tweets.'.format(len(clean_tweet_list))
    print(output)
    logging.info(output)
    return(clean_tweet_list)

#turn tweets into sequences
def tweet_sequences(tokens, size):
    length = size + 1
    sequences = list()
    for i in range(length, len(tokens)):
        seq = tokens[i-length:i]
        line = ' '.join(seq)
        sequences.append(line)

    return(sequences)

def tokenize_xy(sequence):
    tokenizer = keras.preprocessing.text.Tokenizer()
    tokenizer.fit_on_texts(sequence)
    sequences = tokenizer.texts_to_sequences(sequence)
    padded = keras.preprocessing.sequence.pad_sequences(sequences)
    vocab_size = len(tokenizer.word_index) + 1

    pickle.dump(tokenizer, open('tokenizer.pkl', 'wb'))
    print('saved tokenizer.')
    logging.info('saved tokenizer.')
 
    X, y = padded[:,:-1], padded[:,-1]
    y = keras.utils.to_categorical(y, num_classes=vocab_size)
    seq_length = X.shape[1]
    print('X has shape: {}'.format(X.shape))
    print('y has shape: {}'.format(y.shape))
    logging.info('X has shape: {}'.format(X.shape))
    logging.info('y has shape: {}'.format(y.shape))
    return(X, y, seq_length, vocab_size)

def fit_model(X, y, seq_length, vocab_size, epoch):
    model = keras.models.Sequential()
    model.add(keras.layers.Embedding(vocab_size, X.shape[1], input_length=seq_length))
    model.add(keras.layers.LSTM(100, return_sequences=True))
    model.add(keras.layers.LSTM(100))
    model.add(keras.layers.Dense(100, activation='relu'))
    model.add(keras.layers.Dense(vocab_size, activation='softmax'))
    print(model.summary())
    logging.info(model.summary())

    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

    model.fit(X, y, batch_size=128, epochs=epoch)
 

    model.save('model_500.h5')

    print('saved model.')
    logging.info('saved model.')

def main():
    #init log file
    os.chdir(log_dir)
    log_init()
    os.chdir(base)

    try:
        #read data from bq table
        data = bq_read_table()
        tweets = list(data.text)
        np.random.shuffle(tweets)
    except:
        print('data read error.')
        logging.error('data read error.')

    try:
        #clean tweets
        clean_tweet = clean_tweets(tweets)

        #get avg tweet length, corpus of words/tweets
        avg_tweet_len = math.ceil(np.mean([len(c.split(' ')) for c in clean_tweet]))
        corp = " ".join(clean_tweet)
        words = corp.split(' ')

        #turn tweets into sequences
        tweet_seq = tweet_sequences(words, avg_tweet_len)
        output = 'turned {} tweets into {} sequences.'.format(len(clean_tweet), len(tweet_seq))
        print(output)
        logging.info(output)
    except:
        print('error sequencing text.')
        logging.error('error sequencing text.')

    try:
        #tokenize text into X and y
        X, y, s, v = tokenize_xy(tweet_seq)
    except:
        print('error tokenizing text.')
        logging.error('error tokenizing text.')

    try:
        #fit RNN
        fit_model(X, y, s, v, 500)
    except:
        print('error fitting RNN.')
        logging.error('error fitting RNN.')

if __name__ == "__main__":
    main()

