import os
import logging
import logging.handlers
import pprint
import json
import keras
import datetime
import pandas as pd 
import numpy as np 
import google.auth
import tensorflow as tf
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from twitter_bq_upload import bq_read_table

pp = pprint.PrettyPrinter(indent = 1)

base = '/media/steven/big_boi/twitter.ai'
data_dir = os.path.join(base, 'data')
log_dir = os.path.join(base, 'logs')

#handle log files
os.chdir(log_dir)
should_roll_over = os.path.isfile('twitter_train.log')
handler = logging.handlers.RotatingFileHandler('twitter_train.log', mode='w', backupCount=1)
if should_roll_over: 
    handler.doRollover()
logging.basicConfig(filename = 'twitter_train.log', level = logging.DEBUG)
os.chdir(base)

#init bq client
creds_fname = '/media/steven/big_boi/creds_google.json'
client = bigquery.Client.from_service_account_json(creds_fname)
bqstorageclient = bigquery_storage.BigQueryStorageClient.from_service_account_json(creds_fname)
logging.info('initialized bigquery client.')

tf_dev = 'using tf with dev: {}.'.format(tf.config.list_physical_devices('GPU'))
logging.info(tf_dev)
print(tf_dev)
print('imported modules successfully.')

