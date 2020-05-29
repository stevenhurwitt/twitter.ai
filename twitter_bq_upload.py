import os
import logging
import logging.handlers
import logging.config
import pprint
import json
import datetime
import pandas as pd 
import numpy as np 
import google.auth
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage

pp = pprint.PrettyPrinter(indent = 1)

base = os.getcwd()
data_dir = os.path.join(base, 'data')
log_dir = os.path.join(base, 'logs')

#handle log files
def log_init():
    should_roll_over = os.path.isfile('twitter_bq_upload.log')
    handler = logging.handlers.RotatingFileHandler('twitter_bq_upload.log', mode='w', backupCount=1)
    if should_roll_over: 
        handler.doRollover()
    logging.basicConfig(filename = 'twitter_bq_upload.log', level = logging.DEBUG)
    logging.config.dictConfig({'version' : 1, 'disable_existing_loggers' : True})

#init bq client
creds_fname = '/media/steven/big_boi/creds_google.json'
client = bigquery.Client.from_service_account_json(creds_fname)
bqstorageclient = bigquery_storage.BigQueryStorageClient.from_service_account_json(creds_fname)
logging.info('initialized bigquery client.')
print('imported modules successfully.')

### read data currently in bq table
def bq_read_table():

    #read current bq table
    QUERY = "SELECT * FROM `symbolic-bit-277217.basement_dude_tweets.tweets_master`;"
    query_job = client.query(QUERY)
    results = client.query(QUERY).result()
    bq_output = results.to_dataframe()
    return(bq_output)

### function to subset data from master (A) not in bq table (B)
def subset_df(A, B):
    to_upload = pd.concat([A.drop(columns = 'date'), B.drop(columns = 'date')]).drop_duplicates(keep = False)
    print('master len: {}, bq table len: {}, subset df len: {}'.format(A.shape[0], B.shape[0], to_upload.shape[0]))
    print('if master len > bq table len, data size has decreased and there is none to upload.')
    to_upload = pd.concat([to_upload, A.date], axis = 1, join = 'inner')
    to_upload = to_upload[['text', 'user', 'date', 'fav_count', 'retweet_count', 'retweet']]
    to_upload.reset_index(drop = True, inplace = True)
    to_upload.to_csv('master_data_upload.csv', index = False)

    output = 'adding {} records to bq db.'.format(to_upload.shape[0])
    print(output)
    logging.info(output)
    return(to_upload)

### upload new data to bq table
def bq_upload():
    #set table ref
    table_ref = client.dataset('basement_dude_tweets').table('tweets_master')

    #create job_config
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1

    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.auto_detect = True
    job_config.allow_jagged_rows = True
    job_config.allow_quoted_newlines = True

    #upload local file to bq
    filename = os.path.join(data_dir, 'master_data_upload.csv')
    with open(filename, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    print("Starting job {}".format(load_job.job_id))

    try:
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        output = "Loaded {} rows.".format(destination_table.num_rows)
        print(output)
        logging.info(output)
    
    except:
        print('Job failed:')
        for j in load_job.errors:
            logging.error(j)

def bq_dedupe():
    QUERY = """CREATE OR REPLACE TABLE `symbolic-bit-277217.basement_dude_tweets.tweets_master`
            AS SELECT DISTINCT * FROM `symbolic-bit-277217.basement_dude_tweets.tweets_master`;"""
    query_job = client.query(QUERY)
    results = client.query(QUERY).result()
    bq_output = results.to_dataframe()

    bq_output = bq_read_table()
    print('deduplicated bq table to {} results.'.format(bq_output.shape[0]))

def main():
    try:
        #read master data .csv
        master = pd.read_csv('master_data_clean.csv')
    except:
        print('file import error.')

    try:
        #read bq table
        bq_output = bq_read_table()
    except:
        print('bigquery read error.')

    try:
        #subset data to add
        to_upload = subset_df(master, bq_output)
    except:
        print('data subset error.')
        logging.warning('no new data to upload.')

    try:
        #upload new data to bq table
        bq_upload()
    except:
        print('bigquery upload error.')
        logging.warning('bq upload failed.')
    
    try:
        #deduplicate bq table
        bq_dedupe()
    except:
        print('deduplication error.')

if __name__ == "__main__":
    os.chdir(log_dir)
    log_init()
    os.chdir(data_dir)
    main()