import os
import sys
import json
import time
import boto3
import tweepy
import pprint
import datetime
import numpy as np
import pandas as pd
import botocore.session
from dynamodb_json import json_util
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sc.setLogLevel('INFO')
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_secret():

    secret_name = "twitter_creds.json"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    # Your code goes here.
    return(secret)


def scan_dynamo(table, creds):

    client = boto3.client('dynamodb',
    # endpoint_url = "http://localhost:8000",
    aws_access_key_id=creds["aws-access-key"],
    aws_secret_access_key=creds["aws-secret-access-key"],
    region_name='us-east-2')

    dynamodb = boto3.resource('dynamodb', \
                # endpoint_url = "http://localhost:8000", \
                region_name='us-east-2')

    tweets = dynamodb.Table(table)
    # print(tweets.item_count)

    done = False
    start_key = None
    scan_kwargs = {}
    table_list = []

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = tweets.scan(**scan_kwargs)
        table_list.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
        logger.info("read data for key {}".format(start_key))
        time.sleep(5)
    
    return(table_list)

creds = get_secret()

dynamo_list = scan_dynamo("tweets", creds)

tweetSchema = StructType([       
    StructField('favorites', IntegerType(), True),
    StructField('created_at', TimestampType(), True),
    StructField('id', LongType(), True),
    StructField('tweet', StringType(), True),
    StructField('retweets', IntegerType(), True),
    StructField('inserted_at', TimestampType(), True),
    StructField('user', StringType(), True)
])

df = spark.createDataFrame(data=dynamo_list, schema = tweetSchema)
logger.info(df.printSchema())
logger.info(df.show())

filepath = "s3a://dynamo-tweets/clean/"
df.write.format("parquet").partitionBy("user").mode("overwrite").option("overwriteSchema", "true").option("header", True).save(filepath)
logger.info("wrote df to {}".format(filepath))

job.commit()