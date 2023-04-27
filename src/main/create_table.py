import os
import json
import time
import boto3
import tweepy
import pprint
import datetime
import numpy as np
import pandas as pd
import botocore.session
from boto3.dynamodb.conditions import Key, Attr

def main():
    with open("../creds.json", "r") as f:
        creds = json.load(f)
        f.close()
        
    print("imported modules.")

    client = boto3.client('dynamodb',
    # endpoint_url = "https://{}:8000".format(creds["host"]),
    aws_access_key_id=creds["aws-access-key"],
    aws_secret_access_key=creds["aws-secret-access-key"],
    region_name='us-east-2')

    dynamodb = boto3.resource('dynamodb', \
                # endpoint_url = "https://{}:8000".format(creds["host"]), \
                region_name='us-east-2')

    print("created dynamo client.")

    try:
        table = dynamodb.create_table(
            TableName='tweets',
            KeySchema=[
                {
                    'AttributeName': 'user',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'created_at',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'user',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'created_at',
                    'AttributeType': 'S'
                }

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        print("Table status:", table.table_status)

    except Exception as e:
        print("EXCEPTION: {}".format(e))

    try:
        session = botocore.session.get_session()
        dynamodb_session = session.create_client('dynamodb', \
                            region_name='us-east-2',
                            endpoint_url = "https://{}:8000".format(creds["host"]),
                            aws_access_key_id=creds["aws-access-key"],
                            aws_secret_access_key=creds["aws-secret-access-key"]) # low-level client

        waiter = dynamodb_session.get_waiter('table_exists')
        waiter.wait(TableName="tweets")

    except KeyboardInterrupt:
        pass

    except Exception as f:
        print("EXCEPTION: {}".format(f))

    tweets = dynamodb.Table("tweets")

    response = tweets.scan()
    m = len(response["Items"])
    print("got response for table {} w/ count {}.".format(tweets, m))

    print("Table status:", tweets.table_status)

if __name__ == "__main__":
    main()