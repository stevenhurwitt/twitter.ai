#!/bin/bash

docker run -p 8001:8000 -v dynamodb-local:/home/dynamodblocal/data amazon/dynamodb-local \
-jar ./DynamoDBLocal.jar \
-sharedDb -dbPath /home/steven/dynamodb/data/
