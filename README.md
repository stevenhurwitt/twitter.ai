# twitter.ai
Code to generate tweets using an RNN.

`creds.json`:

```json
{
    "twitter-api-key": "abc",
    "twitter-secret-key": "123",
    "twitter-bearer": "xyz",
    "twitter-access-token": "def",
    "twitter-secret-access": "456"
}
```

### tweet_grab.py
Grabs tweets per specified users.

`python3 -m src/main/tweet_grab`

### twitter_bq_upload.py
Pulls data from bigquery table & local, subsets to what is in local but not bq, uploads to bq & deduplicates.

`python3 -m src/main/twitter_bq_upload`

### twitter_train_nn.py
Trains a recurrent neural network (RNN) to learn tweets based off of data in bq table.

`python3 -m src/main/twitter_nn`

### tweet_generate.py
Generates tweets based off of neural net.

`python3 -m src/main/tweet_generate`
