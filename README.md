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
Grabs tweets per specified users. Saves .json & .csv files per user, along with master data .csv.

### twitter_bq_upload.py
Pulls data from bigquery table & local, subsets to what is in local but not bq, uploads to bq & deduplicates.

### twitter_train_nn.py
Trains a recurrent neural network (RNN) to learn tweets based off of data in bq table.

### tweet_generate.py
Generates tweets based off of neural net.
