import json
import logging 
import os
import sys
import re
import numpy as np
import pandas as pd
from tweet_collector import TweetCollector


# Parameters
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

## File to write collected data to / if you want to use a db connection to write
## to you have to change the code at the end of this script with your custom
## connection
OUT_FILE = 'your/out/path/here.json'
## List of twitter screen names (integer user ids coming soon (or trivial to
## implemetn yourself)
TWITTER_USERS = [] 
TWITTER_CREDENTIALS = {"access_token": '', 
                       "consumer_key": '',
                       "access_token_secret": '',
                       "consumer_secret": ''}

# Set up logging
logger = logging.getLogger('tweet_collector')
hdlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

# Data Collection
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
logger.info("Starting data collection")

df = pd.DataFrame({'screen_name': TWITTER_USERS})
n_users = df.shape[0]
logger.info(f"Processing {n_users} users")

# Initialize collector 
collector = TweetCollector(TWITTER_CREDENTIALS)

# Get the latest tweet for each user if script was run before
if os.path.exists(OUT_FILE):
    users = {}
    with open(OUT_FILE) as infile:
        # Count total lines (for status update)
        for n_lines, line in enumerate(infile):
            pass
        # Reset iterator to start
        infile.seek(0)
        logger.info(f"Found {n_lines:,} previously collected tweets in {OUT_FILE}." 
                     " Getting last tweet for each organization...")
        for i,line in enumerate(infile):
            if i % 1e5 == 0:
                logger.info(f"\rProcessed {i:,} of {n_lines:,} tweets")
            tweet = json.loads(line)
            screen_name = tweet['user']['screen_name']
            last_id = users.get(screen_name, -1)
            if tweet['id'] > last_id:
                users[screen_name] = tweet['id']

# Create a dataframe with all screen_names and last tweet id (if existent)
last_tweet = pd.DataFrame({'screen_name': [x.lower() for x in users.keys()], 
                           'last_tweet_id': [int(x) for x in users.values()]})
df = df.merge(last_tweet, on=['screen_name'], how='left')
## Make sure tweet ids stay int64 datatype (if not necessary precision is lost
## on these really big integers)
df['last_tweet_id'] = [int(x) if not np.isnan(x) else 0 
                       for x in df.last_tweet_id]

# Cross check user names with API
handles = df['screen_name']
invalid_handles, valid_handles = collector.check_user_list(handles)
logger.info(f"These handles are invalid: {invalid_handles}")

# Collect tweets and dump to OUTFILE
n_tweets_collected = 0
with open(OUT_FILE, 'a+', encoding='utf-8') as outfile:
    for index, row in df.iterrows():
        user = row['screen_name']
        since_id = row['last_tweet_id']
        # Grab tweets
        try:
            tweets = collector.grab_timeline(user, since_id=since_id)
        except Exception as e:
            logger.error(f"An exception occurred for handle {user}:\n {e}")
            continue
        logger.info('Dumping tweets')
        for t in tweets:
            n_tweets_collected += 1
            json.dump(t._json, outfile)
            outfile.write('\n')

logger.info(f"Collected {n_tweets_collected:,} new tweets")
