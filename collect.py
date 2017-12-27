import json
import logging
import pickle
import os

import pandas as pd

from twitter_credentials import credentials
from tweet_collector import TweetCollector

HANDLE_FILE = 'data/TwitterHandles_101017.xlsx'
OUT_FILE = 'tweets.json'
LOG_FILE = 'logs.txt'
PROCESSED_CACHE_FILE = 'user_cache.p'
ERROR_CACHE_FILE = 'error_user_cache.p'

# Load twitter ids of interest groups
#df = pd.read_excel('data/TwitterHandles_101017.xlsx')
df = pd.read_csv('crc_results.csv')
df = df[df.hit_de == 1]

# Initialize collector 
collector = TweetCollector(credentials)

# Set up logging
logger = logging.getLogger('tweet_collector')
hdlr = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

# Cross check user names with API
ids = df.id.tolist()
#invalid_ids, valid_ids = collector.check_user_list(ids=ids)
#logger.info(f"These handles are invalid: {invalid_handles}")

# Collect tweets and dump to OUTFILE

## Check for previously collected data
if os.path.exists(PROCESSED_CACHE_FILE):
    processed = pickle.load(open(PROCESSED_CACHE_FILE, 'rb'))
    n = len(processed)
    logger.info(f"Found {n} processed handles, skipping those")
else:
    processed = set()

failed_handles = []
ids = set(ids)
ids = ids.difference(processed)
with open(OUT_FILE, 'a+', encoding='utf-8') as outfile:
    for i,user in enumerate(ids):
        # Grab tweets
        try:
            tweets = collector.grab_timeline(user=user)
            n = len(tweets)
        except Exception as e:
            logger.error(f"An exception occurred for handle {user}:\n {e}")
            #logger.exception(f"An exception occurred for handle {user}")
            failed_handles.append(user)
            processed.update([user])
            continue
        if len(tweets) > 0:
            logger.info('Dumping tweets')
        for t in tweets:
            json.dump(t._json, outfile)
            outfile.write('\n')
        pickle.dump(processed, open(PROCESSED_CACHE_FILE, 'wb'))
        pickle.dump(failed_handles, open(ERROR_CACHE_FILE, 'wb'))
