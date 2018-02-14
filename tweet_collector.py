import tweepy
import csv
import logging
import json
import numpy as np

from datetime import datetime
from time import time, sleep

class TweetCollector(object):
    '''
    Class to connect to twitter rest API and collect tweets from users
    '''
    def __init__(self, credentials):
        auth = tweepy.OAuthHandler(credentials['consumer_key'], 
                                   credentials['consumer_secret'])
        auth.set_access_token(credentials['access_token'],
                              credentials['access_token_secret'])
        self.api = tweepy.API(auth, wait_on_rate_limit=True,
                              wait_on_rate_limit_notify=True)
        
        self.logger = logging.getLogger(__name__)

    def _clean_name(self, name):
        if name.startswith('@'):
            return name[1:].lower()
        return name.lower()

    def check_user_list(self, names=None, ids=None):

        if names is None and ids is None:
            raise ValueError("Must provide ids or names to check")
        if names is not None and ids is not None:
            raise ValueError("Mixing of ids and screen names is not supported")

        valid_entities = set()
        if names is not None:
            names = [self._clean_name(x) for x in names]
            for i in range(0, len(names), 100):
                batch = names[i:i + 100]
                response = self.api.lookup_users(screen_names=batch)
                valid_entities.update([r.screen_name.lower() for r in response])
            entities = set(names)
        else:
            for i in range(0, len(ids), 100):
                print(i)
                batch = ids[i:i + 100]
                response = self.api.lookup_users(user_ids=batch)
                valid_entities.update([r.id for r in response])
            entities = set(ids)

        return entities.difference(valid_entities), valid_entities

    def _get_batch(self, user, max_id, since_id, count):
        '''
        Grabs one batch of tweets from a user's timeline
        '''
        tweets = self.api.user_timeline(id=user,  
                                        max_id=max_id, 
                                        since_id=since_id,
                                        count=count)
        n = len(tweets)
        self.logger.debug(f'Got batch of size {n}.')
        return tweets

    def grab_timeline(self, user, since_id=None, max_id=None,
                      batch_size=200):
        '''
        Grabs as much as we can get from a user's timeline
        '''
        
        self.logger.info(f"Grabbing tweets from user: {user}.")

        out = []
        batch = self._get_batch(user=user, max_id=max_id, 
                                since_id=since_id, count=batch_size)
        out.extend(batch)
        if len(out) == 0:
            self.logger.warning(f"No tweets found for user {user}")
            return out

        first_id = out[-1].id - 1
        n_batches = 0

        if len(out) == batch_size:
            while len(batch) > 0:
                self.logger.debug(f'Retrieving additional batch. First id: '
                                  f'{first_id}')
                batch = self._get_batch(user=user, max_id=first_id, 
                                        since_id=since_id, count=batch_size)
                out.extend(batch)
                first_id = out[-1].id - 1
                n_batches += 1
            
        n = len(out)
        last = out[0]._json['created_at']
        first = out[-1]._json['created_at']
        self.logger.info(f"Got {n} tweets in {n_batches+1} batches.")
        self.logger.info(f"Oldest tweet created at: {first}.")
        self.logger.info(f"Latest tweet created at: {last}.")
        return out
