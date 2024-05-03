import sys
import importlib.util
import pandas as pd
import json
import time
import datetime as dt
import tweepy
import numpy as np

#import S3 stuff
import boto3

class screen_name_error(Exception):
    """Raise error if screen_name changed"""
    def __init__(self, user_id, screen_name):
        self.user_id = user_id
        self.screen_name = screen_name

class StreamListener(tweepy.Stream):
    def __init__(self,consumer_key,consumer_secret,access_token,access_token_secret):
        super(StreamListener,self).__init__(consumer_key,consumer_secret,access_token,access_token_secret)       
        
    def on_status(self, status):
        ### Insert tweet into cosmos db container
        #print(status.text)
        tweet = status._json
        # replace id field with string version, keep numeric in 'id_num' field      
        tweet['id_num'] = tweet['id']
        tweet['id'] = tweet['id_str']
        # create date field for partitioning
        tweet['date'] = pd.to_datetime(tweet['created_at']).strftime('%d/%m/%y')
        # datetime string for querying
        tweet['datetime'] = dt.datetime.strftime(
            dt.datetime.strptime(tweet['created_at'],"%a %b %d %X %z %Y"), 
            "%Y-%m-%dT%H:%M:%S.%f0Z"
        )

        current_time = dt.datetime.now()
        date = current_time.strftime("%d-%m-%Y")
        hour_window = current_time.strftime("%H")
        output_name = "raw/output-"+str(date)+"-"+str(hour_window)+".jsonl"

        #print(tweet)
        with open(output_name,"a") as output:
            json.dump(tweet,output)
            output.write('\n')

        #print("Written to file.")

    def on_exception(self, exception):
        if isinstance(exception,screen_name_error):
            #print(dt.datetime.now(), "- Screen name changed:", exception.user_id, exception.screen_name)
            # Modify seed file
            seed_users.loc[seed_users.user_id == exception.user_id, 'screen_name'] = exception.screen_name.lower()
            seed_users.loc[seed_users.user_id == exception.user_id, 'screen_name_orig'] = exception.screen_name
            seed_users.loc[seed_users.user_id == exception.user_id, 'updated'] = str(dt.datetime.now())
            seed_users.to_csv(project_path + "/data/all_screen_names_ids.csv", index=False)
            return True            
       
    def on_request_error(self, status_code):
        if status_code == 420:
            return False
        else: print('Request Error:',status_code)


if __name__ == "__main__":
    ### Project path and stream group
    project_path = sys.argv[1] 
    n_group = int(sys.argv[2])

    keys_file = open("twitter_keys.json")
    twitter_keys = json.load(keys_file)
    client = tweepy.Client(bearer_token=twitter_keys["bearer-token"])
    api = tweepy.API(client)

    consumer_key = twitter_keys["consumer-key"]
    consumer_secret = twitter_keys["consumer-key-secret"]
    access_token = twitter_keys["access-token"]
    access_token_secret = twitter_keys["access-token-secret"]

    seed_users = pd.read_csv(project_path + "/data/all_screen_names_ids.csv",dtype={'user_id': 'str'})
    keywords = pd.read_csv(project_path + "/data/keywords.csv")
    s3client = boto3.client("s3")
    sns_client = boto3.client('sns', region_name="us-east-2") # for receiving notifications of errors

    while True:
        ids = list(seed_users[seed_users.user_id.notna()].user_id.values)[n_group*400:(n_group+1)*400]
        print(ids)
        users = list(seed_users[seed_users.screen_name.notna()].screen_name.values)[n_group*400:(n_group+1)*400]
        print(users)
        users = ["@" + x for x in users if "#" not in x]
        tracking_words = users+list(keywords.word)
        print(tracking_words)
        print()
        print(dt.datetime.now())
        print("*** STARTING LOOP ***")
        print("Number of users: ", len(users), users[:3])
        print("Number of ids: ", len(ids), ids[:3])
        
        dict_users = {}
        for _, row in seed_users[seed_users.user_id.notna()].iterrows():
            dict_users[row['user_id']] = row['screen_name'].replace("@","")
        
        # Start tweepy streamer tracking seed_users
        #print('Starting streamer...')
        
        listener = StreamListener(consumer_key,consumer_secret,access_token,access_token_secret)
        try:
            listener.filter(follow=ids, track=tracking_words)
        
        # Handle screen name changing for seed users - update input file
        except screen_name_error as screen_name_change:
            #print()
            #print(dt.datetime.now())
            #print("Screen name changed", screen_name_change.user_id, screen_name_change.screen_name)
            # Modify seed file
            seed_users.loc[seed_users.user_id == screen_name_change.user_id, 'screen_name'] = screen_name_change.screen_name
            seed_users.to_csv(project_path + "/data/all_screen_names_ids.csv", index=False)

        except Exception as e:
            #print()
            #print(dt.datetime.now())
            #print("Error:", e)
            #print("Waiting 60s")
            time.sleep(60)
            #print("Restarting...")