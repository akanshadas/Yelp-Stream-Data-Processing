# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import sys
import json
import csv
import itertools
import time
import math
import random
import operator
import os
import glob
import re
import binascii
from datetime import datetime
from pyspark.streaming import StreamingContext
import statistics
import tweepy


# variables
#port_num = 9999
port_num = int(sys.argv[1])
out_file_path = sys.argv[2]

sequence_num = 0     
sample = []

f = open(out_file_path, "w")
f.write("")
f.close()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, tweet):
        global sequence_num 
        global sample

        sample_hashtags = dict()
        hashtags = tweet.entities['hashtags']  
        if len(hashtags) > 0: 
            sequence_num += 1

            if sequence_num <= 100:
                sample.append(tweet)
            else:
                randomValue = random.randint(0, sequence_num - 1)

                if randomValue < 100:
                    #add to sample. Replace this randomValue in the sample by the new element
                    sample[randomValue] = tweet

            #find the top 3 hashtags in the sample
            for a_tweet in sample:
                hashtagz = a_tweet.entities['hashtags']
                for tag in hashtagz:
                    tag_name = tag['text']
                    if tag_name in sample_hashtags:
                        sample_hashtags[tag_name] += 1
                    else:
                        sample_hashtags[tag_name] = 1

            sorted_sample_hashtags = sorted(sample_hashtags.items(), key=lambda x: (-x[1], x[0]))
            top_3_hashtags = []
            count = 0
            val = set()
            for hash,freq in sorted_sample_hashtags:
                val.add(freq)
                if len(val) > 3:
                    break
                else:
                    top_3_hashtags.append([hash, freq])

            f = open(out_file_path, "a")
            line = "The number of tweets with tags from the beginning: " + str(sequence_num) + "\n"
            f.write(line)
            for tag_name, freq in top_3_hashtags:
                tag_text = str(tag_name) + " : " + str(freq) + "\n"
                f.write(tag_text)
            f.write("\n")
            f.close()
        return 0

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
        else:
            print ("error:", status_code)
            return False
            

# ======================================================== START ========================================================
start = time.time()

consumer_key = "enter_your_consumer_key"
consumer_secret_key = "enter_your_consumer_secret_key"
access_token = "enter_your_access_token"
access_secret_key = "enter_your_secret_key"

auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret_key)
auth.set_access_token(key=access_token, secret=access_secret_key)

api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.api.wait_on_rate_limit = True

myStream.filter(track=["Trump", "business", "AI", "machine learning", "ML", "AI"])

end = time.time()
#print("\n\nDuration:", end - start)