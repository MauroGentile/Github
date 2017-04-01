import tweepy
import sys
import os
import csv
import pprint
import requests
import json
import time
import codecs
import pandas as pd
import numpy as np
import datetime
from unidecode import unidecode
from Queue import Queue
from threading import Thread
from token_2 import *


def credentials_creation(num_tokens,api):
    auth=[]
    x=[]
    for y in xrange(num_tokens):
        x.append(token_class(tokens[str(y+1)]))
        auth.append(tweepy.OAuthHandler(x[y].consumer_key, x[y].consumer_secret))
        auth[y].secure = True
        auth[y].set_access_token(x[y].access_token, x[y].access_token_secret)
        api.append(tweepy.API(auth[y], wait_on_rate_limit=True, wait_on_rate_limit_notify=True) )


def check_credentials(num_tokens,api):
    verify_credentials = []
    for y in xrange(num_tokens):
            try:
                me_medesimo = api[y].me()
                #			print (me_medesimo.name) + " is connected!"
                verify_credentials.append(1)
            except:
                print "Error: account num",y+1,"not connected"
                verify_credentials.append(0)
    if sum(verify_credentials) == num_tokens:
        print "Connection is OK!"




UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

pp = pprint.PrettyPrinter(indent=4)

requests.packages.urllib3.disable_warnings()

consumer_key = 'Czz9Rh1gXrDXaur6RqH0wlWIp'
consumer_secret = '2rWajmJrykKiftIBcm6kb3aa6wu9Kxu6LG7LpmRXC8miMJEA44'
access_token = '17709504-XW68jHBLiKBNroxPdKqjlzoJIzQ6785X78PB3yMkj'
access_token_secret = 'b5wdCIvg8OMnjKCBQ1MIlIh9CIvpcavhLYQjFcqIxZlp6'

                    
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.secure = True
auth.set_access_token(access_token, access_token_secret)

# eseguo l'autenticazione ed ottengo il riferimento alle API Twitter
api = tweepy.API(auth)




def init_df():
    col=[
        "contributors_enabled",
        "created_at",
        "default_profile_image",
        "default_profile",
        "description",
        "favourites_count",
        "follow_request_sent",
        "followers_count",
        "friends_count",
        "geo_enabled",
        "id_str",
        "id",
        "is_translator",
        "lang",
        "listed_count",
        "location",
        "name",
        "notifications",
        "profile_background_color",
        "profile_background_image_url_https",
        "profile_background_image_url",
        "profile_background_tile",
        #"profile_banner_url",
        "profile_image_url_https",
        "profile_image_url",
        "profile_link_color",
        "profile_sidebar_border_color",
        "profile_sidebar_fill_color",
        "profile_text_color",
        "profile_use_background_image",
        "protected",
        "screen_name",
        #"show_all_inline_media",
        "statuses_count",
        "time_zone",
        "url",
        "utc_offset",
        "verified",
        #"withheld_in_countries",
        #"withheld_scope"
    ]
    return(pd.DataFrame(columns=col))

def append_in_df(user,b):
    col=b.columns
    b=pd.DataFrame(np.array([[user.contributors_enabled,user.created_at,user.default_profile_image,user.default_profile,
           			user.description.encode('utf-8'),
                    user.favourites_count,
                    user.follow_request_sent,
                    user.followers_count,
                    user.friends_count,
                    user.geo_enabled,
                    user.id_str,user.id,
                    user.is_translator,
                    user.lang.encode('utf-8'),
                    user.listed_count,
                    user.location.encode('utf-8'),
                    user.name.encode('utf-8'),
                    user.notifications,
                    user.profile_background_color.encode('utf-8'),
                    user.profile_background_image_url_https,user.profile_background_image_url,
                    user.profile_background_tile,
                    user.profile_image_url_https.encode('utf-8'),
                    user.profile_image_url,user.profile_link_color,
                    user.profile_sidebar_border_color,
                    user.profile_sidebar_fill_color,
                    user.profile_text_color,
                    user.profile_use_background_image,user.protected,
                    user.screen_name.encode('utf-8'),user.statuses_count,
                    user.time_zone,user.url,user.utc_offset,user.verified]]), columns=col).append(b, ignore_index=True)
                    
    return(b)






num_tokens = len(tokens)
api = []

credentials_creation(num_tokens,api)
check_credentials(num_tokens,api)



file_log="log.csv"
file_results="users.csv"
b=init_df()
now= datetime.datetime.now()
Entities_DIR = "Entities"
Tweets_DIR = "Tweets_profilo"
Source_DIR="Source"





cwd = os.getcwd()
cwd_Ent = os.path.join(cwd, Entities_DIR)
cwd_Sou = os.path.join(cwd, Source_DIR)


i=0

users_queue = Queue()

for filename in os.listdir(cwd_Sou):
        if filename.startswith("."):
            continue
        file_in=os.path.join(cwd_Sou,filename)
        with open(file_in, "r") as file:
            file_reader=csv.reader(file)
            for row in file_reader:
                users_queue.put(row)
                

print users_queue.qsize()
while not users_queue.empty():
    m=users_queue.get()
    print (m)

print len(m)



for i in range(num_tokens):
    w1 = Thread(target=worker, args=(api,i,users_queue,time_single_request))
    w1.setDaemon(True)
    w1.start()
users_queue.join()




        
        
        

"""
                    
#user.profile_banner_url,

					
    	        
#dettagli_trend = [position, unicode(trend['name']).encode('utf-8')]
print (api.rate_limit_status()["resources"]["users"]["/users/lookup"])
"""
