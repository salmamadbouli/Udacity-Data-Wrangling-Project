#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling Project: WeRateDogs

# 
# ## Introduction
# 
# The WeRateDogs Twitter archive contains basic tweet data for all 5000+ of their tweets, the purpose of this project is to practice gathering data from a variety of sources and in a variety of formats, assess its quality and tidiness, then clean it.

# ### Importing packages

# In[1]:


import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import tweepy
from tweepy import OAuthHandler
import json
from timeit import default_timer as timer


# ## Gathering Data

# ### Twitter archive data

# In[2]:


# Load the file into dataframe
twitter_archive = pd.read_csv('twitter-archive-enhanced.csv', sep=',')


# ### Image prediction data

# In[3]:


# Downloading the file from the provided URL
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
request = requests.get(url)
open('image_predictions.tsv', 'wb').write(request.content)


# In[4]:


# Load the file into dataframe
image_predictions = pd.read_csv('image_predictions.tsv', sep = '\t')


# ### Tweets JSON data

# In[6]:


"""
# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = 'HIDDEN'
consumer_secret = 'HIDDEN'
access_token = 'HIDDEN'
access_secret = 'HIDDEN'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

# NOTE TO STUDENT WITH MOBILE VERIFICATION ISSUES:
# df_1 is a DataFrame with the twitter_archive_enhanced.csv file. You may have to
# change line 17 to match the name of your DataFrame with twitter_archive_enhanced.csv
# NOTE TO REVIEWER: this student had mobile verification issues so the following
# Twitter API code was sent to this student from a Udacity instructor
# Tweet IDs for which to gather additional data via Twitter's API
tweet_ids = twitter_archive.tweet_id.values
len(tweet_ids)

# Query Twitter's API for JSON data for each tweet ID in the Twitter archive
count = 0
fails_dict = {}
start = timer()
# Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as outfile:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
            tweet = api.get_status(tweet_id, tweet_mode='extended')
            print("Success")
            json.dump(tweet._json, outfile)
            outfile.write('\n')
        except tweepy.TweepError as e:
            print("Fail")
            fails_dict[tweet_id] = e
            pass
end = timer()
print(end - start)
print(fails_dict)
"""


# In[5]:


# Read the JSON data and save it into dataframe
tweet_data = []

with open('tweet-json.txt', 'r', encoding='utf8') as file:
    for line in file:
        tweet_line = json.loads(line)
        tweet_data.append({'tweet_id': tweet_line['id'],
                        'favorite_count': tweet_line['favorite_count'],
                        'retweet_count': tweet_line['retweet_count'],
                        'retweeted': tweet_line['retweeted']})
        
tweets_df = pd.DataFrame(tweet_data)


# # Assessing Data: Quality and Tidiness Issues

# ### ** [Twitter Archive Data]

# In[6]:


twitter_archive.info()


# In[7]:


# View first 10 rows of twitter_archive DataFrame
twitter_archive.head(10)


# In[8]:


# View last 5 rows of twitter archive DataFrame
twitter_archive.tail()


# In[9]:


# view sum of null values
twitter_archive.isnull().sum()


# In[10]:


# View descriptive analysis of twitter archive DataFrame
twitter_archive.describe()


# ### ** [Image prediction data]
# 

# In[11]:


image_predictions.info()


# In[12]:


# View first 10 rows of twitter_archive DataFrame
image_predictions.head(10)


# In[13]:


# View last 5 rows of image predictions DataFrame
image_predictions.tail()


# In[14]:


# View descriptive analysis of image predictions DataFrame
image_predictions.describe()


# In[15]:


# view sum of null values
image_predictions.isnull().sum()


# ### **[Tweets JSON data]

# In[16]:


tweets_df.info()


# In[17]:


# View first 10 rows of tweet_info DataFrame
tweets_df.head(10)


# In[18]:


# View last 5 rows of tweet_info DataFrame
tweets_df.tail()


# In[19]:


tweets_df.favorite_count.describe()


# In[20]:


tweets_df.retweet_count.describe()


# In[21]:


# view sum of null values
tweets_df.isnull().sum()


# 
# ## Quality
# - Some tweets have no images.
# - Convert the null values to nan type.
# - Change datatypes.
# - Remove retweets columns.
# - Change the source content into human readable form.
# - Capitalize the first letter of dog names.
# - Display full content of the “text” column.
# - Fixing the [rating_denominator] that have values != 1
# 
# 
# 
# ## Tidiness
# - doggo, floofer, pupper, and puppo columns in twitter_archive file should be values.
# - Merge 'json file' and 'image_predictions' to 'twitter_archive'
# 

# In[22]:


# Make copy of original data frames.
twitter_archive_clean = twitter_archive.copy()
image_predictions_clean = image_predictions.copy()
tweets_df_clean = tweets_df.copy()


# ## Cleaning

# ### Tidiness Issues
# #### - doggo, floofer, pupper, and puppo columns in twitter_archive file should be values.
# 

# In[23]:


# Code
twitter_archive_clean['stage'] = twitter_archive_clean.text.str.extract('(doggo|floofer|pupper|puppo)', expand=True)
twitter_archive_clean['stage'] = twitter_archive_clean['stage'].astype('category')
twitter_archive_clean.drop(['doggo', 'floofer', 'pupper', 'puppo'], axis=1, inplace=True)


# In[24]:


# Test
twitter_archive_clean.head(30)


# #### - Merge 'image_predictions' to 'twitter_archive'

# In[25]:


#Code
twitter_archive_clean = twitter_archive_clean.merge(image_predictions_clean, on='tweet_id', how='inner')
twitter_archive_clean = twitter_archive_clean.merge(tweets_df_clean, on='tweet_id', how='inner')


# In[26]:


#Test
twitter_archive_clean.head()


# ### Quality Issues
# #### - Remove tweets with no images

# In[27]:


twitter_archive_clean = twitter_archive_clean.dropna(subset=['expanded_urls'])


# In[28]:


#test the code
sum(twitter_archive_clean['expanded_urls'].isnull())


# #### - Remove the incorrect dog names and convert the none values to nan type.

# In[29]:


# code
names_list = ['a','the','an']
for name in names_list:
        twitter_archive_clean['name'].replace(name, 'None', inplace=True)
twitter_archive_clean['name'] = twitter_archive_clean['name'].replace('None', np.NaN)


# In[30]:


#test
twitter_archive_clean.info()


# #### - Change datatypes.

# In[31]:


# code
twitter_archive_clean['timestamp'] = pd.to_datetime(twitter_archive_clean.timestamp)
twitter_archive_clean['source'] = twitter_archive_clean['source'].astype('category')
twitter_archive_clean['rating_numerator'] = twitter_archive_clean['rating_numerator'].astype('float')
twitter_archive_clean['rating_denominator'] = twitter_archive_clean['rating_denominator'].astype('float')


# In[32]:


#test
twitter_archive_clean.info()


# #### - Remove retweets columns.

# In[33]:


#remove rows with retweets
twitter_archive_clean = twitter_archive_clean[pd.isnull(twitter_archive_clean.retweeted_status_id)]

# remove duplicates
twitter_archive_clean = twitter_archive_clean.drop_duplicates()

# Code - drop all columns related to retweets
retweet_columns = ['retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp']

twitter_archive_clean = twitter_archive_clean.drop(retweet_columns, axis=1)


# In[34]:


# test
twitter_archive_clean.info()


# #### - Change the source content into human readable form.
# 

# In[35]:


# code
twitter_archive_clean['source'] = twitter_archive_clean['source'].replace('<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>',
                                                                          'Twitter for iphone')
twitter_archive_clean['source'] =  twitter_archive_clean['source'].replace('<a href="http://vine.co" rel="nofollow">Vine - Make a Scene</a>',
                                                                          'Vine - Make a Scene')
twitter_archive_clean['source'] =  twitter_archive_clean['source'].replace('<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>',
                                                                           'Twitter Web Client')
twitter_archive_clean['source'] =  twitter_archive_clean['source'].replace('<a href="https://about.twitter.com/products/tweetdeck" rel="nofollow">TweetDeck</a>',
                                                                           'TweetDeck')


# In[36]:


# test
twitter_archive_clean['source'].value_counts()


# #### - Capitalize the first letter of dog names.

# In[37]:


# Code
twitter_archive_clean['name'] = twitter_archive_clean.name.str.capitalize()


# In[38]:


#test
twitter_archive_clean['name'].str.islower().sum()


# #### - Display full content of the “text” column.

# In[39]:


# code
pd.set_option('display.max_colwidth', -1)


# In[40]:


# test
twitter_archive_clean.head()


# #### - Fixing the [rating_denominator] that have values != 10

# In[41]:


# code
twitter_archive_clean[twitter_archive_clean['rating_denominator'] != 10]


# In[42]:


# correct the first 5 samples
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 820690176645140481, ['rating_denominator']] = 10
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 810984652412424192, ['rating_denominator']] = 10
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 758467244762497024, ['rating_denominator']] = 10
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 740373189193256964, ['rating_denominator']] = 10
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 731156023742988288, ['rating_denominator']] = 10


# In[43]:


# test
twitter_archive_clean.loc[twitter_archive_clean.tweet_id == 820690176645140481]


# ## Storing 

# In[44]:


#code
twitter_archive_clean.to_csv('twitter_archive_master.csv')


# In[45]:


#test
df1 = pd.read_csv('twitter_archive_master.csv', sep=',')


# In[46]:


df1.info()


# ## Analyzing

# ### - What is the most common dog stage?
# 

# In[47]:


# code
stages = df1['stage'].value_counts().head(4).index
sns.set(style="whitegrid")
sns.countplot(data = df1, x = 'stage', order = stages, orient = 'h')
plt.xticks(rotation = 360)
plt.xlabel('Dog Stages', fontsize=16)
plt.ylabel('Count', fontsize=16)
plt.title('The Distribution of Dog Stages',fontsize=16)


# It looks like the 'pupper' is the most common dog stage then followed by 'doggo' and 'puppo'.

# ### - The Distribution of Sources

# In[48]:


sources = df1['source'].value_counts().index
print(df1['source'].value_counts())
sns.set(style="darkgrid")
sns.countplot(data = df1, y = 'source', order = sources)
plt.xticks(rotation = 360)
plt.xlabel('Count', fontsize=16)
plt.ylabel('Sources', fontsize=16)
plt.title('The Distribution of Source',fontsize=16)


# The plot shows that the main source of tweets is from twitter app for iphone while the TweetDeck is very rare [less than 1%].

# ### - Relationship between favourite and retweet counts.
# 

# In[49]:


colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99']
plt.scatter(df1['favorite_count'], df1['retweet_count'],alpha = 0.5, color = colors)
plt.xlabel('Favorite Count', fontsize = 14)
plt.ylabel('Retweet Count', fontsize = 14)
plt.title('Favorite Count vs. Retweet Count', fontsize = 16)


# The plot shows a strong positive correlation between retweet counts and favorite counts.

# In[ ]:




