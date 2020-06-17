from src.data.HDFStoSQL import *
import os
import joblib

dirname = os.path.dirname(__file__)
print(dirname)

# Connect to psql and catch engine and metadata object
(engine, metadata, connection) = connect_to_database()
FactTweets = Table('facttweets', metadata)
DimUsers = Table('dimusers', metadata)
BridgeUsers = Table('bridgeusers', metadata)
BridgeHashtag = Table('bridgehashtag', metadata)

# Go through folder of readings
folder_of_tweets = 'StreamReading_2020617-164856'
list_of_tweet_files = os.listdir(folder_of_tweets)

# Iterate each file in the folder
for tweetfile in list_of_tweet_files:
    tweets = joblib.load(os.path.join(folder_of_tweets, tweetfile))
    for tweet in tweets:
        if 'RT @' in tweet['text']:
            # Process retweets
            print('This was a retweet')
            process_tweet(tweet=tweet, FactTweets=FactTweets, DimUsers=DimUsers,
                          BridgeHashtag=BridgeHashtag, BridgeUsers=BridgeUsers, engine=engine)
            # INSERT the retweet into FactTweets
            process_tweet(tweet=tweet['retweeted_status'], FactTweets=FactTweets, DimUsers=DimUsers,
                          BridgeHashtag=BridgeHashtag, BridgeUsers=BridgeUsers, engine=engine)
        else:
            # INSERT INTO FactTweets
            print("This was a normal")
            process_tweet(tweet=tweet, FactTweets=FactTweets, DimUsers=DimUsers,
                          BridgeHashtag=BridgeHashtag, BridgeUsers=BridgeUsers, engine=engine)



# TEST
df = joblib.load('StreamReading_2020617-104624/test_2020617-1048768195.pkl')
status = df[0]