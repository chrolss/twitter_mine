from src.data.tweepy_auth import get_auth_token
import tweepy
from src.data.StreamListenerHDFS import StreamListenerHDFS
import os

token = get_auth_token('data/twitter_keys')
#tweet = tweepy.Cursor(token.search, q='#blm').item()

dirname = os.path.dirname(__file__)
# Test the new HDFS StreamListener
my_stream_listener = StreamListenerHDFS(30000, 1200, 'blm', dirname)
my_stream = tweepy.Stream(auth=token.auth, listener=my_stream_listener)
my_stream.filter(track=['#BLM'])
