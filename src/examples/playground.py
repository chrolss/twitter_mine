from src.data.tweepy_auth import get_auth_token
import tweepy
from src.data.StreamListenerHDFS import StreamListenerHDFS

token = get_auth_token('data/twitter_keys')
#tweet = tweepy.Cursor(token.search, q='#blm').item()

# Test the new HDFS StreamListener
my_stream_listener = StreamListenerHDFS(100, 10, 'test')
my_stream = tweepy.Stream(auth=token.auth, listener=my_stream_listener)
my_stream.filter(track='#BLM')


# Read the tweet pickles
import joblib
df = joblib.load('StreamReading_2020615-11251/test_2020615-11253987510.pkl')