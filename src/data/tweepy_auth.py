import tweepy
from tweepy import OAuthHandler


def get_auth_token(_filepath):
    # Takes a text file containing four lines of twitter app credential keys and returns
    # a tweepy token to be used in future functions

    key_file_path = _filepath  # Standard filepath 'learn/twitter/twitter_keys'
    keys = []
    with open(key_file_path) as file:
        keys = file.read().splitlines()

    consumer_key = keys[0]
    consumer_secret = keys[1]
    access_token = keys[2]
    access_secret = keys[3]

    # Setup OAuthentication and access tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    return api
