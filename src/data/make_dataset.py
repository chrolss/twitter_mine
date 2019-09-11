import tweepy
from src.data.tweepy_auth import get_auth_token
from sqlalchemy import create_engine, MetaData, Table


class StreamListenerToDataFrame(tweepy.StreamListener):
    # A stream listener that is supposed to catch statuses and write them to a dataframe

    def __init__(self, _track_key, _maxtweets, _engine, _tweet_table, _user_table):
        super(StreamListenerToDataFrame, self).__init__()
        self.num_tweets = 0
        self.max_tweets = _maxtweets
        self.engine = _engine
        self.tweetList = []
        self.tweet_table = _tweet_table
        self.user_table = _user_table
        self.track_key = _track_key

    def on_status(self, status):
        if self.num_tweets < self.max_tweets:
            self.num_tweets += 1
            self.tweetList.append(status._json)

            ins_tweets = self.tweet_table.insert().values(id=status.id_str,
                                                          text=status.text,
                                                          created_at=status.created_at,
                                                          source=status.source,
                                                          author_id=status.author.id_str,
                                                          language=status.lang,
                                                          place=status.place,
                                                          search_key=self.track_key)
            self.engine.execute(ins_tweets)

            ins_auth = self.user_table.insert().values(id=status.author.id_str,
                                                       name=status.author.name,
                                                       location=status.author.location,
                                                       favourites_count=status.author.favourites_count,
                                                       followers_count=status.author.followers_count,
                                                       friends_count=status.author.friends_count,
                                                       screen_name=status.author.screen_name,
                                                       statuses_count=status.author.statuses_count,
                                                       description=status.author.description)
            self.engine.execute(ins_auth)

            print("Tweet number " + str(self.num_tweets) + " out of " + str(self.max_tweets))
            return True

        else:
            #with open('tweet_dump.txt', 'w') as file:
            #    file.write(json.dumps(self.tweetList, indent=4))

            print("Read all tweets")
            return False

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

        # returning non-False reconnects the stream, with backoff.


def mine_tweets_to_sql(_auth, _track_key, _maxtweets, _engine, _tweet_table, _user_table):
    my_stream_listener = StreamListenerToDataFrame(_track_key, _maxtweets, _engine, _tweet_table, _user_table)
    my_stream = tweepy.Stream(auth=_auth.auth, listener=my_stream_listener)
    my_stream.filter(track=[_track_key])

    return True


# Get the SQL credentials
key_file_path = 'src/data/sql_credentials'
keys = []
with open(key_file_path) as file:
    keys = file.read().splitlines()

sql_username = keys[0]
sql_password = keys[1]
ip = keys[2]

# Setup sqlengine and define table to write
engine_path = 'mssql+pyodbc://' + sql_username + ':' + sql_password + '@' + ip + '\\SQLEXPRESS/' + 'twitter' + '?driver=SQL+Server'
engine = create_engine(engine_path)
con = engine.connect()
metadata = MetaData()
metadata.reflect(engine)
tweet_table = Table('tweets', metadata)
authors_table = Table('authors', metadata)

# Get the tweepy token and start mining tweets
auth_token = get_auth_token('src/data/twitter_keys')
sl = mine_tweets_to_sql(auth_token, '#Brexit', 10, engine, tweet_table, authors_table)





