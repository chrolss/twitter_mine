import json
import os
from sqlalchemy import create_engine, MetaData, Table, exc

# Instructions
# 1. Read one .pkl at the time
# 2. Read one tweet at the time
# 3. Insert one tweet into SQL at the time

# Tweet variations
# Type 1: Short tweet, data model residing in StreamStatusDataModel
# Type 2: Long tweet, data model residing in StreamStatusDataModelExtended
# Type 3: RT, data model residing in StreamStatusDataModelRT

# Database initializations
# Get the SQL credentials
key_file_path = 'src/data/sql_credentials'
keys = []
with open(key_file_path) as file:
    keys = file.read().splitlines()

sql_username = keys[0]
sql_password = keys[1]
ip = keys[2]
database = keys[3]

# Setup sqlengine and define table to write
engine_path = 'postgresql+pg8000://' + keys[0] + ':' + keys[1] + '@' + keys[2] + '/' + keys[3]
engine = create_engine(engine_path)
con = engine.connect()
metadata = MetaData()
metadata.reflect(engine)
FactTweets = Table('FactTweets', metadata)
DimUsers = Table('DimUsers', metadata)
BridgeHashtag = Table('BridgeHashtag', metadata)
BridgeUsers = Table('BridgeUsers', metadata)


def insert_short_tweet(status):
    # Insert a short tweet into the database
    insert_fact_stmt = FactTweets.insert().values(tweet_id=status['id_str'],
                                                  user_id=status['user_id_str'],
                                                  text=status['text'],
                                                  source=status['source'],
                                                  truncated=status['truncated'],
                                                  in_reply_to_status_id_str=status['in_reply_to_status_id_str'],
                                                  in_reply_to_user_id_str=status['in_reply_to_status_id_str'],
                                                  created_at=status['created_at'],
                                                  is_RT='False',
                                                  related_tweet='N/A',
                                                  geo=status['geo'],
                                                  coordinates=status['coordinates'],
                                                  place=status['place'],
                                                  contributors=status['contributors'],
                                                  is_quote_status=status['is_quote_status'],
                                                  quote_count=status['quote_count'],
                                                  reply_count=status['reply_count'],
                                                  retweet_count=status['retweet_count'],
                                                  favorite_count=status['favorite_count']
                                                  )

    try:
        engine.execute(insert_fact_stmt)
    except exc.IntegrityError:
        # If already exists'], we should update the count fields
        print("Tweet already exists")
        pass

    return True


def insert_retweet(status):
    # Insert a retweet into the database
    insert_fact_stmt = FactTweets.insert().values(tweet_id=status['id_str'],
                                                  user_id=status['user_id_str'],
                                                  text=status['text'],
                                                  source=status['source'],
                                                  truncated=status['truncated'],
                                                  in_reply_to_status_id_str=status['in_reply_to_status_id_str'],
                                                  in_reply_to_user_id_str=status['in_reply_to_status_id_str'],
                                                  created_at=status['created_at'],
                                                  is_RT='True',
                                                  related_tweet=status['retweeted_status']['id_str'],
                                                  geo=status['geo'],
                                                  coordinates=status['coordinates'],
                                                  place=status['place'],
                                                  contributors=status['contributors'],
                                                  is_quote_status=status['is_quote_status'],
                                                  quote_count=status['quote_count'],
                                                  reply_count=status['reply_count'],
                                                  retweet_count=status['retweet_count'],
                                                  favorite_count=status['favorite_count']
                                                  )

    try:
        engine.execute(insert_fact_stmt)
    except exc.IntegrityError:
        # If already exists'], we should update the count fields
        print("Tweet already exists")
        pass

    return True


def insert_user(user):
    # Insert a user based on standard Twitter status API format
    insert_user_stmt = DimUsers.insert().values(uid=user['id_str'],
                                                username=user['username'],
                                                screen_name=user['screen_name'],
                                                location=user['location'],
                                                url=user['url'],
                                                description=user['description'],
                                                protected=user['protected'],
                                                verified=user['verified'],
                                                followers_count=user['followers_count'],
                                                friends_count=user['frinds_count'],
                                                listed_count=user['listed_count'],
                                                favourites_count=user['favourites_count'],
                                                statuses_count=user['statuses_count'],
                                                created_at=user['created_at'],
                                                time_zone=user['time_zone'],
                                                geo_enabled=user['geo_enabled'],
                                                lang=user['lang'],
                                                )
    try:
        engine.execute(insert_user_stmt)
    except exc.IntegrityError:
        # If already exists, we should update the count fields
        print("User already exists")
        pass

    return True


def insert_bridge_hashtag(status):
    # Hashtags enter as a list of hashtags
    # status['entities']['hashtags'] = []
    temp_list = status['entities']['hashtags']
    for hashtag in temp_list:
        surrogate_key = status['id_str'] + hashtag
        insert_hashtag_stmt = BridgeHashtag.insert().values(sid=surrogate_key,
                                                            tweet_id=status['id_str'],
                                                            hashtag=hashtag
                                                            )
        try:
            engine.execute(insert_hashtag_stmt)
        except exc.IntegrityError:
            # If exists, skip
            print("Hashtag entry already exists")

    return True


def insert_bridge_users(status):
    # The mentioned users appear in a list of mini-user objects
    # status['entities']['user_mentions']
    temp_user_list = status['entities']['user_mentions']
    for count, user in enumerate(temp_user_list):
        surrogate_key = status['id_str'] + str(count)
        insert_user_mentions_stmt = BridgeHashtag.insert().values(sid=surrogate_key,
                                                                  tweet_id=status['id_str'],
                                                                  user_id=status['user']['id_str'],
                                                                  mentioned_user_id=user['id_str']
                                                                  )
        try:
            engine.execute(insert_user_mentions_stmt)
        except exc.IntegrityError:
            # If exists, skip
            print("User mention entry already exists")

    return True
