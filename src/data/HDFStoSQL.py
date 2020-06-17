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

# Database initializationsc

def connect_to_database():
    # Connects to the psql and returns engine and metadata object
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

    return engine, metadata, con


def insert_tweet(status, sqltable, engine):
    # Insert a short tweet into the FactTweets table
    if 'extended_tweet' in status.keys():
        tweet_text = status['extended_tweet']['full_text']
    else:
        tweet_text = status['text']

    insert_fact_stmt = sqltable.insert().values(tweet_id=status['id_str'],
                                                  user_id=status['user']['id_str'],
                                                  tweet_text=tweet_text,
                                                  tweet_source=status['source'],
                                                  truncated=status['truncated'],
                                                  in_reply_to_status_id_str=status['in_reply_to_status_id_str'],
                                                  in_reply_to_user_id_str=status['in_reply_to_status_id_str'],
                                                  created_at=status['created_at'],
                                                  is_rt=False,
                                                  related_tweet_id='N/A',
                                                  geo=status['geo'],
                                                  coordinates=status['coordinates'],
                                                  place='N/A',
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


    return True


def insert_retweet(status, sqltable, engine):
    # Insert a retweet into the FactTweets table
    insert_fact_stmt = sqltable.insert().values(tweet_id=status['id_str'],
                                                  user_id=status['user_id_str'],
                                                  tweet_text=status['text'],
                                                  tweet_source=status['source'],
                                                  truncated=status['truncated'],
                                                  in_reply_to_status_id_str=status['in_reply_to_status_id_str'],
                                                  in_reply_to_user_id_str=status['in_reply_to_status_id_str'],
                                                  created_at=status['created_at'],
                                                  is_RT=True,
                                                  related_tweet=status['retweeted_status']['id_str'],
                                                  geo=status['geo'],
                                                  coordinates=status['coordinates'],
                                                  place='N/A',
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


def insert_user(status, sqltable, engine):
    # Insert a user based on standard Twitter status API format into DimUsers table
    insert_user_stmt = sqltable.insert().values(user_id=status['user']['id_str'],
                                                username=status['user']['name'],
                                                screen_name=status['user']['screen_name'],
                                                location=status['user']['location'],
                                                url=status['user']['url'],
                                                description=status['user']['description'],
                                                protected=status['user']['protected'],
                                                verified=status['user']['verified'],
                                                followers_count=status['user']['followers_count'],
                                                friends_count=status['user']['friends_count'],
                                                listed_count=status['user']['listed_count'],
                                                favourites_count=status['user']['favourites_count'],
                                                statuses_count=status['user']['statuses_count'],
                                                created_at=status['user']['created_at'],
                                                time_zone=status['user']['time_zone'],
                                                geo_enabled=status['user']['geo_enabled'],
                                                lang=status['user']['lang'],
                                                )
    try:
        engine.execute(insert_user_stmt)
    except exc.IntegrityError:
        # If already exists, we should update the count fields
        print("User already exists")
        pass

    return True


def insert_bridge_hashtag(status, sqltable, engine):
    # Insert into table BridgeHashtag
    # Hashtags enter as a list of hashtags
    # status['entities']['hashtags'] = []
    temp_list = status['entities']['hashtags']

    for hashtag in temp_list:
        surrogate_key = status['id_str'] + hashtag['text']
        insert_hashtag_stmt = sqltable.insert().values(sid=surrogate_key,
                                                            tweet_id=status['id_str'],
                                                            hashtag=hashtag['text']
                                                            )
        try:
            engine.execute(insert_hashtag_stmt)
        except exc.IntegrityError:
            # If exists, skip
            print("Hashtag entry already exists")

    return True


def insert_bridge_users(status, sqltable, engine):
    # Insert into table BridgeUsers
    # The mentioned users appear in a list of mini-user objects
    # status['entities']['user_mentions']
    temp_user_list = status['entities']['user_mentions']
    for count, user in enumerate(temp_user_list):
        surrogate_key = status['id_str'] + str(count)
        insert_user_mentions_stmt = sqltable.insert().values(sid=surrogate_key,
                                                             tweet_id=status['id_str'],
                                                             user_id=status['user']['id_str'],
                                                             mentioned_id=user['id_str']
                                                             )
        try:
            engine.execute(insert_user_mentions_stmt)
        except exc.IntegrityError:
            # If exists, skip
            print("User mention entry already exists")

    return True


def process_tweet(tweet, FactTweets, DimUsers, BridgeHashtag, BridgeUsers, engine):
    insert_tweet(status=tweet, sqltable=FactTweets, engine=engine)
    insert_user(status=tweet, sqltable=DimUsers, engine=engine)
    insert_bridge_hashtag(status=tweet, sqltable=BridgeHashtag, engine=engine)
    insert_bridge_users(tweet, sqltable=BridgeUsers, engine=engine)

    return True