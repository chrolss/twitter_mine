import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from collections import Counter


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
tweet_OLTP_table = Table('trump_tweets', metadata)
authors_table = Table('users', metadata)
stmt_tweets = select([tweet_OLTP_table])
stmt_authors = select([authors_table])
tweets = con.execute(stmt_tweets).fetchall()
authors = con.execute(stmt_authors).fetchall()

# Create the OLTP dataframes
df = pd.DataFrame(tweets, columns=['id', 'content', 'created_at', 'source', 'author_id', 'lang', 'place', 'search_key'])
users = pd.DataFrame(authors, columns=['id', 'name', 'loc', 'favourites_count', 'followers_count', 'friends_count',
                                       'screen_name', 'statuses_count', 'description'])


# Setup tokenization
word_tokenizer = RegexpTokenizer('[A-z]{2,}')
emoji_tokenizer = RegexpTokenizer("['\U0001F300-\U0001F5FF'|'\U0001F600-\U0001F64F'|'\U0001F680-\U0001F6FF'|"
                                  "'\u2600-\u26FF\u2700-\u27BF']")
hashtag_tokenizer = RegexpTokenizer('#[A-z]+')
url_tokenizer = RegexpTokenizer('(?:https://[A-z]+\.[A-z]+\.?(?:[A-z]+)?/[A-z0-9]+)')
# the non-capturing groups from this site https://stackoverflow.com/questions/36353125/nltk-regular-expression-tokenizer
user_tokenizer = RegexpTokenizer('@[A-z0-9]+')


def list_to_text(full_text, tokenizer):
    token_text = ''
    ll = tokenizer.tokenize(full_text.lower())
    if len(ll) == 1:
        return ll[0]

    for word in ll:
        token_text += word + ', '
    return token_text


df['is_RT'] = df['content'].apply(lambda x: True if x[:2] == 'RT' else False)
df['hashtags'] = df['content'].apply(lambda x: list_to_text(x, hashtag_tokenizer))
df['mentions'] = df['content'].apply(lambda x: list_to_text(x, user_tokenizer))


# Write to OLAP
# df['created_at'] = df['created_at'].astype('object')
# df['is_RT'] = df['is_RT'].astype('object')
df.to_sql('trump_OLAP', con=engine, if_exists='replace')
