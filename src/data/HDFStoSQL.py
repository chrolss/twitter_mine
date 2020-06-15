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
