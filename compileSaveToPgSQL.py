import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from credentials import postgresql as settings
import pandas as pd
from sqlalchemy import create_engine
import glob
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from credentials import postgresql as settings
import numpy as np
import gc

#pgconn = psycopg2.connect(
#                    host = settings['pghost'],
#                    database = settings['pgdatabase'],
#                    user = settings['pguser'],
#                    password = settings['pgpassword'],
#                    )

#pgcursor = pgconn.cursor()

#  https://stackoverflow.com/questions/34484066/create-a-postgres-database-using-python
#pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

#pgcursor.execute('DROP DATABASE IF EXISTS "2203BTCLunaDB"')
#pgcursor.execute('CREATE DATABASE "2203BTCLunaDB"')
#pgconn.close()

def get_engine(user, password, host, port, db):
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)


    return engine

engine = get_engine(settings['pguser'],
                    settings['pgpassword'],
                    settings['pghost'],
                    settings['pgport'],
                    settings['pgdatabase'])


pgconn = engine.connect()

# aggregate multiple csv into one
path_trades = r'C:\Python\PycharmProjects\pythonProject\datasets\binance\BTC\220322Luna\trades'
all_files_trades = glob.glob(path_trades + "/*.csv")

cols = ['timestamp','local_timestamp','id','side','price','amount']
table_trades = []

for filename_trades in all_files_trades:
    df_t = pd.read_csv(filename_trades, header=0, usecols=cols)
    table_trades.append(df_t)

df_trades = pd.concat(table_trades, ignore_index=False)

#use pandas to_sql() method to save the dataframe to a PostgreSQL table
#append : add new rows
df_trades.to_sql('trades', engine, if_exists='replace', index= False)

del df_trades
del df_t
del table_trades
gc.collect()


path_orders = r'C:\Python\PycharmProjects\pythonProject\datasets\binance\BTC\220322Luna\book_snapshot_25'
all_files_orders = glob.glob(path_orders + "/*.csv")

cols = ['timestamp','local_timestamp','asks[0].price','asks[0].amount','bids[0].price','bids[0].amount','asks[1].price','asks[1].amount','bids[1].price','bids[1].amount',
        'asks[2].price','asks[2].amount','bids[2].price','bids[2].amount','asks[3].price','asks[3].amount','bids[3].price','bids[3].amount',
        'asks[4].price','asks[4].amount','bids[4].price','bids[4].amount','asks[5].price','asks[5].amount','bids[5].price','bids[5].amount',
        'asks[6].price','asks[6].amount','bids[6].price','bids[6].amount','asks[7].price','asks[7].amount','bids[7].price','bids[7].amount',
        'asks[8].price','asks[8].amount','bids[8].price','bids[8].amount','asks[9].price','asks[9].amount','bids[9].price','bids[9].amount',
        'asks[10].price','asks[10].amount','bids[10].price','bids[10].amount','asks[11].price','asks[11].amount','bids[11].price','bids[11].amount',
        'asks[12].price','asks[12].amount','bids[12].price','bids[12].amount','asks[13].price','asks[13].amount','bids[13].price','bids[13].amount',
        'asks[14].price','asks[14].amount','bids[14].price','bids[14].amount','asks[15].price','asks[15].amount','bids[15].price','bids[15].amount',
        'asks[16].price','asks[16].amount','bids[16].price','bids[16].amount','asks[17].price','asks[17].amount','bids[17].price','bids[17].amount',
        'asks[18].price','asks[18].amount','bids[18].price','bids[18].amount','asks[19].price','asks[19].amount','bids[19].price','bids[19].amount',
        'asks[20].price','asks[20].amount','bids[20].price','bids[20].amount','asks[21].price','asks[21].amount','bids[21].price','bids[21].amount',
        'asks[22].price','asks[22].amount','bids[22].price','bids[22].amount','asks[23].price','asks[23].amount','bids[23].price','bids[23].amount',
        'asks[24].price','asks[24].amount','bids[24].price','bids[24].amount']
table_orders = []

for filename_orders in all_files_orders:
    table_orders = []
    for chunk in pd.read_csv(filename_orders, header=0, usecols=cols, chunksize=2000):
        #chunk.to_sql(name of SQL table, engine, if_exsists = "append/replace/fail")
        df_orders = chunk.to_sql('orders', engine, if_exists='append')
    del df_orders

gc.collect()
#engine.dispose()

#print(df_trades)

########microseconds = us
#df_trades['datetime'] = pd.to_datetime(df_trades['timestamp'], origin='unix', unit='us')





#df = pd.read_csv(r"C:\Python\PycharmProjects\pythonProject\datasets\binance\BTC\220322Luna\trades")


#cols = ['timestamp','local_timestamp','id','side','price','amount']
#table_trades = []

