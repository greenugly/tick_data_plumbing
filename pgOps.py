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

print(pd.read_sql_query('''select ordinal_position, column_name, data_type  
                     from information_schema.columns 
                     where table_name = 'trades'
                ''', engine).head())

pgconn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON trades("timestamp")')
pgconn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON orders("timestamp")')

df_trade = pd.read_sql_query('select * from "trades"', engine)
df_t = df_trade.set_index("timestamp")
df_orders = pd.read_sql_query('select * from "orders"', engine)
df_o = df_orders.set_index("timestamp")

df_combined = pd.merge(df_o, df_t, on='timestamp', how='outer')
df_combined.to_sql('combined', engine, if_exists='replace')

# engine_loco = get_engine(settings['pguser'],
#                    settings['pgpassword'],
#                    settings['pghost'],
#                    settings['pgport'],
#                    settings['pgdatabase'])


# pgconn = engine.connect()

del df_combined
gc.collect()
