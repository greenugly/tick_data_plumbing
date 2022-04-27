import asyncio
from tardis_client import TardisClient, Channel
import psycopg2
from psycopg2.extensions import AsIs

con = psycopg2.connect(
    host="localhost",
    database="2203LunaBTCDB",
    user="postgres",
    password="Guest#01",
    port="5432")

#asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#asyncio.run(tardisStream.py())
asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))

async def replay():
    tardis_client = TardisClient()
    #tardis_client = TardisClient("TD.izWw3WRlaGj-CNQm.5vEAUW4gD0tYXKu.GcvZE-5QOXIhmDy.896lsIsIYbX2G7L.k8U33CrpLbf4faz.7ytW")




    # replay method returns Async Generator
    # https://rickyhan.com/jekyll/update/2018/01/27/python36.html
    messages = tardis_client.replay(
        exchange="binance",
        from_date="2022-03-01",
        to_date="2023-03-02",
        filters=[
          Channel(name="trade", symbols=["BTCUSDT"]),
          Channel("orderBookL2", ["BTCUSDT"])
        ],
    )




    # this will print all trades and orderBookL2 messages for XBTUSD
    # and all trades for ETHUSD for bitmex exchange
    # between 2019-06-01T00:00:00.000Z and 2019-06-02T00:00:00.000Z
    #(whole first day of June 2019)
    async for local_timestamp, message in messages:
        # local timestamp is a Python datetime that marks timestamp
        # when given message has been received
        # message is a message object as provided by exchange real-time stream

        print(message)

asyncio.run(replay())
