import requests
import psycopg2
from psycopg2 import sql
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

host = "localhost"
port = "5432"
user = "postgres"
password = "pass"
database = "postgres"
table_name = "openinterest"
coins=["BTCUSD_PERP", "ETHUSD_PERP", "LINKUSD_PERP", "BNBUSD_PERP", "TRXUSD_PERP", "DOTUSD_PERP", "ADAUSD_PERP", "XRPUSD_PERP"]

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()
except psycopg2.Error as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit()


# Function to insert data into PostgreSQL
def insert_data(exchange, market, symbol, open_interest, timestamp):
    try:
        query = sql.SQL(
            "INSERT INTO {} (id, exchange, market, symbol, openinterest, timestamp) VALUES (uuid_generate_v4(), %s, %s, %s, %s, %s)").format(
            sql.Identifier(table_name))
        cursor.execute(query, (exchange, market, symbol, open_interest, timestamp))
        conn.commit()
        #logger.info(f"Inserted data for {symbol}")
    except psycopg2.Error as e:
        logger.error(f"Failed to insert data for {symbol}: {e}")

# Fetch data from Binance Coin-M Futures Open Interest Statistics for each coin
for coin in coins:
    pair = coin.split("_")[0]
    contract_type = "PERPETUAL"
    market = "INVERSE"
    period = "1h"
    limit = "500"
    startTime="1708662644000" #23 Feb 2024
    endTime="1711081844000" #22 March 2024
    oi_data = requests.get(
        f'https://dapi.binance.com/futures/data/openInterestHist?pair={pair}&contractType={contract_type}&period={period}&limit={limit}&startTime={startTime}&endTime={endTime}')

    try:
        oi_json = oi_data.json()
        # print(oi_json)
        # Iterate through fetched data and insert into PostgreSQL
        for data in oi_json:
            insert_data("Binance", market, coin, float(data['sumOpenInterest']), data['timestamp'])
    except ValueError as e:
        logger.error(f"Failed to parse JSON for {coin}: {e}")


# Close the cursor and connection
cursor.close()
conn.close()
