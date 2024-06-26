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
coins = ["ADAUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"]

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
def insert_data(exchange, symbol, open_interest, timestamp):
    try:
        query = sql.SQL(
            "INSERT INTO {} (id, exchange, market, symbol, openinterest, timestamp) VALUES (uuid_generate_v4(), %s, %s, %s, %s, %s)").format(
            sql.Identifier(table_name))
        cursor.execute(query, (exchange, "LINEAR", symbol, open_interest, timestamp))
        conn.commit()
        #logger.info(f"Inserted data for {symbol}")
    except psycopg2.Error as e:
        logger.error(f"Failed to insert data for {symbol}: {e}")


# Fetch data from Binance Historical Open Interest for each coin
for symbol in coins:
    period = "1h"
    limit = "500"
    #Only the data of the latest 30 days is available.
    startTime="1708662644000" #23 Feb 2024
    endTime="1711081844000" #22 March 2024
    oi_data = requests.get(
        f'https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period={period}&limit={limit}&startTime={startTime}&endTime={endTime}')

    try:
        oi_json = oi_data.json()
        # Iterate through fetched data and insert into PostgreSQL
        for data in oi_json:
            insert_data("Binance", data['symbol'], float(data['sumOpenInterest']), data['timestamp'])
    except ValueError as e:
        logger.error(f"Failed to parse JSON for {symbol}: {e}")

# Close the cursor and connection
cursor.close()
conn.close()
