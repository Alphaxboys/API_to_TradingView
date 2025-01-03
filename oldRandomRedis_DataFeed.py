# Appends the generated symbol - price pair to a Redis list named stock_prices.
#                                               ------------ 

import redis
import random
import time


# Connect a random data feed to Redis to test the data pipeline from Redis Db to Tradingview

# Connect to Redis
redis_host = "127.0.0.1"
redis_port = 6379
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

# Define a list of symbols
symbols = ["GOOGL", "AAPL", "MSFT", "AMZN", "TSLA", "META"]

def generate_random_price():
    """Generate a random price between 100 and 1500."""
    return round(random.uniform(100, 1500), 2)

def insert_random_data():
    """Insert random symbol-price pairs into Redis continuously."""
    while True:
        symbol = random.choice(symbols)
        price = generate_random_price()
        
        # Store the data in a Redis list
        redis_client.rpush("stock_prices", f"{symbol} - {price}")
        
        print(f"Inserted: {symbol} - {price}")
        
        # Wait for a random interval between 0.5 to 2 seconds
        time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    print("Starting data insertion...")
    try:
        insert_random_data()
    except KeyboardInterrupt:
        print("Stopped by user.")
