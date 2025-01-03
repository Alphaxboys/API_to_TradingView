import redis
import random
import time

# Connect a random data feed to Redis to test the data pipeline from Redis Db to Tradingview
# Using Sorted Sets (ZSET) for efficient storage and querying
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
        timestamp = int(time.time())  # Use the current UNIX timestamp as the score

        # Use Sorted Sets to store data
        redis_client.zadd(symbol, {price: timestamp})
        
        print(f"Inserted: {symbol} - {price} (timestamp: {timestamp})")
        
        # Wait for a random interval between 0.5 to 2 seconds
        time.sleep(random.uniform(0.5, 2))

def fetch_latest_data(symbol, count=10):
    """Fetch the latest 'count' data points for a specific symbol."""
    # Use ZRANGE to get the latest `count` data points
    data = redis_client.zrevrange(symbol, 0, count - 1, withscores=True)
    return data

if __name__ == "__main__":
    print("Starting data insertion...")
    try:
        insert_random_data()
    except KeyboardInterrupt:
        print("Stopped by user.")
        # Example of how to fetch data
        symbol_to_fetch = "GOOGL"
        print(f"Latest data for {symbol_to_fetch}:")
        latest_data = fetch_latest_data(symbol_to_fetch, count=10)
        for price, timestamp in latest_data:
            print(f"Price: {price}, Timestamp: {timestamp}")
