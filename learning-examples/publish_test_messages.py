import redis
import json
import time
import random
import asyncio

async def publish_test_data():
    # Initialize Redis client
    redis_client = redis.Redis(host='localhost', port=6381, decode_responses=True)

    # Publish migration message
    test_token = '2JkBLfWm3fdyoGZHPGvq3hmFMFiam8LCk33K8XFwpump'
    migration_data = {
        "tokenAddress": test_token,
        "liquidityAddress": "8b4MrQXkvZz8JTWm1H5aGCYoP4UNw5mWZ8JwoxhqaDoN",
    }

    print(f"\nPublishing migration event for token: {test_token}")
    redis_client.publish('migrations', json.dumps(migration_data))

    print("\nFinished publishing test messages")
    print(f"Token Address: {test_token}")
    print("Check the price_data directory for the CSV file")

if __name__ == "__main__":
    asyncio.run(publish_test_data())
