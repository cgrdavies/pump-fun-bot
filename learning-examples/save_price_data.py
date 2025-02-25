import redis
import json
import csv
import time
import multiprocessing
from datetime import datetime, timedelta
import os

def collect_price_data(token_address):
    """Child process function to collect price data for a specific token"""
    print(f"[{token_address}] Starting price collection process")

    # Initialize Redis client for the child process
    redis_client = redis.Redis(host='localhost', port=6381, decode_responses=True)

    # Create directory for price data if it doesn't exist
    os.makedirs('price_data', exist_ok=True)

    # Setup CSV file
    filename = f'price_data/{token_address}_{int(time.time())}.csv'
    print(f"[{token_address}] Created CSV file: {filename}")

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        csvfile.flush()  # Flush header immediately
        print(f"[{token_address}] Wrote CSV header")

        # Subscribe to price updates for this token
        pubsub = redis_client.pubsub()
        channel = f'price_updates:{token_address}'
        pubsub.subscribe(channel)
        print(f"[{token_address}] Subscribed to channel: {channel}")

        # Calculate end time (2 hours from now)
        end_time = datetime.now() + timedelta(hours=2)
        print(f"[{token_address}] Will collect data until: {end_time}")

        # Listen for messages until timeout
        message_count = 0
        while datetime.now() < end_time:
            message = pubsub.get_message(timeout=1.0)
            if message and message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    print(f"[{token_address}] Received message: {data}")
                    writer.writerow([
                        data.get('timestamp', time.time()),
                        data.get('open', 0.0),
                        data.get('high', 0.0),
                        data.get('low', 0.0),
                        data.get('close', 0.0),
                        data.get('volume', 0.0)
                    ])
                    csvfile.flush()  # Ensure data is written to disk
                    message_count += 1

                except json.JSONDecodeError as e:
                    print(f"[{token_address}] Error decoding message: {str(e)}")
                    print(f"[{token_address}] Raw message data: {message['data']}")
                    continue

def main():
    # Initialize Redis client for the parent process
    redis_client = redis.Redis(host='localhost', port=6381, decode_responses=True)

    # Subscribe to migration messages
    pubsub = redis_client.pubsub()
    pubsub.subscribe('migrations')

    # Track active processes
    active_processes = {}

    print("Listening for migration events...")

    while True:
        message = pubsub.get_message(timeout=1.0)

        if message and message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                token_address = data.get('tokenAddress')

                if token_address:
                    print(f"Starting price collection for token: {token_address}")

                    # Start a new process for this token
                    process = multiprocessing.Process(
                        target=collect_price_data,
                        args=(token_address,)
                    )
                    process.start()

                    # Store process with its start time
                    active_processes[token_address] = {
                        'process': process,
                        'start_time': datetime.now()
                    }
            except json.JSONDecodeError:
                print("Error decoding migration message")
                continue

        # Check for completed processes
        current_time = datetime.now()
        completed_tokens = []

        for token, process_info in active_processes.items():
            if (current_time - process_info['start_time']).total_seconds() >= 7200:  # 2 hours
                process_info['process'].terminate()
                process_info['process'].join()

                # Publish cancellation message
                redis_client.publish('token_cancellation',
                                   json.dumps({'tokenAddress': token}))
                print(f"Completed price collection for token: {token}")
                completed_tokens.append(token)

        # Remove completed processes from tracking
        for token in completed_tokens:
            del active_processes[token]

if __name__ == "__main__":
    main()
