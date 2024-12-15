import csv
import time

import pulsar
import json

from pulsar_admin.p_admin import PulsarAdmin

# Configuration variables
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
ADMIN_URL = "http://localhost:8080"
TOPIC_NAME = "persistent://public/default/bitcoin-tweets"
CSV_FILE_PATH = "mbsa.csv"

def create_partitioned_topic(admin_url, topic_name, num_partitions):
    admin_client = PulsarAdmin(host="localhost", port=8080)
    admin_client.persistent_topics().create_partitioned_topic(topic_name, num_partitions)


def send_callback(message_id, exception):
    pass

# Send message to Pulsar asynchronously
def send_message_to_pulsar_async(producer, message):
    try:
        producer.send_async(message.encode('utf-8'), send_callback)
    except Exception as e:
        print(f"Failed to publish message: {e}")

# Produce messages from CSV to Pulsar
def produce_messages():
    start = time.time()
    try:
        # Initialize Pulsar client and producer
        client = pulsar.Client(PULSAR_SERVICE_URL)
        producer = client.create_producer(
            TOPIC_NAME,
            max_pending_messages=1,
            block_if_queue_full=True,
            batching_enabled=False
        )
        print("Connected to Pulsar.")

        # Read CSV and send messages to Pulsar
        with open(CSV_FILE_PATH, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if not all(key in row for key in ["text", "Sentiment", "Date"]):
                    print("Missing required keys in row. Skipping...")
                    continue

                message = json.dumps({
                    "tweet": row["text"],
                    "Sentiment": row["Sentiment"],
                    "Date": row["Date"]
                })

                send_message_to_pulsar_async(producer, message)

        finish = time.time()
        print(f"Time taken to publish messages: {finish - start:.2f} seconds")
        print("All messages published to Pulsar.")
    except FileNotFoundError:
        print(f"CSV file '{CSV_FILE_PATH}' not found. Please check the file path.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        client.close()

def main():
    try:
        # Step 2: Produce messages to Pulsar
        produce_messages()
    except Exception as e:
        print(f"Execution failed: {e}")

if __name__ == "__main__":
    main()
