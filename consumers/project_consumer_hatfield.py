"""
json_consumer_case.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
from collections import defaultdict
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

# Kafka Configuration
TOPIC = "project_json"
KAFKA_SERVER = "localhost:9092"

#####################################
# Date Storage
#####################################
#Total message counts per category 
message_counts = defaultdict(int) 

#####################################
# Consumer Setup
#####################################

consumer = KafkaConsumer(
TOPIC,
bootstrap_servers=KAFKA_SERVER,
value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#####################################
# Set up live visuals
#####################################

def process_messages():
    """Update the live chart with the latest subject counts."""
    print("Waiting for messages...")
    for message in consumer:
        data = message.value
        category = data.get("category", "other")
        # Increment the count for this category
    message_counts[category] += 1

    print(f"Updated message counts: {message_counts}")

def update_chrart():
    """Update the live chart with the latest subject counts."""
    plt.cla()
    categories = list(message_counts.keys())
    counts = list(message_counts.values())
# Define unique colors for categories
    colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'pink'][:len(categories)]
    plt.bar(categories, counts, color=colors)
    plt.xlabel("Categories")
    plt.ylabel("Number of Messages")
    plt.title("Real-Time Messages vs Category")
    plt.xticks(rotation=45)


#####################################
# Define main function for this module
#####################################
if __name__ == "__main__":
# Start Kafka consumer in the background
    import threading
    threading.Thread(target=process_messages, daemon=True).start()

# Start Matplotlib animation
fig = plt.figure()
ani = FuncAnimation(fig, update_chrart, interval=1000, cache_frame_data=False)
plt.show()
