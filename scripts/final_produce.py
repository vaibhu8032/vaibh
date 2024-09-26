from time import time, sleep
from kafka import KafkaProducer,KafkaConsumer
from kafka.errors import KafkaError
import json
import random
from datetime import datetime
import pytz

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Kafka topics
topics = ['t1', 't2', 't3']

# Result topic
recon_topic = 'recon'
IST = pytz.timezone('Asia/Kolkata')

# Function to generate random data
def generate_data():
    current_time = datetime.now(IST)
    return {
        'timestamp': current_time.timestamp(),
        'value': random.randint(0, 100)
    }

# Function to send data to Kafka topic
def send_data(topic, data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    try:
        producer.send(topic, data)
        producer.flush()
        print(f"data produced to topic {topic}:{data}")
    except KafkaError as e:
        print(f"Failed to send message to Kafka topic {topic}: {e}")
    finally:
        producer.close()

# Function to calculate and send recon data
def send_recon_data(start_time, end_time, topic, count):
    recon_data = {
        'topic': topic,
        'start_time': start_time,
        'end_time': end_time,
        'counts': count
    }
    send_data(recon_topic, recon_data)

 # Function to consume messages from Kafka topic and get count
# def get_message_count(topic):
#     consumer = KafkaConsumer(topic,
#                              bootstrap_servers=bootstrap_servers,
#                              auto_offset_reset='earliest',
#                              enable_auto_commit=True,
#                              group_id='count_group',
#                              value_deserializer=lambda x: json.loads(x.decode('utf-8')))

#     message_count = sum(1 for _ in consumer)
#     consumer.close()

#     return message_count
    

# Main function
def main():
    counts = {topic: 0 for topic in topics}
    start_time = time()
    end_time = start_time + 30  # 30 seconds

    while time() < end_time:
        for topic in topics:
                data = generate_data()
                send_data(topic, data)
                counts[topic] += 1
                sleep(0.5)

            

         # Wait for 1 second

    # Calculate recon data
    for topic,count in counts.items():
            send_recon_data(start_time, end_time, topic, count)  # 30 values sent per topic

if __name__ == "__main__":
    main()
