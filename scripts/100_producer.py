from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import random
from datetime import datetime, timedelta
import pytz

# Redpanda broker address
bootstrap_servers = ['localhost:9092']

# Redpanda topics from t1 to t100
topics = [f't{i}' for i in range(1, 101)]

# Result topic
recon_topic = 'recon'

IST = pytz.timezone('Asia/Kolkata')
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Function to generate random data
def generate_data():
    current_time = datetime.now(IST)
    iso_time = current_time.isoformat()
    return {
        'timestamp': iso_time,
        'value': random.randint(0, 100)
    }


# Function to send data to topic
def send_data(topic, data):
    try:
        producer.send(topic,data)  # Specify the topic when sending data
        producer.flush()
        print(f"data produced to topic {topic}: {data}")
    except KafkaError as e:
        print(f"Failed to send message to Kafka topic {topic}: {e}")
    finally:
        # Close the producer connection
        producer.close()
        #print("done")
        
       
# Function to calculate and send recon data
def send_recon_data(start_time, end_time, topic, count):
    recon_data = {
        'topic': topic,
        'start_time': start_time,
        'end_time': end_time,
        'counts': count
    }
    send_data(recon_topic, recon_data)

# Main function
def main():
    
    counts = {topic: 0 for topic in topics}
    current_time = datetime.now(IST)
    start_time = current_time.isoformat()
    start_time_dt = datetime.fromisoformat(start_time)

    # Add 30 seconds to start_time
    end_time_dt = start_time_dt + timedelta(seconds=30)

    # Convert end_time back to ISO formatted string
    end_time = end_time_dt.isoformat()

    while datetime.now(IST) < end_time_dt:
        for topic in topics:
            data = generate_data()
            send_data(topic, data)  # Send data to the corresponding topic
            counts[topic] += 1
            sleep(0.5)
             
    # Calculate recon data
    for topic, count in counts.items():
        send_recon_data(start_time, end_time, topic, count)
    producer.close() 
if __name__ == "__main__":
    main()
