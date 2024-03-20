import requests
from confluent_kafka import Producer

# Configure Kafka producer
conf = {'bootstrap.servers': 'broker:9092'}  # Assuming Kafka broker is running locally
producer = Producer(conf)

# Define Kafka topic
topic = 'latest_events'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), 'partition', msg.partition())

# Fetch data from URL and publish to Kafka topic
def fetch_and_publish_data(url, producer_instance, topic_name):
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            # Publish data to Kafka topic
            producer_instance.poll(0)
            producer_instance.produce(topic_name, value=line, callback=delivery_report)
            producer_instance.flush()

if __name__ == '__main__':
    # URL to fetch data from
    data_url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    # Fetch data from URL and publish to Kafka topic
    fetch_and_publish_data(data_url, producer, topic)

