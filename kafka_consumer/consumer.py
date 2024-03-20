from confluent_kafka import Consumer, KafkaError

# Configure Kafka consumer
conf = {'bootstrap.servers': 'broker:9092', 'group.id': 'my_consumer_group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['latest_events'])

# Consume data from Kafka topic
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print('Consumed message: {}'.format(msg.value().decode('utf-8')), msg)

consumer.close()

