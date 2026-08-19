from confluent_kafka import Producer
from config import KafkaConfig


producer = Producer(
    KafkaConfig.server_port
)

def report(err, msg):
    print(f"Message {msg.value().decode()} -> partition {msg.partition()}, offset {msg.offset()}")


for i in range(10):
    producer.produce("orders", value=f'Order-{i}', callback=report)

producer.flush()