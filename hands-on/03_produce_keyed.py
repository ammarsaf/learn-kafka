from confluent_kafka import Producer
from config import KafkaConfig
producer = Producer(
    KafkaConfig.server_port
)

def report(err, msg):
    print(f"key={msg.key().decode()} value={msg.value().decode()} "
          f"-> partition {msg.partition()}, offset P{msg.offset()}")

customers = [
    "alice", "bob", "james", "mike", "ali"
]

for i in range(15):
    customer = customers[i % 4] # distribute between partition
    producer.produce(
        "orders", 
        key=customer, 
        value=f"{customer}-order-{i}",
        callback=report
    )

producer.flush()