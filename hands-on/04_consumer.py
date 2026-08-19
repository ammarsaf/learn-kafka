"""
Consumer is the people
who consume the production (process)
from producer

For example 3 partitions

- 1 consumer -> consume all the partition
- 3 consumers -> consume parallely
- 5 consumers -> 3 working, 2 idle

* Sending heartbeat to coordinator to tell it stull alive
* one partition, one owner (consumer) (Kafka iron's law for safe guarantee reason)
* problem of increasing the consumer is it will be the hard ceilling as of
when the traffic increase, the count of the consumer then increase, hence not scalable
* solve using async, multithreading, batch processing
"""

import sys
from confluent_kafka import Consumer
from config import KafkaConfig

name = sys.argv[1]
KafkaConfig.server_port.update({"auto.offset.reset":"earliest", 
                                "group.id":"order-processors"})
consumer = Consumer(
    KafkaConfig.server_port
)

consumer.subscribe(["orders"])

print(f"[{name}] started")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print(f"[{name}] error: {msg.error()}")
            continue

        print(f"[{name}] partition {msg.partition()} offset {msg.offset()}"
              f"{msg.value().decode()}"
              )

finally:
    consumer.close()