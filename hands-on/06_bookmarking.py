"""
Problem: fail in the mid


enable.auto.commit=true; -- auto bookmark even though
- after ; will reprocess the process, duplicate
- before; skipsl no duplicate, message lost

"""
from confluent_kafka import Consumer
from config import KafkaConfig
import sys

name = sys.argv[1]

KafkaConfig.server_port.update(
    {
        "group.id":f"dup-lab", 
        "auto.offset.reset":"earliest", 
        "partition.assignment.strategy":"range", 
        "enable.auto.commit": False # manual bookmarks
    }
)
consumer = Consumer(
    KafkaConfig.server_port
)

consumer.subscribe(["orders"])

count = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        print(f"[{name}] p{msg.partition()} o{msg.offset()}: {msg.value().decode()}")
        count += 1
        if count % 5 == 0:
            consumer.commit()
            print(f"[{name}] -- committed -- ")

finally:
    consumer.close()
