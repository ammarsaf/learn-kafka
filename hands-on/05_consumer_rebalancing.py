"""
- eager rebalancing - happened whenever there is a change in 
group membership, subsc changes; whenever consumer leaves, added, dies

flow
- changes detected
- partition revokes, eveything stop
- everyone rejoin
- partitions redistributed, consumption resumes


modern way - cooperative-sticky
- 
"""

import sys
from confluent_kafka import Consumer
from config import KafkaConfig

name = sys.argv[1]
strategy = sys.argv[2] if len(sys.argv) > 2 else "range"

def on_assign(consumer, partitions):
    print(f"[{name}] ASSIGNED: {[p.partition for p in partitions]}")

def on_revoke(consumer, partitions):
    print(f"[{name}] REVOKED: {[p.partition for p in partitions]}")


KafkaConfig.server_port.update(
    {
        "group.id":f"lab-{strategy}", 
        "auto.offset.reset":"earliest", 
        "partition.assignment.strategy":strategy
    }
)
consumer = Consumer(
    KafkaConfig.server_port
)
consumer.subscribe(["orders"], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{name}] started with strategy={strategy}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        print(f"[{name}] p{msg.partition()} o{msg.offset()}: {msg.value().decode()}")

finally:
    consumer.close()