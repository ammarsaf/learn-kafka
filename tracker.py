from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092", 
    "group.id":"order-tracker", 
    "auto.offset.reset":"earliest"

}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("👂🏻 Listening for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error : {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 Recieved order: {order["quantity"]} x {order['user']} from user {order["user"]}")
except KeyboardInterrupt as kbe:
    print("\n Keyboard interrupted. Stopping cosumer.")

finally:
    consumer.close()