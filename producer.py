from confluent_kafka import Producer
import uuid
import json

# if you have event, send to this server
producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

# Event
order = {
    "order_id": str(uuid.uuid4()),
    "user": "jack",
    "item":"pizza", 
    "quantity": 10
} 


value = json.dumps(order).encode("utf-8") # need bytes, kafka understand this

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivery succeed: {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : {msg.partition()} :  at offset {msg.offset()}")
        


producer.produce(topic="orders", 
                 value=value,
                 callback=delivery_report) # take the value, assign to topic order

producer.flush()

# so, we need a producer to use kafka
# which can be consider such, user input, real time data etc
