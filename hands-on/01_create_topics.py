from confluent_kafka.admin import AdminClient, NewTopic


"""
Create topics
"""

admin = AdminClient({
    "bootstrap.servers":"localhost:9092"
})

future = admin.create_topics([
    NewTopic("orders", num_partitions=3)
])

future["orders"].result()
print("Topic 'orders' create with 3 partitions")

