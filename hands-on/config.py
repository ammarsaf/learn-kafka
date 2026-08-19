from dataclasses import dataclass

@dataclass
class KafkaConfig:
    server_port = {"bootstrap.servers": "localhost:9092"}