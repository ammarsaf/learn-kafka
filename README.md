# learn-kafka

# Concept

- an event-driven architecture communcation between function

# Getting Started

## Kafka

- Consist of
  1. Broker
  - basically a server dedicated for Kafka purpose
  - every broker has id
  - role
    - Controller
      - cluster state management
      - tracking broker leader
      - reassign partition case of broker failer
      - handling all cluster admin task
  2. Producer
  - Create **topic**
  3. Consumer
  - Subscribe **topic**
