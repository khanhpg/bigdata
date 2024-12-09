from kafka import KafkaConsumer
import json

# Khởi tạo consumer
consumer = KafkaConsumer(
    "example_topic",
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    group_id="group1",
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def receive_data():
    try:
        print("Consumer 1 started - Processing all messages")
        for message in consumer:
            data = message.value
            print(f"Consumer 1 received: {data}")
    except KeyboardInterrupt:
        consumer.close()
        print("\nConsumer 1 stopped")

if __name__ == "__main__":
    receive_data()