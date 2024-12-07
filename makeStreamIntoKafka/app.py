from time import sleep
from kafka import KafkaProducer

def read_file_and_send_to_kafka(file_path, topic, bootstrap_servers):
    print("sleepingggg........................................................")
    sleep(15)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: v.encode('utf-8'))
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for i in range(0, len(lines), 5):
                chunk = lines[i:i+5]
                for line in chunk:
                    producer.send(topic, value=line.strip())
                producer.flush()
                print(f"Sent {len(chunk)} lines to Kafka topic '{topic}'.")
                sleep(5) 
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    FILE_PATH = "data.txt"  # Replace with your file name
    TOPIC = "mytopic"      # Replace with your Kafka topic name
    BOOTSTRAP_SERVERS = "kafka:9092"  # Replace with your Kafka broker address
    read_file_and_send_to_kafka(FILE_PATH, TOPIC, BOOTSTRAP_SERVERS)
