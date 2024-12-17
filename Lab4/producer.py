import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError


def main(argv):
    # kafka broker address
    producer = KafkaProducer(bootstrap_servers=[argv])

    for _ in range(100):
        # send msg to topic Hello
        future = producer.send("Hello", b'msg')
        try:
            record_metadata = future.get(timeout=10)
            print(record_metadata)
        except KafkaError as e:
            print(e)
    producer.flush()


if __name__ == '__main__':
    main(sys.argv[1])