from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import time
import sys


class Consumer:
    def __init__(self, brokers):
        self.consumer = KafkaConsumer(
            # consumer group id, it is a string, you can named.
            group_id="my",
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            # kafka broker address
            bootstrap_servers=[brokers]
        )

    def consumer_data(self, topic, partition):
        my_partition = TopicPartition(topic=topic, partition=partition)
        self.consumer.assign([my_partition])

        print(f"consumer start position: {self.consumer.position(my_partition)}")

        try:
            while True:
                # poll msg from topic
                poll_num = self.consumer.poll(timeout_ms=1000, max_records=5)
                if poll_num == {}:
                    print("consumer poll is empty, will exit")
                    exit(1)
                for key, record in poll_num.items():
                    for message in record:
                        print(
                            f"{message.topic}:{message.partition}:{message.offset}: key={message.key} value={message.value}")

                try:
                    self.consumer.commit_async()
                    time.sleep(5)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
        finally:
            try:
                self.consumer.commit()
            finally:
                self.consumer.close()


def main(argv):
    # topic name
    topic = "Hello"
    partition = 0
    my_consumer = Consumer(argv)
    my_consumer.consumer_data(topic, partition)


if __name__ == '__main__':
    main(sys.argv[1])