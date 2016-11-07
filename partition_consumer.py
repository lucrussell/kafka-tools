"""
# Consume messages from a specific topic partition.
"""
from pykafka import KafkaClient
from pykafka.common import OffsetType

OFFSET_TYPE = OffsetType.LATEST
CONSUMER_GROUP = b'group_id'
TOPIC = b'my-topic'
PARTITION = 11
TEST_KAFKA_BROKER = 'my-broker:9092'


def main():
    read_kafka_messages()


def get_consumer():
    client = KafkaClient(hosts=TEST_KAFKA_BROKER)
    topic = client.topics[TOPIC]
    partition = topic.partitions[PARTITION]
    return topic.get_simple_consumer(
        consumer_group=CONSUMER_GROUP,
        auto_commit_enable=False,
        auto_offset_reset=OFFSET_TYPE,
        reset_offset_on_start=False,
        auto_start=True,
        partitions=[partition])
    return simple_consumer


def read_kafka_messages():
        consumer = get_consumer()

        try:
            for message in consumer:
                if message is not None:
                    print(
                        str(message.partition.id) + '||' +
                        str(message.offset) + '||' +
                        message.value.decode('utf-8'))
        except Exception as ex:
            consumer.stop()
            print(ex)


if __name__ == "__main__":
    main()
