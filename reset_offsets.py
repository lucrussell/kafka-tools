"""
# Reset a group/topic/partition to an offset.
"""
from pykafka import KafkaClient
from pykafka.common import OffsetType

OFFSET_TYPE = OffsetType.LATEST
CONSUMER_GROUP = b'group_id'
TOPIC = b'my-topic'
PARTITION = 11
TEST_KAFKA_BROKER = 'my-broker:9092'
OFFSET = 100


def reset_offsets(offset):
    simple_consumer = get_consumer()
    partition_offsets = {
        p: offset for p in simple_consumer.partitions.values()}
    print('resetting offset to '+str(offset))
    simple_consumer.reset_offsets(partition_offsets.items())
    simple_consumer.commit_offsets()


def get_consumer():
    client = KafkaClient(hosts=TEST_KAFKA_BROKER)
    print(TEST_KAFKA_BROKER)
    print('Topic: {0}'.format(TOPIC))

    topic = client.topics[TOPIC]
    partition = topic.partitions[PARTITION]
    simple_consumer = topic.get_simple_consumer(
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset=OFFSET_TYPE,
        reset_offset_on_start=False,
        auto_start=True,
        partitions=[partition])
    return simple_consumer

if __name__ == "__main__":
    reset_offsets(OFFSET)
