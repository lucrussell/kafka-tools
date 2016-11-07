"""
# Publish messages from file to a specific topic partition.
"""
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner

TEST_KAFKA_BROKER = 'mybroker:9092'
TOPIC = b'my-topic'
PARTITION = 11


def send_file():
    client = KafkaClient(hosts=TEST_KAFKA_BROKER)
    topic = client.topics[TOPIC]
    producer = topic.get_producer(partitioner=HashingPartitioner(hash_func=simple_hash_func))
    count = 0

    with open('message_file.txt') as f:
        content = f.readlines()
        for line in content:
            if line.startswith("b'"):
                # cut the b' from the start and '\n from the end of the messages in the file
                # this allows pasting in messages to 'message_file' in the format received by consumers
                line = line[2:-2]
            key = str(PARTITION).encode('utf-8')
            producer.produce(line.encode('utf-8'), partition_key=key)
            count += 1
            print('sent ' + str(count))


def simple_hash_func(k):
    # This partition function will simply return the supplied key as integer, allowing us to direct messages to
    # a specific partition.
    return int(k)


if __name__ == "__main__":
    send_file()
