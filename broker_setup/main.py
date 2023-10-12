import os
from admin import CreateKafkaTopic
from dotenv import load_dotenv

load_dotenv()

ckt = CreateKafkaTopic(
    topic=os.getenv("TOPIC"),
    num_partition=int(os.getenv("NUM_PARTITION")),
    replication_factor=int(os.getenv("REPLICATION_FACTOR")),
    max_msg_KB=int(os.getenv("MAX_MSG_KB")),
    bootstrap_servers=os.environ.get("KAFKA_BROKERCONNECT"),
)

topic = ckt.create_topic()

print(topic.code)

max_msg_size = ckt.set_max_msg_size_for_topic()

print(max_msg_size.code)
