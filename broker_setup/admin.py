from http_status import Status
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource


class CreateKafkaTopic:
    def __init__(
        self, topic, num_partition, replication_factor, max_msg_KB, bootstrap_servers
    ):
        self.topic = topic
        self.num_partition = num_partition
        self.replication_factor = replication_factor
        self.max_msg_KB = max_msg_KB
        self.bootstrap_servers = bootstrap_servers
        self.config = {"bootstrap.servers": self.bootstrap_servers}

    def instantiate_admin(self):
        admin = AdminClient(self.config)
        return admin

    def topic_exists(self):
        admin = self.instantiate_admin()
        cluster_metadata = admin.list_topics()
        topics = cluster_metadata.topics.values()
        for t in topics:
            if t.topic == self.topic:
                return True
        return False

    def create_topic(self):
        topic_exists = self.topic_exists()
        if not topic_exists:
            admin = self.instantiate_admin()
            new_topic = NewTopic(
                self.topic,
                num_partitions=self.num_partition,
                replication_factor=self.replication_factor,
            )
            result_dict = admin.create_topics([new_topic])
            for topic, future in result_dict.items():
                try:
                    future.result()
                    print(f"Topic {topic} created")
                    return Status(201)
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
                    return Status(500)
        else:
            return Status(200)

    def get_current_max_msg_size_for_topic(self):
        admin = self.instantiate_admin()
        resource = ConfigResource("topic", self.topic)
        result_dict = admin.describe_configs([resource])
        config_entries = result_dict[resource].result()
        max_size = config_entries["max.message.bytes"]
        return max_size.value

    def set_max_msg_size_for_topic(self):
        admin = self.instantiate_admin()
        current_max_msg_size = self.get_current_max_msg_size_for_topic()
        if current_max_msg_size != str(self.max_msg_KB * 1024):
            config_dict = {"max.message.bytes": str(self.max_msg_KB * 1024)}
            resource = ConfigResource("topic", self.topic, config_dict)
            result_dict = admin.alter_configs([resource])
            result_dict[resource].result()
        return Status(200)
