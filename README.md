Task-Manager
============

Kafka-based task queue management

# Preparation
1. install zookeeper
2. install kafka
3. start kafka service `kafka-server-start /usr/local/etc/kafka/server.properties`
4. create topics in kafka through `kafka-python` package
```python
    
from kafka.admin import KafkaAdminClient, NewTopic
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

topic_list = []
topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
```

or through CMD
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions <partition number> --topic <your topic name>
```
