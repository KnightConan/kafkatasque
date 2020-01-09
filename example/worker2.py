import logging

from kafka import KafkaConsumer
from task_manager import Worker
from core.views import tm

# Set up logging.
formatter = logging.Formatter('[%(levelname)s] %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger('task_manager.worker')
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

# Set up a Kafka consumer.
consumer = KafkaConsumer(
    bootstrap_servers='127.0.0.1:9092',
    group_id='slaves',
)

# Set up a worker.
worker = Worker(topics=['example_topic'], consumer=consumer, task_manager=tm)
worker.start()
