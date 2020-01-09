__all__ = ['Queue']

import logging

from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from typing import Callable, Optional, Any

from .job import Job
from .utils import is_optional_type, validated_setter_property
from .db import Tasker
from .abc import QueueABC, EnqueueAgent, TypeEnqueue


class DefaultEnqueueAgent(EnqueueAgent):
    """Default Enqueue class using Job class the encapsulate the task
    information.

    :param producer: Instance of KafkaProducer to send the job to Kafka brokers.
    :type producer: :doc:`kafka.KafkaProducer`
    :param serializer: serializer function
    :type serializer: callable
    :param logger: logger instance
    :type logger: :doc:`logging.Logger`
    :param tasker: instance of Tasker. If defined, then use the
        database to store the job information; if None, then don't use DB.
    :type tasker: :doc:`.db.Tasker` | None
    """
    def __init__(self,
                 producer: KafkaProducer,
                 serializer: Optional[Callable] = None,
                 logger: Optional[logging.Logger] = None,
                 tasker: Optional[Tasker] = None) -> None:
        super(DefaultEnqueueAgent, self).__init__(producer, serializer, logger,
                                                  tasker)

    def create_job(self,
                   topic: str,
                   content: Any,
                   key: Optional[str] = None,
                   partition: Optional[int] = None,
                   use_db: bool = False,
                   func: Optional[Callable] = None,
                   timeout: int = 0,
                   job_id: Optional[str] = None,
                   *args: Any,
                   **kwargs: Any) -> Job:
        """Create a new job instance with the given parameters.

        :param topic: name of the kafka topic
        :type topic: str
        :param content: value of the kafka message
        :type content: str
        :param key: key of the kafka message
        :type key: str | None
        :param partition: specific partition of the kafka topic. If not set, let
            kafka decide.
        :type partition: int | None
        :param use_db: whether use the database to store the job information. if
            not set, False by default.
        :type use_db: bool
        :param func: function. Must be serializable and available to
            :doc:`workers <worker>`.
        :type func: callable | None
        :param timeout: timeout threshold for the function call.
        :type timeout: int
        :param job_id: id of the job, if not given, a new one will be created.
        :type job_id: str | None
        :param args: positional arguments for the function.
        :param kwargs: keyword arguments for the function.
        :return: created job instance
        :rtype: :doc:`.job.Job`
        """
        self.logger.info(
            'Creating Job for topic [{}] with content [{}] ...'.format(topic,
                                                                       content))
        job = Job(
            job_id=job_id,
            topic=topic,
            key=key,
            partition=partition,
            value=content,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            use_db=use_db
        )
        return job

    def send(self,
             job: Job) -> FutureRecordMetadata:
        """Use the kafka producer to send the message.

        :param job: job instance to send
        :type job: :doc:`.job.Job`
        :return: RecordMetadata instance
        :rtype: :doc:`kafka.producer.future.RecordMetadata`
        """
        self.logger.info('Enqueueing {} ...'.format(job))
        # return RecordMetadata, to get the partition info can through
        # RecordMetadata.value.partition
        return self.producer.send(
            job.topic,
            value=self.serializer(job),
            key=self.serializer(job.key) if job.key else None,
            partition=job.partition,
            timestamp_ms=job.timestamp
        )

    def enqueue(self,
                topic: str,
                content: Any,
                key: Optional[str] = None,
                partition: Optional[int] = None,
                use_db: bool = False,
                func: Optional[Callable] = None,
                timeout: int = 0,
                *args: Any,
                **kwargs: Any) -> FutureRecordMetadata:
        """Enqueue message or a :doc:`job <job>`.

        :param topic: name of the kafka topic
        :type topic: str
        :param content: value of the kafka message
        :type content: str
        :param key: key of the kafka message
        :type key: str | None
        :param partition: specific partition of the kafka topic. If not set, let
            kafka decide.
        :type partition: int | None
        :param use_db: whether use the database to store the job information. if
            not set, False by default.
        :type use_db: bool
        :param func: function. Must be serializable and available to
            :doc:`workers <worker>`.
        :type func: callable | None
        :param timeout: timeout threshold for the function call.
        :type timeout: int
        :param args: positional arguments for the function.
        :param kwargs: keyword arguments for the function.
        :return: RecordMetadata instance
        :rtype: :doc:`kafka.producer.future.RecordMetadata`
        """
        # if the content is a job instance, no need to create a new job instance
        if isinstance(content, Job):
            job = content
        else:
            job = self.create_job(topic, content, key, partition,
                                  use_db, func, timeout, *args, **kwargs)
        # if use database, the tasker must not be None
        if job.use_db and not self.tasker:
            raise ValueError("Related Tasker was not provided, unable to use "
                             "DB.")
        elif job.use_db and self.tasker:
            self.logger.info('Saving {} to DB...'.format(job))
            self.tasker.create_task(job)
        return self.send(job)


class Queue(QueueABC):
    """Enqueues message in Kafka topics as :doc:`jobs <job>`.

    :param producer: kafka producer instance.
    :type producer: kafka.KafkaProducer
    :param serializer: function which takes a :doc:`job <job>` instance and
        returns a serialized byte string. If not set, ``json_encode`` is used
        by default.
    :type serializer: callable | None
    :param logger: logger for recording queue activities. If not set, logger
        named ``tasker.queue`` is used with default settings.
    :type logger: :doc:`logging.Logger` | None
    :param enqueue_agent: instance of subclass of Enqueue class. If net set,
        :doc:`DefaultEnqueueAgent` will be used by default.
    :type enqueue_agent: :doc:`Enqueue` | None
    :param tasker: Tasker instance which controls the DB connection.
        If not set, then the task management functionality cannot be used.
    :type tasker: :doc:`.db.Tasker` | None

    **Example:**

    .. code-block:: python

        import requests

        from kafka import KafkaProducer
        from tasker import Queue

        # Set up a Kafka producer.
        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

        # Set up a queue.
        queue = Queue(producer=producer)

        # Enqueue a function call.
        queue.enqueue('example', 'https://www.google.com/', func=requests.get)
    """

    def __init__(self,
                 producer: KafkaProducer,
                 serializer: Optional[Callable] = None,
                 logger: Optional[logging.Logger] = None,
                 enqueue_agent: Optional[TypeEnqueue] = None,
                 tasker: Optional[Tasker] = None) -> None:
        super(Queue, self).__init__(producer, serializer, logger, tasker)
        self.enqueue_agent: TypeEnqueue = enqueue_agent or DefaultEnqueueAgent(
            producer=self.producer,
            serializer=self.serializer,
            logger=self.logger,
            tasker=self.tasker
        )
        self.hosts: str = producer.config['bootstrap_servers']

    @validated_setter_property
    def enqueue_agent(self, value) -> None:
        if not is_optional_type(value, EnqueueAgent):
            raise TypeError("Queue.enqueue_agent must be either EnqueueAgent "
                            "or None. Received {}".format(type(value)))

    def __repr__(self) -> str:
        """Return the string representation of the queue.

        :return: string representation of the queue.
        :rtype: str
        """
        return 'Queue(hosts="{}")'.format(self.hosts)

    def __del__(self) -> None:
        """Close the producer."""
        try:
            self.producer.close()
        except Exception as e:  # noinspection PyBroadException
            if __debug__:
                print('Failed to close producer due to ({}).'.format(str(e)))
            raise e

    def enqueue(self,
                topic: str,
                content: Any,
                key: Optional[str] = None,
                partition: Optional[int] = None,
                use_db: bool = False,
                func: Optional[Callable] = None,
                timeout: int = 0,
                *args: Any,
                **kwargs: Any) -> FutureRecordMetadata:
        """Enqueue message or a :doc:`job <job>`.

        :param topic: name of the kafka topic
        :type topic: str
        :param content: value of the kafka message
        :type content: str
        :param key: key of the kafka message
        :type key: str | None
        :param partition: specific partition of the kafka topic. If not set, let
            kafka decide.
        :type partition: int | None
        :param use_db: whether use the database to store the job information. if
            not set, False by default.
        :type use_db: bool
        :param func: function. Must be serializable and available to
            :doc:`workers <worker>`.
        :type func: callable | None
        :param timeout: timeout threshold for the function call.
        :type timeout: int
        :param args: positional arguments for the function.
        :param kwargs: keyword arguments for the function.
        :return: RecordMetadata instance
        :rtype: :doc:`kafka.producer.future.RecordMetadata`

        **Example:**

        .. code-block:: python

            import requests

            from kafka import KafkaProducer
            from tasker import Queue

            # Set up a Kafka producer.
            producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

            # Set up a queue service.
            queue = Queue(producer=producer)

            # Enqueue a function call.
            queue.enqueue('example', 'https://www.google.com/',
                          func=requests.get)
        """
        return self.enqueue_agent.enqueue(
            topic=topic,
            content=content,
            key=key,
            partition=partition,
            use_db=use_db,
            func=func,
            timeout=timeout,
            *args,
            **kwargs
        )
