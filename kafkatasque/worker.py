__all__ = ['Worker']

import logging
import warnings
import threading
import _thread

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from typing import Optional, List, Callable

from .utils import (
    json_decode, repr_func, is_optional_type, is_optional,
    validated_setter_property
)
from .db import Tasker
from .job import Job
from .abc import TasqueABC


class Worker(TasqueABC):
    """Fetches messages from Kafka topics and processes them.

    :param consumer: kafka consumer instance.
    :type consumer: :doc:`kafka.KafkaConsumer`
    :param topics: names of the Kafka topics.
    :type topics: list | None
    :param deserializer: callable which takes a byte string and returns a
        deserialized :doc:`job <job>` object. If not set, ``json_decode``
        is used by default.
    :type deserializer: callable | None
    :param logger: logger for recording worker activities. If not set, logger
        named ``tasker.worker`` is used with default settings (you need to
        define your own formatters and handlers).
    :type logger: :doc:`logging.Logger` | None
    :param tasker: Tasker instance which controls the DB connection.
        If not set, then the task management functionality cannot be used.
    :type tasker: :doc:`.db.Tasker` | None

    **Example:**

    .. code-block:: python

        from kafka import KafkaConsumer
        from tasker import Worker

        # Set up a Kafka consumer. Group ID is required.
        consumer = KafkaConsumer(
            bootstrap_servers='127.0.0.1:9092',
            group_id='group'
        )

        # Set up a worker.
        worker = Worker(topics=['topic'], consumer=consumer)

        # Start the worker to process jobs.
        worker.start()
    """

    def __init__(self,
                 consumer: KafkaConsumer,
                 topics: Optional[List[str]] = None,
                 deserializer: Optional[Callable] = None,
                 logger: Optional[logging.Logger] = None,
                 tasker: Optional[Tasker] = None) -> None:
        self.consumer: KafkaConsumer = consumer
        self.topics: Optional[List[str]] = topics
        # if the consumer already subscribes some topics, then alert the user
        # that all those former topics will be unsubscribed and subscribe the
        # new ones
        self.topics_check()

        self.deserializer: Callable = deserializer or json_decode
        super(Worker, self).__init__(logger, tasker)

    @validated_setter_property
    def consumer(self, value) -> None:
        if not isinstance(value, KafkaConsumer):
            raise TypeError("Worker.consumer must be KafkaConsumer. "
                            "Received {}".format(type(value)))

    @validated_setter_property
    def topics(self, value) -> None:
        if not is_optional_type(value, list):
            raise TypeError("Worker.topics must be either list or None. "
                            "Received {}".format(type(value)))

    @validated_setter_property
    def deserializer(self, value) -> None:
        if not is_optional(value, callable(value)):
            raise TypeError("Worker.deserializer must be either callable "
                            "function or None. Received {}".format(type(value)))

    def topics_check(self) -> None:
        """Check if the consumer already subscribes the same topics than the
        worker. If not, warns the user that the consumer's topics are going to
        be updated to to the worker and remaining topics are going to be
        unsubscribed. New topics are going to be subscribed"""
        consumer_topics = self.consumer.subscription()
        if not self.topics and not consumer_topics:
            raise ValueError("Topics cannot be empty.")
        elif not consumer_topics:
            self.consumer.subscribe(self.topics)
        elif self.topics:
            warnings.warn("Consumer topics are now override with "
                          "{}".format(self.topics))
            self.consumer.unsubscribe()
            self.consumer.subscribe(self.topics)

    @property
    def hosts(self) -> str:
        """Return comma-separated Kafka hosts and ports string from consumer.

        :return: comma-separated Kafka hosts and ports.
        :rtype: str
        """
        return self.consumer.config['bootstrap_servers']

    @property
    def group(self) -> Optional[str]:
        """Return the Kafka consumer group ID.

        :return: kafka consumer group ID.
        :rtype: str | None
        """
        return self.consumer.config['group_id']

    def __repr__(self) -> str:
        """Return the string representation of the worker.

        :return: string representation of the worker.
        :rtype: str
        """
        return '{}(hosts={}, topics={}, group={})'.format(
            self.__class__.__name__, self.hosts, self.topics, self.group
        )

    def __del__(self) -> None:
        """Close the consumer."""
        try:
            self.consumer.close()
        except Exception as e:  # noinspection PyBroadException
            if __debug__:
                print('Failed to close consumer due to {}.'.format(str(e)))
            raise e

    def _execute_job(self, job: Job) -> None:
        """Execute the job.

        :param job: extracted job instance from kafka message
        :type job: .job.Job
        """
        # start the timer for controlling timeout
        timer = None
        if job.timeout:
            timer = threading.Timer(job.timeout, _thread.interrupt_main)
            timer.start()

        # if use database, then check the DB whether start the task.
        if job.use_db and self.tasker:
            execute_task = self.tasker.start_task(job.id)
            if not execute_task:
                self.logger.info('Job {} was not executed.'.format(job.id))
                return
        # result of the function execution
        res = None
        # if the function is successfully executed
        is_succeed = False
        try:
            # if job.func is None, then result should None too.
            if callable(job.func):
                res = job.func(job.value, *job.args, **job.kwargs)
        except KeyboardInterrupt:
            res = 'Job {} timed out or was interrupted'.format(job.id)
            self.logger.error(res)
        except Exception as err:
            self.logger.exception(
                'Job {} raised an exception: {}'.format(job.id, err))
            res = err
        else:
            is_succeed = True
            self.logger.info('Job {} returned: {}'.format(job.id, res))
        finally:
            if timer is not None:
                timer.cancel()
            # finish task
            if job.use_db and self.tasker:
                self.tasker.finish_task(job.id, is_succeed, str(res))

    def _process_message(self,
                         msg: ConsumerRecord) -> None:
        """De-serialize the message and execute the job.

        :param msg: Kafka message.
        :type msg: :doc:`kafka.consumer.fetcher.ConsumerRecord`
        """
        self.logger.info(
            'Processing Message(topic={}, partition={}, offset={}) ...'
            .format(msg.topic, msg.partition, msg.offset))
        try:
            job = self.deserializer(msg.value)
            job_repr = repr_func(job.func, job.value, *job.args, **job.kwargs)
        except Exception as err:
            err_msg = 'Invalid message content: {}'.format(err)
            self.logger.exception(err_msg)
            if __debug__:
                print(err_msg)
        else:
            # start the task
            if job.use_db and not self.tasker:
                raise ValueError(
                    "Related tasker was not provided, unable to use DB.")
            self.logger.info('Executing job {}: {}'.format(job.id, job_repr))
            self._execute_job(job)

    def start(self,
              commit_offsets: bool = True) -> None:
        """Start processing Kafka messages and executing jobs.

        :param commit_offsets: if set to True, consumer offsets are committed
            every time a message is processed (default: True).
        :type commit_offsets: bool
        """
        # if the group id is not set for the consumer, commit_offsets must be
        # false
        if commit_offsets and not self.group:
            raise ValueError(
                "If group id is None, commit_offsets must be False.")

        self.logger.info('Starting {} ...'.format(self))

        for msg in self.consumer:
            self._process_message(msg)

            if commit_offsets:
                self.consumer.commit()
