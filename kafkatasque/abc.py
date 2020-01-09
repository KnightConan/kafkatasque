import logging
import abc
from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from typing import Optional, Callable, TypeVar, Any
from .utils import (
    json_encode, is_optional, is_optional_type, validated_setter_property
)
from .db import Tasker
from .job import TypeJob


TypeEnqueue = TypeVar('TypeEnqueue', bound='EnqueueAgent')


class TasqueABC:
    """Abstract class for having logger and tasker as its variables."""
    def __init__(self,
                 logger: Optional[logging.Logger] = None,
                 tasker: Optional[Tasker] = None) -> None:
        self.logger: logging.Logger = logger or \
            logging.getLogger('kafkatasque.{}'.format(self.__class__.__name__))
        self.tasker: Optional[Tasker] = tasker

    @validated_setter_property
    def logger(self,
               value: Optional[logging.Logger]) -> None:
        """logger value type check function.

        :param value: new value of the logger.
        :type value: logging.Logger | None
        :return: if the value's type doesn't match the expectation, raises error
        :rtype: None
        """
        if not is_optional_type(value, logging.Logger):
            raise TypeError("{}.logger must be either logging.Logger or None."
                            " Received {}".format(self.__class__.__name__,
                                                  type(value)))

    @validated_setter_property
    def tasker(self,
               value: Optional[Tasker]) -> None:
        """tasker value type check function.

        :param value: new value of the tasker.
        :type value: db.Tasker | None
        :return: if the value's type doesn't match the expectation, raises error
        :rtype: None
        """
        if not is_optional_type(value, Tasker):
            raise TypeError("{}.tasker must be either Tasker or None. "
                            "Received {}".format(self.__class__.__name__,
                                                 type(value)))


class QueueABC(TasqueABC):
    """Abstract class for Queue classes which haves producer, serializer, logger
     and tasker as its variables."""
    def __init__(self,
                 producer: KafkaProducer,
                 serializer: Optional[Callable] = None,
                 logger: Optional[logging.Logger] = None,
                 tasker: Optional[Tasker] = None) -> None:
        self.producer: KafkaProducer = producer
        self.serializer: Callable = serializer or json_encode
        super(QueueABC, self).__init__(logger, tasker)

    @validated_setter_property
    def producer(self,
                 value: KafkaProducer) -> None:
        """producer value type check function.

        :param value: new value of the producer.
        :type value: kafka.KafkaProducer
        :return: if the value's type doesn't match the expectation, raises error
        :rtype: None
        """
        if not isinstance(value, KafkaProducer):
            raise TypeError("{}.producer must be KafkaProducer. "
                            "Received {}".format(self.__class__.__name__,
                                                 type(value)))

    @validated_setter_property
    def serializer(self,
                   value: Optional[Callable]) -> None:
        """serializer value type check function.

        :param value: new value of the serializer.
        :type value: Callable | None
        :return: if the value's type doesn't match the expectation, raises error
        :rtype: None
        """
        if not is_optional(value, callable(value)):
            raise TypeError(
                "{}.serializer must be either callable function or None. "
                "Received {}".format(self.__class__.__name__, type(value)))


class EnqueueAgent(QueueABC):
    """Abstract class the force the subclass implement the required functions
    with specific parameters, such that the worker won't be broken."""
    @abc.abstractmethod
    def create_job(self,
                   topic: str,
                   content: Any,
                   key: Optional[str],
                   partition: Optional[int],
                   use_db: bool,
                   func: Optional[Callable],
                   timeout: int,
                   job_id: Optional[str] = None,
                   *args: Any,
                   **kwargs: Any) -> TypeJob: pass

    @abc.abstractmethod
    def send(self,
             job: TypeJob) -> FutureRecordMetadata: pass

    @abc.abstractmethod
    def enqueue(self,
                topic: str,
                content: Any,
                key: Optional[str] = None,
                partition: Optional[int] = None,
                use_db: bool = False,
                func: Optional[Callable] = None,
                timeout: int = 0,
                *args: Any,
                **kwargs: Any) -> FutureRecordMetadata: pass
