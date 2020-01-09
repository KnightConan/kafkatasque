__all__ = ['Job']

import uuid
import time

from collections.abc import Sequence
from typing import Callable, Optional, Any, TypeVar

from .utils import (
    validate_uuid4, is_optional, is_optional_type, validated_setter_property
)


TypeJob = TypeVar('TypeJob', bound='Job')


class Job:
    """Job for the queue service.

    It contains the information of a kafka producer needs to send a message.

    :param topic: kafka topic for the job
    :type topic: str
    :param value: content of the message, it should be a nonempty string. If the
        `func` is passed, then this value will be the first parameter applied to
        the function in worker.
    :type value: str
    :param job_id: id of this job.
    :type job_id: str | None
    :param timestamp: created time
    :type timestamp: int | None
    :param key: kafka message key. (optional)
    :type key: str | None
    :param partition: specific partition of this message to send. (optional)
        If not given, then let kafka decide.
    :type partition: int | None
    :param func: message process function. (optional)
    :type func: callable | None
    :param args: positional parameters for the function call to process the
        message. (optional)
    :type args: list | tuple | None
    :param kwargs: keyword parameters for the function call to process the
        message. (optional)
    :type kwargs: dict | None
    :param timeout: timeout threshold for executing the process function. The
        default value is 0.
    :type timeout: int
    :param use_db: If True, use the database to store the job information. The
        default value is False.
    :type use_db: bool
    """
    def __init__(self,
                 topic: str,
                 value: Any,
                 job_id: Optional[str] = None,
                 timestamp: Optional[int] = None,
                 key: Optional[str] = None,
                 partition: Optional[int] = None,
                 func: Optional[Callable] = None,
                 args: Any = None,
                 kwargs: Any = None,
                 timeout: int = 0,
                 use_db: bool = False):
        self.timestamp: int = timestamp or int(time.time() * 1000)
        self.id: str = job_id or uuid.uuid4().hex
        self.key: Optional[str] = key
        self.value: Any = value
        self.partition: Optional[int] = partition
        self.topic: str = topic
        self.func: Optional[Callable] = func
        self.args: Any = args
        self.kwargs: Any = kwargs
        self.timeout: int = timeout
        self.use_db: bool = use_db

    @validated_setter_property
    def topic(self, value):
        if not isinstance(value, str):
            raise TypeError("Job.topic must be str. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def id(self, value):
        if not is_optional_type(value, str, validate_uuid4(value)):
            raise TypeError("Job.id must be either UUID or None. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def timestamp(self, value):
        if not is_optional_type(value, int):
            raise TypeError("Job.timestamp must be either an int or None. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def key(self, value):
        if not is_optional_type(value, str):
            raise TypeError("Job.key must be either string or None. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def partition(self, value):
        if not is_optional_type(value, int):
            raise TypeError("Job.partition must be either int or None. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def func(self, value):
        if not is_optional(value, callable(value)):
            raise TypeError("Job.func must be either None or callable method. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def args(self, value):
        if not is_optional_type(value, Sequence):
            raise TypeError("Job.args must be either None or Sequence. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def kwargs(self, value):
        if not is_optional_type(value, dict):
            raise TypeError("Job.kwargs must be either None or dictionary. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def timeout(self, value):
        if not is_optional_type(value, int):
            raise TypeError("Job.timeout must be int. "
                            "Received {}.".format(type(value)))

    @validated_setter_property
    def use_db(self, value):
        if not is_optional_type(value, bool):
            raise TypeError("Job.use_db must be bool. "
                            "Received {}.".format(type(value)))
