"""This module contains all the constants used in the TaskMixin class."""

from enum import Enum


class Status(Enum):
    """Statuses of the task in DB"""
    active = 0
    waiting = 1
    succeed = 2
    failed = 3
    postponed = 4
    canceled = 5
    recovered = 6


class Action(Enum):
    """Actions for the task"""
    postpone = 0
    cancel = 1
    recover = 2
    repeat = 3


BLANK = ""
