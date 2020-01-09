"""This file contains one class TaskScheduling, which controls all the backend
operations for the task management."""

__all__ = ['Taduler']

from kafka.producer.future import FutureRecordMetadata
from typing import Optional

from .constants import Status, Action
from .db import Tasker
from .utils import ensure_int, validated_setter_property
from .queue import Queue


class Taduler:
    """Class to control the scheduling processes, like repeating a task,
    postponing, cancelling and recovering.

    :param queue: Queue instance with tasker set.
    :type queue: :doc:`.queue.Queue`
    """
    def __init__(self,
                 queue: Queue) -> None:
        self.queue: Queue = queue

    @validated_setter_property
    def queue(self, value) -> None:
        if not isinstance(value, Queue):
            raise TypeError("Taduler.queue must be Queue. "
                            "Received {}".format(type(value)))
        elif not isinstance(value.tasker, Tasker):
            raise TypeError("Taduler.queue must be Queue with not None value "
                            "tasker. Received {}".format(type(value)))

    def _resend_task(self,
                     task_id: int) -> Optional[FutureRecordMetadata]:
        """Resend the task to kafka brokers.

        :param task_id: id of the already created task record
        :type task_id: int
        :return: return value of the kafka producer's `send` function
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        # check the task_id has an associated record in DB.
        task = self.queue.tasker.get_task(id=task_id)
        task = task.one_or_none()
        if not task:
            return None
        # for recovery and postponed job, no need to create new job_id
        job_id = None
        if task.status in (Status.recovered, Status.postponed):
            job_id = task.job_id
        job = self.queue.enqueue_agent.create_job(job_id=job_id,
                                                  topic=task.job_topic,
                                                  content=task.job_value,
                                                  key=task.job_key,
                                                  partition=task.job_partition,
                                                  func=task.job_func,
                                                  timeout=task.job_timeout,
                                                  use_db=True,
                                                  *task.job_args,
                                                  **task.job_kwargs)
        # update job_id in task
        if not job_id:
            task.job_id = job.id
            self.queue.tasker.session.commit()
        return self.queue.enqueue_agent.send(job)

    def _schedule_task(self,
                       task_id: int,
                       creator_id: int,
                       comment: Optional[str],
                       action: Action,
                       new_status: Optional[Status] = None) \
            -> Optional[FutureRecordMetadata]:
        """General method for scheduling a task, i.e. postpone a task, cancel a
        task, repeat a task, and recover a task

        :param task_id: id of the already created task instance in DB
        :type task_id: int
        :param creator_id: id of the user who is managing the task
        :type creator_id: int
        :param comment: comment for creating a TaskLog instance, it can be
          used to record the related Task instance's id
        :type comment: str | None
        :param action: enumeration of the taken action, cancellation,
          postponement, recovery or repeating
        :type action: :doc:`.constants.TaskAction`
        :param new_status: the status to be used for updating task.
        :type new_status: :doc:`.constants.TaskStatus` | None
        :return: return value of the kafka producer's `send` function or None.
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        # create a task log in DB first
        self.queue.tasker.create_task_log(task_id=task_id,
                                          creator_id=creator_id,
                                          comment=comment, action=action)

        # if new_status is provided, it means a task is postponed or canceled
        # if not, a task is repeated or recovered
        if new_status:
            # update the status first
            task = self.queue.tasker.get_task(id=task_id)
            task.update(dict(status=new_status))
            self.queue.tasker.session.commit()
            # if task is cancelled, just return
            if new_status == Status.canceled:
                return None
        else:
            new_task_id = ensure_int(comment, 0)
            if new_task_id:
                task_id = new_task_id
        return self._resend_task(task_id=task_id)

    def postpone_task(self,
                      task_id: int,
                      creator_id: int,
                      comment: Optional[str] = None) \
            -> Optional[FutureRecordMetadata]:
        """Postpone a task.

        :param task_id: id of the related task
        :type task_id: int
        :param creator_id: id of the user who is postponing the task.
        :type creator_id: int
        :param comment: comment for the postponement
        :type comment: str | None
        :return: return value of the kafka producer's `send` function
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        return self._schedule_task(task_id=task_id,
                                   creator_id=creator_id,
                                   comment=comment,
                                   action=Action.postpone,
                                   new_status=Status.postponed)

    def cancel_task(self,
                    task_id: int,
                    creator_id: int,
                    comment: Optional[str] = None) -> \
            Optional[FutureRecordMetadata]:
        """Cancel a task.

        :param task_id: id of the related task
        :type task_id: int
        :param creator_id: id of the user who is canceling the task.
        :type creator_id: int
        :param comment: comment for the canceling.
        :type comment: str | None
        :return: return value of the kafka producer's `send` function
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        return self._schedule_task(task_id=task_id,
                                   creator_id=creator_id,
                                   comment=comment,
                                   action=Action.cancel,
                                   new_status=Status.canceled)

    def recover_task(self,
                     task_id: int,
                     creator_id: int,
                     comment: Optional[str]) -> Optional[FutureRecordMetadata]:
        """Recover a task.

        :param task_id: id of the related task
        :type task_id: int
        :param creator_id: id of the user who is recovering the task.
        :type creator_id: int
        :param comment: comment for the recovery.
        :type comment: str | None
        :return: return value of the kafka producer's `send` function
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        return self._schedule_task(task_id=task_id,
                                   creator_id=creator_id,
                                   comment=comment,
                                   action=Action.recover)

    def repeat_task(self,
                    task_id: int,
                    creator_id: int) -> Optional[FutureRecordMetadata]:
        """Repeat a task.

        :param task_id: id of the related task
        :type task_id: int
        :param creator_id: id of the user who is recovering the task.
        :type creator_id: int
        :return: return value of the kafka producer's `send` function
        :rtype: :doc:`kafka.producer.future.RecordMetadata` | None
        """
        new_task_id = self.queue.tasker.duplicate_task(task_id).id
        return self._schedule_task(task_id=task_id,
                                   creator_id=creator_id,
                                   comment=str(new_task_id),
                                   action=Action.repeat)
