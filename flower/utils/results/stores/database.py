import json
import pickle
from collections.abc import Iterator
from typing import Any

import kombu.serialization
import sqlalchemy as sa
from celery.backends.database import DatabaseBackend, TaskExtended
from sqlalchemy.orm.session import Session

from flower.utils.results.result import ResultIdWithResultPair, Result
from flower.utils.results.stores import AbstractBackendResultsStore


class DatabaseBackendResultsStore(AbstractBackendResultsStore[DatabaseBackend]):
    """
    Results store capable of reading from Celery's supported `DatabaseBackend`, which uses SQLAlchemy models to persist
    tasks in a SQL database.
    """
    def results_by_timestamp(self, limit: int | None = None, reverse: bool = True) -> Iterator[
        ResultIdWithResultPair
    ]:
        query_limit = self.max_tasks_in_memory
        if limit is not None and limit < query_limit:
            query_limit = limit

        session: Session
        with self.backend.ResultSession() as session:
            ordering = TaskExtended.date_done.desc() if reverse else TaskExtended.date_done.asc()
            task_select_query = sa.select(TaskExtended).order_by(ordering).limit(query_limit)
            for task in session.execute(task_select_query).scalars():
                result = self._map_task_to_result(task)
                yield result.task_id, result

    def _map_task_to_result(self, task: TaskExtended) -> Result:
        """
        Convert a `TaskExtended` ORM object into our shared `Result` data structure. This class assumes the usage of
        `TaskExtended` in order to query the "taskmeta" table, since `TaskExtended` queries can successfully return
        full data for both tasks that were saved with `result_extended=True` and those that were saved with
        `result_extended=False`.

        Because we want to support both extended and non-extended tasks, we need a way to figure out whether the
        provided task was _actually_ extended or not at the time it was saved. We can do this by looking at the `name`
        field. When a task is saved under `result_extended=True`, then it will have a name referencing the name of the
        function. Otherwise, that field will be null and we know that it was `result_extended=False`.
        """
        is_actually_extended: bool = task.name is not None
        if is_actually_extended:
            return Result(
                task_id=task.task_id,
                status=task.status,
                date_done=task.date_done,
                result=task.result,
                traceback=task.traceback,
                args=self.deserialize_binary_column_value(task.args),
                kwargs=self.deserialize_binary_column_value(task.kwargs),
                name=task.name,
            )

        return Result(
            task_id=task.task_id,
            status=task.status,
            date_done=task.date_done,
            result=task.result,
            traceback=task.traceback,
        )

    def deserialize_binary_column_value(self, value: bytes) -> Any:
        """
        Attempt to deserialize the provided `value` using the available serialization decoders. Celery stores task
        `args` and `kwargs` in binary columns, but the proper decoding mechanism for those binary columns is not
        immediately obvious. These fields get serialized bsed on whatever the value of `Celery.conf.result_serializer`
        is at the time the task result is saved. However, it's possible that the value of that config setting changed
        across different Celery processes, and therefore we may be dealing with a database that has co-mingled records
        from different serializers. Unfortunately, there is no column in the database schema that records which
        serializer was used for each task.

        To work around this limitation, this method takes guesses at the serialization of `value` based on whatever
        serializers are available in the active `result_accept_content` or `accept_content` Celery config setting.
        Each serializer will attempt deserialization, and if one succeeds, we return the deserialized value immediately.
        If all deserialization attempts fail, we will gracefully return the original bytes value with a prefix message
        explaining that deserialization failed.

        TODO: currently this method only attempts deserialization with JSON and pickle. We should support more built-in
          content types, and potentially allow for deserialization using custom encodings. We chose to limit the
          supported serializers here because JSON and pickle are the only serialization mechanisms available without
          additional dependencies (e.g. 'yaml' requires the inclusion of the third-party `yaml` library).
        """
        celery_result_accept_content: list[str] = (
            self.backend.app.conf.result_accept_content
            or self.backend.app.conf.accept_content
        )
        accept_content_types: list[str] = [item.lower() for item in celery_result_accept_content]

        if 'json' in accept_content_types or 'application/json' in accept_content_types:
            try:
                return kombu.serialization.registry._decoders['application/json'](value)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        if 'pickle' in accept_content_types or 'application/x-python-serialize' in accept_content_types:
            try:
                return kombu.serialization.registry._decoders['application/x-python-serialize'](value)
            except pickle.UnpicklingError:
                pass

        # couldn't deserialize; just fall back to an error message plus the `repr()` of the original byte string
        return 'Failed to deserialize binary value: ' + repr(value)
