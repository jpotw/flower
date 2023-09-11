import abc
from collections.abc import Iterator
from typing import TypeVar, Generic

from celery.backends.base import BaseBackend

from flower.utils.results.result import ResultIdWithResultPair

BaseBackend_T = TypeVar("BaseBackend_T", bound=BaseBackend)


class AbstractBackendResultsStore(abc.ABC, Generic[BaseBackend_T]):
    def __init__(self, backend: BaseBackend_T, max_tasks_in_memory: int):
        self.backend = backend
        self.max_tasks_in_memory = max_tasks_in_memory

    @abc.abstractmethod
    def results_by_timestamp(self, limit: int | None = None, reverse: bool = True) -> Iterator[
        ResultIdWithResultPair
    ]:
        """
        Create an iterator over the Results found in the backend data store. This iterator should yield paris of
        the form (Task ID associated with Result, the full Result object). We use this iteration pattern because it
        closely resembles the `iter_tasks()` util function.

        :param limit: If not-none, limit number of results in the iterator to this value
        :param reverse: If true, iterator will yield results by timestamp DESC. Otherwise, will yield results by
          timestamp ASC.
        """
        ...
