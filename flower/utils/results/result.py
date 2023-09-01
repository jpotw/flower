import datetime
import heapq
from collections.abc import Iterator, Iterable
from functools import total_ordering
from typing import Any, TypeAlias

import dateutil.parser
import kombu.clocks


class Result:
    def __init__(
        self,
        *,
        task_id: str,
        status: str,
        date_done: str,
        result: Any,
        traceback: Any,
        # fields with default values may be null when Celery's `result_extended=False`
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        name: str | None = None,
        # add graceful handling for extra fields that may have been persisted to the result backend record
        **kw: Any,
    ):
        """
        Simple data structure for unpacking Result data from the result backend. Parameters to `__init__()` map to
        the field names that Celery stores when persisting a result object to the backend.
        """
        self.task_id = task_id
        self.status = status
        self.date_done: datetime.datetime = dateutil.parser.parse(date_done)
        self.result = result
        self.traceback = traceback
        self.name = name
        self.args = args
        self.kwargs = kwargs

    @property
    def result_extended(self) -> bool:
        """
        :return: Value of the Celery' app's `result_extended` setting when this Result was persisted to the backend. If
          this is False, then some of the fields like name, args, kwargs will be null due to Celery storing only limited
          fields in the backend.
        """
        return self.name is not None

    def to_render_dict(self) -> dict[str, Any]:
        """
        Convert this data class record into an object that can be formatted by a template
        """
        return {
            "result_extended": self.result_extended,
            "name": self.name,
            "task_id": self.task_id,
            "status": self.status,
            "date_done": self.date_done.timestamp(),
            "args": repr(self.args),
            "kwargs": repr(self.kwargs),
            "result": repr(self.result),
            "traceback": str(self.traceback),
        }


ResultIdWithResultPair: TypeAlias = tuple[str, Result]


@total_ordering
class ResultHeapEntry:
    """
    TODO: document
    """
    def __init__(self, result: Result, *, reverse_ordering: bool):
        self.result = result
        self.reverse_ordering = reverse_ordering
        self.timetup = kombu.clocks.timetuple(None, result.date_done.timestamp(), result.task_id, result)

    def __lt__(self, other: "ResultHeapEntry") -> bool:
        if self.reverse_ordering:
            return self.timetup > other.timetup
        else:
            return self.timetup < other.timetup

    def __eq__(self, other: "ResultHeapEntry") -> bool:
        return self.timetup == other.timetup


class ResultHeap(Iterable[ResultIdWithResultPair]):
    """
    TODO: document
    """
    def __init__(self, *, heap_size_limit: int, reverse_ordering: bool):
        self._heap_size_limit = heap_size_limit
        self._reverse_ordering = reverse_ordering
        self._heap: list[ResultHeapEntry] = []

    def push(self, result: Result) -> Result | None:
        """
        TODO: document
        """
        new_heap_entry = ResultHeapEntry(result, reverse_ordering=self._reverse_ordering)

        if len(self._heap) >= self._heap_size_limit:
            popped_heap_entry: ResultHeapEntry = heapq.heappushpop(self._heap, new_heap_entry)
            return popped_heap_entry.result

        heapq.heappush(self._heap, new_heap_entry)
        return None

    def __iter__(self) -> Iterator[ResultIdWithResultPair]:
        heap_copy = list(self._heap)
        while len(heap_copy) > 0:
            result = heapq.heappop(heap_copy).result
            yield result.task_id, result
