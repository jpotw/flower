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
        date_done: str | datetime.datetime,
        result: Any,
        traceback: Any,
        # fields with default values may be null when Celery's `result_extended=False`
        args: Any = None,
        kwargs: Any = None,
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
        self.date_done: datetime.datetime = (
            dateutil.parser.parse(date_done) if isinstance(date_done, str) else date_done
        )
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
    Data structure that represents an individual entry in a heap of Result objects. This class is a container of
    the actual `Result`, that also allows for greater than/less than comparison against the timestamp of other entries
    in the heap.
    """
    def __init__(self, result: Result, *, is_max_heap: bool):
        """
        :param result: The Result object referenced by this heap
        :param is_max_heap: Reference to whether this entry exists within a min-heap or a max-heap
        """
        self.result = result
        self.is_max_heap = is_max_heap
        self.timetup = kombu.clocks.timetuple(None, result.date_done.timestamp(), result.task_id, result)

    def __lt__(self, other: "ResultHeapEntry") -> bool:
        if self.is_max_heap:
            # Python's `heapq` functions treat all heap arrays as a min-heap. This means that if we want to work with
            # a max heap, we need to invert the greater than/less than comparison in order to invert the push/pop
            # behavior of `heapq`.
            return self.timetup > other.timetup
        else:
            return self.timetup < other.timetup

    def __eq__(self, other: "ResultHeapEntry") -> bool:
        return self.timetup == other.timetup


class ResultHeap(Iterable[ResultIdWithResultPair]):
    """
    Data structure that maintains a sorted heap of `Result` objects. Results are compared on their `date_done`
    attribute, and can be stored by `date_done` ASC (min-heap) or DESC (max-heap) depending on the `is_max_heap`
    parameter passed into `__init__()`. The heap has a maximum size limit for the sake of memory safety, since result
    backends may have extremely large numbers of task records and loading those all into memory at the same time is
    liable to cause OOM errors.
    """
    def __init__(self, *, heap_size_limit: int, is_max_heap: bool):
        """
        :param heap_size_limit: Maximum number of results in the heap. This represents both the maximum number of
          elements held in-memory by this heap, as well as the maximum number of elements yielded during iteration.
        :param is_max_heap: If true, this heap will behave like a max-heap by sorting Results with large timestamps to
          the top of the heap. If false, this heap will be a min-heap and will forward Results with smaller timestamps
          to the top.
        """
        self._heap_size_limit = heap_size_limit
        self._is_max_heap = is_max_heap
        self._heap: list[ResultHeapEntry] = []

    def push(self, result: Result) -> Result | None:
        """
        Add `result` to the heap, popping off the lowest-priority item from the heap if the heap is already at maximum
        capacity. If `result` would itself be the lowest-priority item in the heap, the heap is not modified and
        `result` is returned.

        :param result: A `Result` object that we want to push to the heap, if its priority allows us to do so
        :return: The lowest-priority `Result` that was popped from the heap in order to perform this insert, if
          pushing an element would exceed the heap's maximum capacity. If the heap is not at maximum capacity when this
          method is called, will return None because no element needed to be popped.
        """
        new_heap_entry = ResultHeapEntry(result, is_max_heap=self._is_max_heap)

        if len(self._heap) >= self._heap_size_limit:
            if new_heap_entry >= self._heap[-1]:
                # This new entry doesn't belong in the heap. Doing a pop/push here would force this item onto
                # the heap despite the fact that its score doesn't beat that of any existing heap entry. We really
                # want something like `pop_push_if_good_enough()`, so we handle that edge case here.
                return result

            # We use `list.pop()` here instead of `heapq.heappop()` in order to remove an element. While
            # `.heappushpop()` will maintain the heap invariant, it will actually return the lowest element (in case of
            # min-heap) or greatest element (in case of max-heap). We want the opposite of that behavior; the
            # weaker-priority item should be removed in favor of this better-priority item. Thankfully, `heapq`
            # maintains heaps in sorted Python lists where the front (index 0) item is the strongest-priority item
            # and the back (index -1) is the weakest-priority item. So a simple `list.pop()` here will remove the
            # weakest-priority item and allow us to `.heappush()` an item that we know will be preferable.
            popped_heap_entry: ResultHeapEntry = self._heap.pop()
            heapq.heappush(self._heap, new_heap_entry)
            return popped_heap_entry.result

        # heap not yet full; can simply push this new entry
        heapq.heappush(self._heap, new_heap_entry)
        return None

    def __iter__(self) -> Iterator[ResultIdWithResultPair]:
        """
        Iterate over a copy of the heap in-order. Whether the order is ASC or DESC is determined by the `is_max_heap`
        property of this heap.
        """
        heap_copy = list(self._heap)
        while len(heap_copy) > 0:
            result = heapq.heappop(heap_copy).result
            yield result.task_id, result
