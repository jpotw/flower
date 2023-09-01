import json
from collections.abc import Iterator
from typing import Any

from celery.backends.redis import RedisBackend

from flower.utils.results.result import Result, ResultIdWithResultPair, ResultHeap
from flower.utils.results.stores import AbstractBackendResultsStore


class RedisBackendResultsStore(AbstractBackendResultsStore[RedisBackend]):
    def results_by_timestamp(self, limit: int | None = None, reverse: bool = True) -> Iterator[
        ResultIdWithResultPair
    ]:
        heap_size_limit = self.max_tasks_in_memory
        if limit is not None and limit < heap_size_limit:
            heap_size_limit = limit

        task_key_prefix = self.backend.task_keyprefix

        heap = ResultHeap(heap_size_limit=heap_size_limit, reverse_ordering=reverse)

        for key in self.backend.client.scan_iter(
            match=task_key_prefix + ("*" if isinstance(task_key_prefix, str) else b"*")
        ):
            result_data: dict[str, Any] = json.loads(self.backend.client.get(key))
            result = Result(**result_data)
            heap.push(result)

        for task_id, result in heap:
            yield task_id, result
