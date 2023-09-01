from collections.abc import Iterator

from celery.backends.database import DatabaseBackend

from flower.utils.results.result import ResultIdWithResultPair
from flower.utils.results.stores import AbstractBackendResultsStore


class DatabaseBackendResultsStore(AbstractBackendResultsStore[DatabaseBackend]):
    def results_by_timestamp(self, limit: int | None = None, reverse: bool = True) -> Iterator[
        ResultIdWithResultPair
    ]:
        raise NotImplementedError()
