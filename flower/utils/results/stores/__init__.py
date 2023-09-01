from celery.backends.base import BaseBackend

from flower.utils.results.stores.abstract import AbstractBackendResultsStore

try:
    from celery.backends.database import DatabaseBackend
except ImportError:
    DatabaseBackend = None


try:
    from celery.backends.redis import RedisBackend
except ImportError:
    RedisBackend = None


def store_for_backend(for_backend: BaseBackend, max_tasks_in_memory: int = 10000) -> "AbstractBackendResultsStore":
    """
    Factory function that takes a given `BaseBackend` subclass and returns an instance of the
    `AbstractBackendResultsStore` that corresponds to the type of backend.
    """
    if DatabaseBackend is not None and isinstance(for_backend, DatabaseBackend):
        from flower.utils.results.stores.database import DatabaseBackendResultsStore

        return DatabaseBackendResultsStore(for_backend, max_tasks_in_memory=max_tasks_in_memory)
    elif RedisBackend is not None and isinstance(for_backend, RedisBackend):
        from flower.utils.results.stores.redis import RedisBackendResultsStore

        return RedisBackendResultsStore(for_backend, max_tasks_in_memory=max_tasks_in_memory)

    raise TypeError(f"Unsupported backend type {for_backend.__class__}")
