import inspect
import time
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Callable

import celery.app.task
import kombu.serialization
import sqlalchemy as sa
from celery.backends.database import Task, TaskSet, DatabaseBackend
from sqlalchemy.orm import Session

from flower.utils.results.result import Result, ResultIdWithResultPair
from flower.utils.results.stores.database import DatabaseBackendResultsStore
from tests.unit import DatabaseBackendDependentTestCase


class TestDatabaseBackendResultsStoreResultsByTimestamp(DatabaseBackendDependentTestCase):

    def setUp(self):
        super().setUp()
        self._purge_all_db_results()
        self.session: Session = self.backend.ResultSession()

        self.task_1_id = 'TASK1-69141a82-d075-44fa-831c-f0ce9b7cd053'
        self._persist_result(
            task_id=self.task_1_id,
            result=None,
            traceback=None,
            name='my_module.success_1_task',
            args=(1, 2),
            kwargs={'seconds': 2},
        )
        time.sleep(0.01)
        self.task_2_id = 'TASK2-feb138ca-d673-40f2-b6e2-7ad0146b36d5'
        self._persist_result(
            task_id=self.task_2_id,
            result='all good',
            traceback=None,
            name=None,
            args=None,
            kwargs=None,
        )
        time.sleep(0.01)
        self.task_3_id = 'TASK3-52a34f7d-d0ba-43c9-b13d-14fc5d1c6094'
        self._persist_result(
            task_id=self.task_3_id,
            result=ZeroDivisionError('You cannot divide by zero'),
            traceback='Traceback (most recent call last): \nOh no, zero division :(',
            name='another.module.task_3_error_task',
            args=(1, 'hey'),
            kwargs={'more': 'stuff'},
        )

    def tearDown(self):
        try:
            super().tearDown()
            if self.session.in_transaction():
                self.session.rollback()
            self.session.close()
        finally:
            self._purge_all_db_results()

    def test_results_by_timestamp_is_lazy_generator(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        self.assertTrue(inspect.isgenerator(results_store.results_by_timestamp()))

    def test_results_by_timestamp_no_limit_reverse_true(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=None, reverse=True))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 3)
        self.assertEqual(result_ids, [self.task_3_id, self.task_2_id, self.task_1_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_3, expected_result_2, expected_result_1 = results
        self._assert_is_result_1(expected_result_1)
        self._assert_is_result_2(expected_result_2)
        self._assert_is_result_3(expected_result_3)

    def test_results_by_timestamp_no_limit_reverse_false(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=None, reverse=False))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 3)
        self.assertEqual(result_ids, [self.task_1_id, self.task_2_id, self.task_3_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_1, expected_result_2, expected_result_3 = results
        self._assert_is_result_1(expected_result_1)
        self._assert_is_result_2(expected_result_2)
        self._assert_is_result_3(expected_result_3)

    def test_results_by_timestamp_with_limit_reverse_true(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=2, reverse=True))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 2)
        self.assertEqual(result_ids, [self.task_3_id, self.task_2_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_3, expected_result_2 = results
        self._assert_is_result_2(expected_result_2)
        self._assert_is_result_3(expected_result_3)

    def test_results_by_timestamp_with_limit_reverse_false(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=2, reverse=False))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 2)
        self.assertEqual(result_ids, [self.task_1_id, self.task_2_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_1, expected_result_2 = results
        self._assert_is_result_1(expected_result_1)
        self._assert_is_result_2(expected_result_2)

    def test_max_tasks_in_memory_works_same_as_limit(self) -> None:
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=1)

        result_pairs_reverse_true: list[ResultIdWithResultPair] = list(
            results_store.results_by_timestamp(limit=None, reverse=True)
        )
        self.assertEqual(len(result_pairs_reverse_true), 1)
        self.assertEqual(result_pairs_reverse_true[0][0], self.task_3_id)
        self._assert_is_result_3(result_pairs_reverse_true[0][1])

        result_pairs_reverse_false: list[ResultIdWithResultPair] = list(
            results_store.results_by_timestamp(limit=None, reverse=False)
        )
        self.assertEqual(len(result_pairs_reverse_false), 1)
        self.assertEqual(result_pairs_reverse_false[0][0], self.task_1_id)
        self._assert_is_result_1(result_pairs_reverse_false[0][1])

    def test_empty_result_set_when_no_results_present(self) -> None:
        self._purge_all_db_results()
        results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        self.assertEqual(list(results_store.results_by_timestamp(limit=None, reverse=True)), [])

    def test_results_are_queryable_in_sqlalchemy(self) -> None:
        session: Session
        with self.backend.ResultSession() as session:
            ids_in_db = list[str](
                session.execute(sa.select(Task.task_id).order_by(Task.date_done.asc())).scalars()
            )

        self.assertListEqual(ids_in_db, [self.task_1_id, self.task_2_id, self.task_3_id])

    def _persist_result(
        self,
        *,
        task_id: str,
        result: Exception | Any,
        traceback: str | None,
        name: str | None,
        args: tuple[Any, ...] | None,
        kwargs: dict[Any, Any] | None,
    ) -> None:
        is_error: bool = isinstance(result, Exception)
        task_ctx = celery.app.task.Context(
            id=task_id,
            task=name,  # gets stored as `name`
            args=args,
            kwargs=kwargs,
            status='FAILURE' if is_error else 'SUCCESS',
        )
        should_persist_result_extended: bool = name is not None

        with patch_celery_conf(self.app, 'result_extended', should_persist_result_extended):
            # The `result_extended` setting gets used during `DatabaseBackend.__init__()`, so if we want to "change"
            # the value of the setting during the test lifecycle, we need a new instance of `DatabaseBackend` that
            # has access to the modified `result_extended` value during `__init__()`. We create a throwaway instance
            # here to save our task using the new setting.
            tmp_backend = DatabaseBackend(app=self.app)
            if is_error:
                tmp_backend.mark_as_failure(
                    task_id=task_id,
                    exc=result,
                    traceback=traceback,
                    request=task_ctx,
                )
            else:
                tmp_backend.mark_as_done(
                    task_id=task_id,
                    result=result,
                    request=task_ctx,
                )

    def _assert_is_result_1(self, result: Result) -> None:
        self.assertEqual(result.task_id, self.task_1_id)
        self.assertIsNone(result.result)
        self.assertEqual(result.name, 'my_module.success_1_task')
        self.assertEqual(result.args, [1, 2])
        self.assertEqual(result.kwargs, {'seconds': 2})
        self.assertEqual(result.status, 'SUCCESS')
        self.assertIsNone(result.traceback)
        self.assertIs(result.result_extended, True)

    def _assert_is_result_2(self, result: Result) -> None:
        self.assertEqual(result.task_id, self.task_2_id)
        self.assertEqual(result.result, 'all good')
        self.assertIsNone(result.name)
        self.assertIsNone(result.args)
        self.assertIsNone(result.kwargs)
        self.assertEqual(result.status, 'SUCCESS')
        self.assertIsNone(result.traceback)
        self.assertIs(result.result_extended, False)

    def _assert_is_result_3(self, result: Result) -> None:
        self.assertEqual(result.task_id, self.task_3_id)
        self.assertEqual(
            result.result,
            {
                'exc_message': ('You cannot divide by zero',),
                'exc_module': 'builtins',
                'exc_type': 'ZeroDivisionError',
            },
        )
        self.assertEqual(result.name, 'another.module.task_3_error_task')
        self.assertEqual(result.args, [1, 'hey'])
        self.assertEqual(result.kwargs, {'more': 'stuff'})
        self.assertEqual(result.status, 'FAILURE')
        self.assertEqual(result.traceback, 'Traceback (most recent call last): \nOh no, zero division :(')
        self.assertIs(result.result_extended, True)

    def _purge_all_db_results(self) -> None:
        with self.backend.ResultSession() as session:
            session.execute(sa.delete(Task))
            session.execute(sa.delete(TaskSet))
            session.commit()


class TestDatabaseBackendResultsStoreDeserializeBinaryColumnValue(DatabaseBackendDependentTestCase):

    def setUp(self):
        super().setUp()
        self.app.conf.accept_content = ['json', 'pickle']

        self.actual_args: tuple[Any, ...] = (34, 'hello', Decimal('55.32'))
        self.actual_kwargs: dict[str, Any] = {'one': 'the thing', 'two': [8, 1290, 4]}

        json_encode: Callable[..., str] = kombu.serialization.registry._encoders['json'].encoder
        self.json_args: bytes = json_encode(self.actual_args).encode()
        self.json_kwargs: bytes = json_encode(self.actual_kwargs).encode()

        pickle_encode: Callable[..., bytes] = kombu.serialization.registry._encoders['pickle'].encoder
        self.pickled_args: bytes = pickle_encode(self.actual_args)
        self.pickled_kwargs: bytes = pickle_encode(self.actual_kwargs)

        self.results_store = DatabaseBackendResultsStore(backend=self.backend, max_tasks_in_memory=10_000)

    def test_json_and_pickle_both_work_when_both_present(self) -> None:
        for json_and_pickle_accept in (['json', 'pickle'], ['application/x-python-serialize', 'application/json']):
            self.app.conf.accept_content = json_and_pickle_accept
            self.assertEqual(
                self.results_store.deserialize_binary_column_value(self.json_args),
                list(self.actual_args),
            )
            self.assertEqual(
                self.results_store.deserialize_binary_column_value(self.json_kwargs),
                self.actual_kwargs,
            )
            self.assertEqual(
                self.results_store.deserialize_binary_column_value(self.pickled_args),
                self.actual_args,
            )
            self.assertEqual(
                self.results_store.deserialize_binary_column_value(self.pickled_kwargs),
                self.actual_kwargs,
            )

    def test_original_byte_string_returned_when_content_type_missing(self) -> None:
        self.app.conf.accept_content = ['something_not_known']
        self.assertEqual(
            self.results_store.deserialize_binary_column_value(self.json_args),
            'Failed to deserialize binary value: ' + repr(self.json_args),
        )
        self.assertEqual(
            self.results_store.deserialize_binary_column_value(self.json_kwargs),
            'Failed to deserialize binary value: ' + repr(self.json_kwargs),
        )
        self.assertEqual(
            self.results_store.deserialize_binary_column_value(self.pickled_args),
            'Failed to deserialize binary value: ' + repr(self.pickled_args),
        )
        self.assertEqual(
            self.results_store.deserialize_binary_column_value(self.pickled_kwargs),
            'Failed to deserialize binary value: ' + repr(self.pickled_kwargs),
        )


@contextmanager
def patch_celery_conf(app: celery.Celery, conf_setting_name: str, tmp_conf_setting_value: Any) -> Iterator[None]:
    """
    Similar to the `mock.patch.object()` context manager, but to be used for the `Celery.conf` object. Because of how
    `Celery.conf` is implemented, `mock.patch.object()` doesn't work out of the box. This does the exact same thing
    in a way that is compatible with that config class.
    """
    original_value = getattr(app.conf, conf_setting_name)
    try:
        setattr(app.conf, conf_setting_name, tmp_conf_setting_value)
        yield
    finally:
        setattr(app.conf, conf_setting_name, original_value)
