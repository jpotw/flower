import inspect
import time
import unittest

import celery.app.task
from celery.backends.redis import RedisBackend

from flower.utils.results.result import ResultIdWithResultPair, Result
from flower.utils.results.stores.redis import RedisBackendResultsStore


class TestRedisBackendResultsStore(unittest.TestCase):

    def setUp(self):
        self.app = celery.Celery()
        self.app.conf.result_extended = True

        self.backend = RedisBackend(app=self.app)
        self.backend.client.flushdb()

        self.task_1_id = 'TASK1-332592ed-da66-4ef9-b60d-403a8eda57e5'
        self.backend.mark_as_done(
            task_id=self.task_1_id,
            result=None,
            request=celery.app.task.Context(
                id=self.task_1_id,
                task='my_module.success_1_task',  # gets stored as `name`
                args=(1, 2),
                kwargs={'seconds': 2},
                status='SUCCESS',
            )
        )
        time.sleep(0.01)
        self.task_2_id = 'TASK2-95120ce9-470a-419d-a589-d57b14f3f2ca'
        self.backend.mark_as_done(
            task_id=self.task_2_id,
            result='all good',
            request=celery.app.task.Context(
                id=self.task_2_id,
                task=None,  # gets stored as `name`
                args=None,
                kwargs=None,
                status='SUCCESS',
            )
        )
        time.sleep(0.01)
        self.task_3_id = 'TASK3-b2c3760f-e6fe-4121-8c6e-4127c45507a0'
        self.backend.mark_as_failure(
            task_id=self.task_3_id,
            exc=ValueError('Something bad!'),
            traceback='Traceback (most recent call last): \nSomething bad!',
            request=celery.app.task.Context(
                id=self.task_3_id,
                task='another.module.task_3_error_task',  # gets stored as `name`
                args=(1, 'hey'),
                kwargs={'more': 'stuff'},
                status='FAILURE',
            )
        )

    def tearDown(self):
        self.backend.client.flushdb()

    def test_results_by_timestamp_is_lazy_generator(self) -> None:
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        self.assertTrue(inspect.isgenerator(results_store.results_by_timestamp()))

    def test_results_by_timestamp_no_limit_reverse_true(self) -> None:
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
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
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
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
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=2, reverse=True))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 2)
        self.assertEqual(result_ids, [self.task_3_id, self.task_2_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_3, expected_result_2 = results
        self._assert_is_result_2(expected_result_2)
        self._assert_is_result_3(expected_result_3)

    def test_results_by_timestamp_with_limit_reverse_false(self) -> None:
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        result_pairs: list[ResultIdWithResultPair] = list(results_store.results_by_timestamp(limit=2, reverse=False))
        result_ids: list[str] = [result_tup[0] for result_tup in result_pairs]
        self.assertEqual(len(result_ids), 2)
        self.assertEqual(result_ids, [self.task_1_id, self.task_2_id])
        results: list[Result] = [result_tup[1] for result_tup in result_pairs]
        expected_result_1, expected_result_2 = results
        self._assert_is_result_1(expected_result_1)
        self._assert_is_result_2(expected_result_2)

    def test_max_tasks_in_memory_works_same_as_limit(self) -> None:
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=1)

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
        self.backend.client.flushdb()
        results_store = RedisBackendResultsStore(backend=self.backend, max_tasks_in_memory=100)
        self.assertEqual(list(results_store.results_by_timestamp(limit=None, reverse=True)), [])

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
                'exc_message': ['Something bad!'],
                'exc_module': 'builtins',
                'exc_type': 'ValueError',
            },
        )
        self.assertEqual(result.name, 'another.module.task_3_error_task')
        self.assertEqual(result.args, [1, 'hey'])
        self.assertEqual(result.kwargs, {'more': 'stuff'})
        self.assertEqual(result.status, 'FAILURE')
        self.assertEqual(result.traceback, 'Traceback (most recent call last): \nSomething bad!')
        self.assertIs(result.result_extended, True)
