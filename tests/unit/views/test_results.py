import datetime
import json
import unittest
from collections.abc import Iterator, Sequence
from copy import deepcopy
from typing import Optional, Union
from unittest import mock

import celery.states
from celery.backends.base import BaseBackend

from flower.utils.results.result import ResultIdWithResultPair, Result
from flower.utils.results.stores import AbstractBackendResultsStore
from tests.unit import AsyncHTTPTestCase


class DummyBackend(BaseBackend):
    def _forget(self, task_id):
        raise NotImplementedError('Not needed for test')

    def add_to_chord(self, chord_id, result):
        raise NotImplementedError('Not needed for test')


class DummyBackendResultsStore(AbstractBackendResultsStore[DummyBackend]):

    def __init__(self, capp: celery.Celery, dummy_results: Sequence[Result]):
        super().__init__(backend=DummyBackend(capp), max_tasks_in_memory=len(dummy_results))
        self.dummy_results = dummy_results

    def results_by_timestamp(self, limit: int | None = None, reverse: bool = True) -> Iterator[
        ResultIdWithResultPair
    ]:
        results: list[Result] = list(self.dummy_results)
        results.sort(key=lambda r: r.date_done, reverse=reverse)
        for result in results:
            yield result.task_id, result


class ResultApiTests(AsyncHTTPTestCase):
    def setUp(self):
        self.app = super().get_app()
        super().setUp()
        self.success_result_extended = Result(
            task_id='54a336ca-672f-43c7-b34e-4643a0224fcd',
            status=celery.states.SUCCESS,
            date_done=datetime.datetime(2021, 3, 12, 5, 24, 9, 138).isoformat('T'),
            result='String Result',
            traceback=None,
            args=['arg1', 44],
            kwargs={
                'a_kwarg_name': 'a_kwarg_value',
            },
            name='a_module.my_success_job',
        )
        self.failure_result = Result(
            task_id='fe874934-1cb6-43d7-9bf4-d5ae3e3aa776',
            status=celery.states.FAILURE,
            date_done=(self.success_result_extended.date_done + datetime.timedelta(minutes=1)).isoformat("T"),
            result={'exc_type': 'Exception', 'exc_message': ['oh no!'], 'exc_module': 'builtins'},
            traceback='Traceback (most recent call last): ... some more stuff',
            args=[],
            kwargs={'a': 'kwarg'},
            name='a_module.my_failure_job',
        )
        self.success_result_non_extended = Result(
            task_id='4d8670af-6789-45c6-918c-601c532eed0e',
            status=celery.states.SUCCESS,
            date_done=(self.failure_result.date_done + datetime.timedelta(seconds=35)).isoformat('T'),
            result=None,
            traceback=None,
            args=None,
            kwargs=None,
            name=None,
        )
        self.dummy_results_store = DummyBackendResultsStore(
            self.app.capp,
            dummy_results=(
                self.success_result_extended,
                self.failure_result,
                self.success_result_non_extended,
            ),
        )

    def get_app(self, capp=None):
        return self.app

    def _get_dummy_results_store(self, *a, **kw) -> DummyBackendResultsStore:
        return self.dummy_results_store

    def run(
        self, result: Optional[unittest.TestResult] = None
    ) -> Optional[unittest.TestResult]:
        with mock.patch('flower.utils.results.stores.store_for_backend', side_effect=self._get_dummy_results_store):
            return super().run(result=result)

    def test_results_pagination_default_sort(self) -> None:
        params_page_1 = self._required_datatable_query_params(start=0, limit=2)

        page1 = self.get('/results/datatable?' + '&'.join(
            map(lambda x: '%s=%s' % x, params_page_1.items())))
        self.assertEqual(200, page1.code)
        self.assertDictEqual(
            {
                'draw': 1,
                'recordsTotal': 3,
                'recordsFiltered': 2,
                'data': [
                    {
                        'task_id': '54a336ca-672f-43c7-b34e-4643a0224fcd',
                        'status': 'SUCCESS',
                        'date_done': self.success_result_extended.date_done.timestamp(),
                        'result': "'String Result'",
                        'traceback': 'None',
                        'args': "['arg1', 44]",
                        'kwargs': "{'a_kwarg_name': 'a_kwarg_value'}",
                        'name': 'a_module.my_success_job',
                        'result_extended': True,
                    },
                    {
                        'task_id': 'fe874934-1cb6-43d7-9bf4-d5ae3e3aa776',
                        'status': 'FAILURE',
                        'date_done': self.failure_result.date_done.timestamp(),
                        'result': "{'exc_type': 'Exception', 'exc_message': ['oh no!'], 'exc_module': 'builtins'}",
                        'traceback': 'Traceback (most recent call last): ... some more stuff',
                        'args': '[]',
                        'kwargs': "{'a': 'kwarg'}",
                        'name': 'a_module.my_failure_job',
                        'result_extended': True,
                    },
                ],
            },
            json.loads(page1.body),
        )

        params_page_2 = deepcopy(params_page_1)
        params_page_2["start"] = 2

        page2 = self.get('/results/datatable?' + '&'.join(
            map(lambda x: '%s=%s' % x, params_page_2.items())))

        self.assertEqual(200, page2.code)
        self.assertDictEqual(
            {
                "draw": 1,
                "recordsTotal": 3,
                "recordsFiltered": 1,
                "data": [
                    {
                        'task_id': '4d8670af-6789-45c6-918c-601c532eed0e',
                        'status': 'SUCCESS',
                        'date_done': self.success_result_non_extended.date_done.timestamp(),
                        'result': 'None',
                        'traceback': 'None',
                        'args': 'None',
                        'kwargs': 'None',
                        'name': None,
                        'result_extended': False,
                    },
                ],
            },
            json.loads(page2.body),
        )

    def test_results_reverse_ordering(self) -> None:
        params = self._required_datatable_query_params(start=0, limit=100)
        del params['order[0][dir]']
        params['order[0][dir]'] = 'desc'

        resp = self.get('/results/datatable?' + '&'.join(
            map(lambda x: '%s=%s' % x, params.items())))
        self.assertEqual(200, resp.code)
        self.assertDictEqual(
            {
                "draw": 1,
                "recordsTotal": 3,
                "recordsFiltered": 3,
                "data": [
                    {
                        'task_id': '4d8670af-6789-45c6-918c-601c532eed0e',
                        'status': 'SUCCESS',
                        'date_done': self.success_result_non_extended.date_done.timestamp(),
                        'result': 'None',
                        'traceback': 'None',
                        'args': 'None',
                        'kwargs': 'None',
                        'name': None,
                        'result_extended': False,
                    },
                    {
                        'task_id': 'fe874934-1cb6-43d7-9bf4-d5ae3e3aa776',
                        'status': 'FAILURE',
                        'date_done': self.failure_result.date_done.timestamp(),
                        'result': "{'exc_type': 'Exception', 'exc_message': ['oh no!'], 'exc_module': 'builtins'}",
                        'traceback': 'Traceback (most recent call last): ... some more stuff',
                        'args': '[]',
                        'kwargs': "{'a': 'kwarg'}",
                        'name': 'a_module.my_failure_job',
                        'result_extended': True,
                    },
                    {
                        'task_id': '54a336ca-672f-43c7-b34e-4643a0224fcd',
                        'status': 'SUCCESS',
                        'date_done': self.success_result_extended.date_done.timestamp(),
                        'result': "'String Result'",
                        'traceback': 'None',
                        'args': "['arg1', 44]",
                        'kwargs': "{'a_kwarg_name': 'a_kwarg_value'}",
                        'name': 'a_module.my_success_job',
                        'result_extended': True,
                    },
                ],
            },
            json.loads(resp.body),
        )

    @staticmethod
    def _required_datatable_query_params(*, start: int, limit: int) -> dict[str, Union[str, int]]:
        return {
            'start': start,
            'length': limit,
            'draw': 1,
            'search[value]': '',
            'order[0][dir]': 'asc',
        }
