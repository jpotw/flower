import os
import unittest
from distutils.util import strtobool
from unittest.mock import patch
from urllib.parse import urlencode

import celery
import tornado.testing
from tornado.ioloop import IOLoop
from tornado.options import options

from flower import command  # noqa: F401 side effect - define options
from flower.app import Flower
from flower.events import Events
from flower.urls import handlers, settings


class AsyncHTTPTestCase(tornado.testing.AsyncHTTPTestCase):

    def _get_celery_app(self):
        return celery.Celery()

    def get_app(self, capp=None):
        if not capp:
            capp = self._get_celery_app()
        events = Events(capp, IOLoop.current())
        app = Flower(capp=capp, events=events,
                     options=options, handlers=handlers, **settings)
        return app

    def get(self, url, **kwargs):
        return self.fetch(url, **kwargs)

    def post(self, url, **kwargs):
        if 'body' in kwargs and isinstance(kwargs['body'], dict):
            kwargs['body'] = urlencode(kwargs['body'])
        return self.fetch(url, method='POST', **kwargs)

    def mock_option(self, name, value):
        return patch.object(options.mockable(), name, value)


class BackendDependentTestCase(unittest.TestCase):
    """
    TestCase base class that will automatically skip all test methods if the `SKIP_BACKEND_DEPENDENT_TESTS` is set
    to `true`, or some other variation of a "true" representation as determined by `strtobool()`. You should use this
    when your tests require the presence of some specific backend like Redis, SQLAlchemy, MongoDB, etc. and you want to
    be able to execute the test suite without depending on these other libraries.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        skip_backend_tests_env_var = 'SKIP_BACKEND_DEPENDENT_TESTS'
        should_skip: bool = strtobool(os.environ.get(skip_backend_tests_env_var, 'f'))
        if should_skip:
            setattr(cls, '__unittest_skip__', True)
            setattr(
                cls,
                '__unittest_skip_why__',
                f'Skipping this test case due to the "{skip_backend_tests_env_var}" being true',
            )
