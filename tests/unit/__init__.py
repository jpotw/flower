import os
import unittest
from distutils.util import strtobool
from unittest.mock import patch
from urllib.parse import urlencode, urlparse, urlunparse

import celery.backends.database.session
import sqlalchemy.schema
import tornado.testing
from celery.backends.database import DatabaseBackend
from celery.exceptions import ImproperlyConfigured
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


class DatabaseBackendDependentTestCase(BackendDependentTestCase):
    """
    Extension of `BackendDependentTestCase` that sets a default value for `self.app.conf.database_url` based on the
    `TEST_DATABASE_CELERY_RESULT_BACKEND_CONNECTION_STRING` environment variable. If no such environment variable
    exists, the setup will assume a localhost connection to Postgres.
    """

    test_schema_name = 'test_flower'
    """
    Name of the DB schema within which we should run tests. This should be separate from the main database schema so
    we are safe to create/destroy records at-will throughout the testing lifecycle.
    """

    def setUp(self):
        super().setUp()
        if hasattr(self, 'app'):
            if not isinstance(self.app, celery.Celery):
                raise ImproperlyConfigured(
                    'If `self.app` is initialized by another class setUp, it must be an instance of Celery'
                )
        else:
            self.app = celery.Celery()

        database_url_parsed = urlparse(
            os.environ.get(
                'TEST_DATABASE_CELERY_RESULT_BACKEND_CONNECTION_STRING',
                'postgresql://postgres:postgres@localhost:5432',
            )
        )
        if '+' in database_url_parsed.scheme:
            raise ImproperlyConfigured(
                'Should exclude the "+" from Celery database_url scheme and instead only supply the database protocol'
            )
        self.app.conf.database_url = urlunparse(database_url_parsed)

        # restrict creation/deletion of DB models to a separate schema
        self.app.conf.database_table_schemas = {
            'task': self.test_schema_name,
            'group': self.test_schema_name,
        }

        self.backend = DatabaseBackend(app=self.app)
        self._ensure_test_schema()

    def _ensure_test_schema(self) -> None:
        """
        Create a short-lived session that executes a CREATE SCHEMA statement if the test schema does not yet exist
        in the database.
        """
        test_schema_name = self.test_schema_name

        class CreateSchemaSessionManager(celery.backends.database.session.SessionManager):
            def prepare_models(self, engine):
                with engine.connect() as conn:
                    if not conn.dialect.has_schema(conn, test_schema_name):
                        engine.execute(sqlalchemy.schema.CreateSchema(test_schema_name))
                return super().prepare_models(engine)

        with self.backend.ResultSession(session_manager=CreateSchemaSessionManager()):
            # invoking the context manager will invoke the `prepare_models()` that ensures a schema
            pass
