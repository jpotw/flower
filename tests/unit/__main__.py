import unittest
from glob import glob

import tornado.testing


def all():
    test_modules = list(map(lambda x: x.rstrip('.py').replace('/', '.'),
                            glob('tests/unit/*.py') + glob('tests/unit/**/*.py', recursive=True)))
    return unittest.defaultTestLoader.loadTestsFromNames(test_modules)


if __name__ == "__main__":
    tornado.testing.main()
