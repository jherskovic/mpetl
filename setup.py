from __future__ import absolute_import
from distutils.core import setup

setup(
    name=u'mpetl',
    version='1.2',
    packages=['mpetl'],
    url=u'https://github.com/jherskovic/mpetl',
    license=u'Apache',
    author=u'Jorge Herskovic',
    extras_require={'with_setproctitle': ['setproctitle']},
    author_email=u'jherskovic@gmail.com',
    description=u'MultiProcessing ETL pipelines for embarrassingly parallel work.'
)
