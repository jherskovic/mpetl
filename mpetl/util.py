#!/usr/bin/env python
from __future__ import absolute_import
import random
__author__ = u'Jorge R. Herskovic <jherskovic@gmail.com>'


def _random_string(length=50):
    return u''.join(random.choice(u'abcdefghijklmnopqrstuvwxyz') for i in xrange(50))

SENTINEL = u"##" + _random_string(50) + u"##"
