#!/usr/bin/env python
import random
__author__ = 'Jorge R. Herskovic <jherskovic@gmail.com>'


def _random_string(length=50):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(50))

SENTINEL = "##" + _random_string(50) + "##"
