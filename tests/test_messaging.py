#!/usr/bin/env python

from __future__ import absolute_import
__author__ = u'Jorge R. Herskovic <jherskovic@gmail.com>'

import unittest
from mpetl.pipeline import _Pipeline
from mpetl.messaging import *

def generator(up_to):
    for i in xrange(up_to):
        yield i
    return

def conditional_routing(number, pipeline_msg):
    u"""Routes even numbers to a pipeline called "even" and odd numbers to a pipeline called "odd"
    """
    #print("Got", number)
    if number % 2 == 0:
        pipeline_msg.send_message(u"even", [number])
    else:
        pipeline_msg.send_message(u"odd", [number])


def gathering_function(number):
    #print("Gathering",number)
    return number


class test_messaging(unittest.TestCase):
    def setUp(self):
        self.messaging = MessagingCenter()
        self.messaging.start()

    def test_result(self):
        self.evens = _Pipeline()
        self.evens.add_destination(gathering_function)
        self.evens.start()

        self.odds = _Pipeline()
        self.odds.add_destination(gathering_function)
        self.odds.start()

        self.original = _Pipeline()
        self.original.add_origin(generator)
        self.original.add_destination(conditional_routing, pipeline_msg = self.messaging)
        self.original.start()

        self.messaging.register_pipeline_queue(u"even", self.evens._queues[0])
        self.messaging.register_pipeline_queue(u"odd", self.odds._queues[0])

        self.original.feed(100)

        self.original.join()
        self.messaging.flush()

        self.odds.join()
        self.evens.join()
        evens = [x for x in self.evens.as_completed()]
        odds = [x for x in self.odds.as_completed()]

        self.messaging.close_pipeline_queue(u"odd")
        self.messaging.close_pipeline_queue(u"even")

if __name__ == u'__main__':
    unittest.main()
