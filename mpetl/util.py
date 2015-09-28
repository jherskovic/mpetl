#!/usr/bin/env python
from __future__ import absolute_import, print_function
import random
import signal
from multiprocessing import Event
import sys
__author__ = u'Jorge R. Herskovic <jherskovic@gmail.com>'


def _random_string(length=50):
    return u''.join(random.choice(u'abcdefghijklmnopqrstuvwxyz') for i in xrange(50))

SENTINEL = u"##" + _random_string(50) + u"##"

_verbose_debugging = Event()
#  Shortcut
verbose_debugging = _verbose_debugging.is_set

_old_siginfo_handler = None


def _mpetl_siginfo_handler(signum, frame):
    global _verbose_debugging, _old_siginfo_handler
    _verbose_debugging.set()

    if _old_siginfo_handler is not None:
        if _old_siginfo_handler not in (signal.SIG_DFL, signal.SIG_IGN):
            return _old_siginfo_handler(signum, frame)


def signal_to_handle():
    u"""We prefer SIGINFO, but only FreeBSD and Mac OS X have it. In Linux, we'll have to go with
    SIGUSR1."""
    if hasattr(signal, u"SIGINFO"):
        return signal.SIGINFO
    else:
        return signal.SIGUSR1


def enable_siginfo_trap():
    global _old_siginfo_handler
    _old_siginfo_handler = signal.signal(signal_to_handle(), _mpetl_siginfo_handler)


def dprint(*args, **kwargs):
    if not verbose_debugging():
        return

    print(*args, **kwargs)


def trap_under_nose():
    u"""Sets up the siginfo handler if it detects that we're running under nosetests."""
    if _old_siginfo_handler is not None:
        return

    if u'nosetests' in sys.argv[0]:
        print("Nose detected; trapping a signal.")
        enable_siginfo_trap()
