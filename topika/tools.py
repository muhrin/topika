from __future__ import absolute_import
import functools
import pika.exceptions
import tornado.concurrent
from tornado import gen

__all__ = 'wait', 'create_future', 'create_task', 'iscoroutinepartial'


def iscoroutinepartial(coro):
    """
    Function returns True if function it's a partial instance of coroutine. See additional information here_.

    :param coro: Function
    :return: bool

    .. _here: https://goo.gl/C0S4sQ

    """

    while True:
        parent = coro

        coro = getattr(parent, 'func', None)

        if coro is None:
            break

    return gen.is_coroutine_function(parent)


def create_future(loop):
    """ Helper for `create a new future`_ with backward compatibility for Python 3.4

    .. _create a new future: https://goo.gl/YrzGQ6
    """

    try:
        return loop.create_future()
    except AttributeError:
        # Compatibility with older tornado
        return tornado.concurrent.Future()


def create_task(yielded, loop=None):
    """ Helper for `create a new Task`_ with backward compatibility for Python 3.4

    .. _create a new Task: https://goo.gl/g4pMV9
    """

    return gen.convert_yielded(yielded)


@gen.coroutine
def wait(tasks):
    """
    Simple helper for gathering all passed :class:`Task`s.

    :param tasks: list of the :class:`asyncio.Task`s
    :param _loop: Event loop (:func:`tornado.ioloop.IOLoop.current()` when :class:`None`)
    :return: :class:`tuple` of results
    """
    raise gen.Return((yield gen.multi(tasks)))


def ensure_connection_exception(exception_or_message):
    """
    If passed an exception this will be returned.  Otherwise it is assumed
    a string is passed giving the reason for the connection error

    :param exception_or_message:
    :return:
    """
    if isinstance(exception_or_message, Exception):
        return exception_or_message

    # We got a string message
    return pika.exceptions.AMQPConnectionError(exception_or_message)


# Get rid of the stupid default replace_callback in tornado coroutine
coroutine = functools.partial(gen._make_coroutine_wrapper, replace_callback=False)
