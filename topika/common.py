from __future__ import absolute_import
import contextlib
import enum
import tornado.ioloop
from tornado import gen
import tornado.gen
from tornado.gen import coroutine, Return

from . import exceptions
from . import tools


@enum.unique
class ConfirmationTypes(enum.Enum):
    ACK = 'ack'
    NACK = 'nack'


def ensure_coroutine(func_or_coro):
    if gen.is_coroutine_function(func_or_coro):
        return func_or_coro
    else:

        @coroutine
        def coro(*args, **kwargs):
            raise Return(func_or_coro(*args, **kwargs))

        return coro


class _CallbackWrapper(object):
    """
    Take a callback that expects the first argument to be the object corresponding
    to the source of the message and replace it with a proxy object that the
    function will receive as the source object instead.  This wrapper will
    also correctly call the callback in the correct way whether it is a coroutine
    or plain function.
    """

    def __init__(self, source_proxy, callback):
        self._proxy = source_proxy
        self._callback = ensure_coroutine(callback)

    @gen.coroutine
    def __call__(self, unused_source, *args, **kwargs):
        result = yield self._callback(self._proxy, *args, **kwargs)
        raise Return(result)


def future_with_timeout(loop, timeout, future=None):
    """
    Create a future with a timeout

    :param loop: The tornado event loop
    :param timeout: The timeout in seconds
    :param future: An optional existing future
    :return:
    """
    loop = loop if loop else tornado.ioloop.IOLoop.current()
    f = future or tools.create_future(loop=loop)

    def on_timeout():
        if f.done():
            return
        f.set_exception(tornado.ioloop.TimeoutError)

    if timeout:
        handle = loop.call_later(timeout, on_timeout)

        def on_result(_unused_future):
            # Cancel the timeout if the future is done
            loop.remove_timeout(handle)

        f.add_done_callback(on_result)

    return f


class FutureStore(object):
    """
    Borrowed from aio_pika (https://github.com/mosquito/aio-pika)
    """
    __slots__ = "__collection", "__loop", "__parent_store"

    def __init__(self, loop, parent_store=None):
        self.__parent_store = parent_store
        self.__collection = set()
        self.__loop = loop if loop else tornado.ioloop.IOLoop.current()

    def _on_future_done(self, future):
        if future in self.__collection:
            self.__collection.remove(future)

    @staticmethod
    def _reject_future(future, exception):
        if future.done():
            return

        future.set_exception(exception)

    def add(self, future):
        if self.__parent_store:
            self.__parent_store.add(future)

        self.__collection.add(future)
        future.add_done_callback(self._on_future_done)

    def reject_all(self, exception):
        for future in list(self.__collection):
            self.__collection.remove(future)
            self.__loop.add_callback(self._reject_future, future, exception)

    @staticmethod
    def _on_timeout(future):
        if future.done():
            return

        future.set_exception(tornado.ioloop.TimeoutError)

    def create_future(self, timeout=None):
        future = future_with_timeout(self.__loop, timeout)

        self.add(future)

        if self.__parent_store:
            self.__parent_store.add(future)

        return future

    def create_child(self):
        return FutureStore(self.__loop, parent_store=self)

    @contextlib.contextmanager
    def pending_future(self, timeout=None):
        future = None
        try:
            future = self.create_future(timeout)
            yield future
        finally:
            # Cleanup
            if future in self.__collection:
                self.__collection.remove(future)


class BaseChannel(object):
    __slots__ = ('_channel_futures', 'loop', '_futures', '_closing')

    def __init__(self, loop, future_store):
        """
        :type loop: :class:`tornado.ioloop.IOLoop`
        :type future_store: :class:`FutureStore`
        """
        self.loop = loop
        self._futures = future_store
        self._closing = tools.create_future(loop=self.loop)

    @property
    def is_closed(self):
        return self._closing.done()

    def _create_future(self, timeout=None):
        f = self._futures.create_future(timeout)
        return f

    @staticmethod
    def _ensure_channel_is_open(func):

        @contextlib.wraps(func)
        @tools.coroutine
        def wrap(self, *args, **kwargs):
            if self.is_closed:
                raise RuntimeError("The channel is closed")

            raise gen.Return((yield func(self, *args, **kwargs)))

        return wrap

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, getattr(self, 'name', id(self)))
