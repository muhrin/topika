from __future__ import absolute_import
from functools import wraps
from logging import getLogger
from typing import Callable, Generator, Any
import pika.channel
from pika.exceptions import ChannelClosed
from tornado import gen

from . import compat
from .exceptions import ProbableAuthenticationError
from .connection import Connection, connect
from .robust_channel import RobustChannel

log = getLogger(__name__)


def _ensure_connection(func):

    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self.is_closed:
            raise RuntimeError("Connection closed")

        return func(self, *args, **kwargs)

    return wrap


class RobustConnection(Connection):
    """ Robust connection """

    DEFAULT_RECONNECT_INTERVAL = 1
    CHANNEL_CLASS = RobustChannel

    def __init__(self,
                 host='localhost',
                 port=5672,
                 login='guest',
                 password='guest',
                 virtual_host='/',
                 loop=None,
                 **kwargs):
        """
        :type host: str
        :type port: int
        :type login: str
        :type password: str
        :type virtual_host: str
        :type loop: :class:`tornado.ioloop.IOLoop`
        :type kwargs: dict
        """

        self.reconnect_interval = kwargs.pop('reconnect_interval', self.DEFAULT_RECONNECT_INTERVAL)

        super(RobustConnection, self).__init__(
            host=host, port=port, login=login, password=password, virtual_host=virtual_host, loop=loop, **kwargs)

        self._closed = False
        self._on_connection_lost_callbacks = []
        self._on_reconnect_callbacks = []
        self._on_close_callbacks = []

    def add_connection_lost_callback(self, callback):
        """ Add callback which will be called after connection was lost.

        :type callback: :class:`Callable[[], None]`
        :return: None
        """

        self._on_connection_lost_callbacks.append(lambda c: callback(c))

    def add_reconnect_callback(self, callback):
        """ Add callback which will be called after reconnect.

        :type callback: :class:`Callable[[], None]`
        :return: None
        """

        self._on_reconnect_callbacks.append(lambda c: callback(c))

    def add_close_callback(self, callback):
        """ Add callback which will be called after connection will be closed.

        :type callback: :class:`Callable[[], None]`
        :return: None
        """

        self._on_close_callbacks.append(lambda c: callback(c))

    def _on_connection_lost(self, future, connection, reason):
        """
        :type future: :class:`tornado.concurrent.Future`
        :type connection: :class:`pika.TornadoConnection`
        :type reason: Exception
        """
        for callback in self._on_connection_lost_callbacks:
            callback(self)

        if self._closed:
            super(RobustConnection, self)._on_connection_lost(future, connection, reason)

        if isinstance(reason, ProbableAuthenticationError):
            log.error("Authentication error: %s", reason)

        if isinstance(reason, compat.ConnectionRefusedError):
            log.error("Connection refused: %s", reason)

        if not future.done():
            future.set_result(None)

        self.loop.call_later(self.reconnect_interval, self.connect)

    def _channel_cleanup(self, channel):
        """
        :type channel: :class:`pika.channel.Channel`
        """
        pika_channel = self._channels[channel.channel_number]  # type: RobustChannel
        pika_channel._futures.reject_all(ChannelClosed)

        if pika_channel._closed:
            self._channels.pop(channel.channel_number)  # type: RobustChannel

    def _on_channel_error(self, channel):
        """
        :type channel: :class:`pika.channel.Channel`
        """
        log.error("Channel closed: %s. Will attempt to reconnect", channel)
        channel.connection.close(reply_code=500, reply_text="Channel canceled")

    def _on_channel_cancel(self, channel):
        """
        :type channel: :class:`pika.channel.Channel`
        """
        log.debug("Channel canceled: %s", channel)
        self._on_channel_error(channel)

    @gen.coroutine
    def connect(self):
        result = yield super(RobustConnection, self).connect()

        while self._connection is None:
            yield gen.sleep(self.reconnect_interval)
            result = yield super(RobustConnection, self).connect()

        for number, channel in tuple(self._channels.items()):
            try:
                yield channel.on_reconnect(self, number)
            except ChannelClosed:
                self._on_channel_error(channel._channel)
                return

        for callback in self._on_reconnect_callbacks:
            callback(self)

        raise gen.Return(result)

    @property
    def is_closed(self):
        """ Is this connection is closed """

        return self._closed or super(RobustConnection, self).is_closed

    def close(self):
        """
        Close AMQP connection

        :rtype: :class:`tornado.concurrent.Future`
        """
        self._closed = True

        try:
            for callback in self._on_close_callbacks:
                callback(self)
        finally:
            return super(RobustConnection, self).close()


@gen.coroutine
def connect_robust(url=None,
                   host='localhost',
                   port=5672,
                   login='guest',
                   password='guest',
                   virtualhost='/',
                   loop=None,
                   ssl_options=None,
                   connection_class=RobustConnection,
                   **kwargs):
    """ Make robust connection to the broker.

    That means that connection state will be restored after reconnect.
    After connection has been established the channels, the queues and the
    exchanges with their bindings will be restored.

    Example:

    .. code-block:: python

        import topika
        import tornado.gen

        @tornado.gen.coroutine
        def main():
            connection = yield topika.connect_robust("amqp://guest:guest@127.0.0.1/")

    Connect to localhost with default credentials:

    .. code-block:: python

        import topika
        import tornado.gen

        @tornado.gen.coroutine
        def main():
            connection = yield topika.connect_robust()

    .. note::

        The available keys for ssl_options parameter are:
            * cert_reqs
            * certfile
            * keyfile
            * ssl_version

        For an information on what the ssl_options can be set to reference the
        `official Python documentation`_.

        .. _official Python documentation: http://docs.python.org/3/library/ssl.html

    URL string might be contain ssl parameters e.g.
    `amqps://user:password@10.0.0.1//?ca_certs=ca.pem&certfile=cert.pem&keyfile=key.pem`

    :param url: `RFC3986`_ formatted broker address. When :class:`None` \
                will be used keyword arguments.
    :type url: str
    :param host: hostname of the broker
    :type host: str
    :param port: broker port 5672 by default
    :type port: int
    :param login: username string. `'guest'` by default. Provide empty string \
                  for pika.credentials.ExternalCredentials usage.
    :type login: str
    :param password: password string. `'guest'` by default.
    :type password: str
    :param virtualhost: virtualhost parameter. `'/'` by default
    :type virtualhost: str
    :param ssl_options: A dict of values for the SSL connection.
    :type ssl_options: dict
    :param loop: Event loop (:func:`tornado.ioloop.IOLoop.current()` when :class:`None`)
    :type loop: :class:`tornado.ioloop.IOLoop`
    :param connection_class: Factory of a new connection
    :param kwargs: addition parameters which will be passed to \
                   the pika connection.
    :rtype: :class:`topika.connection.Connection`

    .. _RFC3986: https://tools.ietf.org/html/rfc3986
    .. _pika documentation: https://goo.gl/TdVuZ9

    """
    raise gen.Return((yield connect(
        url=url,
        host=host,
        port=port,
        login=login,
        password=password,
        virtualhost=virtualhost,
        loop=loop,
        connection_class=connection_class,
        ssl_options=ssl_options,
        **kwargs)))


__all__ = 'RobustConnection', 'connect_robust',
