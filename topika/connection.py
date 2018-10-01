from __future__ import absolute_import
import logging
from functools import wraps, partial
import pika
from pika import ConnectionParameters
from pika.credentials import ExternalCredentials, PlainCredentials
from pika.spec import REPLY_SUCCESS
import pika.exceptions
import tornado.ioloop
from tornado import gen, ioloop, locks
from tornado.concurrent import Future
from tornado.gen import coroutine, Return
from six.moves.urllib.parse import urlparse

from .channel import Channel
from . import common
from . import compat
from . import exceptions
from . import tools

__all__ = ('Connection', 'connect')

LOGGER = logging.getLogger(__name__)


class Connection(object):
    """ Connection abstraction """

    __slots__ = ('loop', '__closing', '_connection', 'future_store', '__sender_lock', '_io_loop',
                 '__connection_parameters', '__credentials', '__write_lock', '_channels', '__close_started')

    CHANNEL_CLASS = Channel

    def __init__(self,
                 host='localhost',
                 port=5672,
                 login='guest',
                 password='guest',
                 virtual_host='/',
                 loop=None,
                 **kwargs):

        self.loop = loop if loop else ioloop.IOLoop.current()
        self.future_store = common.FutureStore(loop=self.loop)

        self.__credentials = PlainCredentials(login, password) if login else ExternalCredentials()

        self.__connection_parameters = ConnectionParameters(
            host=host, port=port, credentials=self.__credentials, virtual_host=virtual_host, **kwargs)

        self._channels = dict()
        self._connection = None
        self.__closing = None
        self.__close_started = False
        self.__write_lock = locks.Lock()

    def __str__(self):
        return 'amqp://{credentials}{host}:{port}/{vhost}'.format(
            credentials="{0.username}:********@".format(self.__credentials) if isinstance(
                self.__credentials, PlainCredentials) else '',
            host=self.__connection_parameters.host,
            port=self.__connection_parameters.port,
            vhost=self.__connection_parameters.virtual_host,
        )

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{0}: "{1}">'.format(cls_name, str(self))

    def add_close_callback(self, callback):
        """ Add callback which will be called after connection will be closed.

        :class:`tornado.concurrent..Future` will be passed as a first argument.

        Example:

        .. code-block:: python

            import topika
            from tornado import gen

            @gen.coroutine
            def main():
                connection = yield topika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )
                connection.add_close_callback(print)
                yield connection.close()
                # <Future finished result='Normal shutdown'>


        :type callback: Callable[[], None]
        :return: None
        """
        self._closing.add_done_callback(callback)

    @property
    def is_closed(self):
        """ Is this connection closed """

        if not self._connection:
            return True

        if self._closing.done():
            return True

        return False

    @property
    def _closing(self):
        self._ensure_cosing_future()
        return self.__closing

    def _ensure_cosing_future(self, force=False):
        if self.__closing is None or force:
            self.__closing = self.future_store.create_future()

    @property
    @gen.coroutine
    def closing(self):
        """ Return coroutine which will be finished after connection close.

        Example:

        .. code-block:: python

            import topika
            import tornado.gen

            @tornado.gen.coroutine
            def async_close(connection):
                yield tornado.gen.sleep(2)
                yield connection.close()

            @tornado.gen.coroutine
            def main(loop):
                connection = yield topika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )
                topika.create_task(async_close(connection))

                yield connection.closing

        """
        raise gen.Return((yield self._closing))

    def __del__(self):
        with compat.suppress():
            if not self.is_closed:
                self.close()

    @gen.coroutine
    def connect(self):
        """ Connect to AMQP server. This method should be called after :func:`topika.connection.Connection.__init__`

        .. note::
            This method is called by :func:`connect`. You shouldn't call it explicitly.

        :rtype: :class:`pika.TornadoConnection`
        """

        if self.__closing and self.__closing.done():
            raise RuntimeError("Invalid connection state")

        with (yield self.__write_lock.acquire()):
            self._connection = None

            LOGGER.debug("Creating a new AMQP connection: %s", self)

            connect_future = tools.create_future(loop=self.loop)

            connection = pika.TornadoConnection(
                parameters=self.__connection_parameters,
                custom_ioloop=self.loop,
                on_open_callback=connect_future.set_result,
                on_close_callback=partial(self._on_connection_lost, connect_future),
                on_open_error_callback=partial(self._on_connection_refused, connect_future),
            )

            connection.channel_cleanup_callback = self._channel_cleanup
            connection.channel_cancel_callback = self._on_channel_cancel

            result = yield connect_future

            LOGGER.debug("Connection ready: %r", self)

            self._connection = connection
            raise gen.Return(result)

    @gen.coroutine
    def channel(self, channel_number=None, publisher_confirms=True, on_return_raises=False):
        """ Coroutine which returns new instance of :class:`Channel`.

        Example:

        .. code-block:: python
            from tornado import gen
            import topika

            @gen.coroutine
            def main(loop):
                connection = yield topika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )

                channel1 = yield connection.channel()
                yield channel1.close()

                # Creates channel with specific channel number
                channel42 = yield connection.channel(42)
                yield channel42.close()

                # For working with transactions
                channel_no_confirms = connection.channel(publisher_confirms=True)
                yield channel_no_confirms.close()

        :param channel_number: specify the channel number explicit
        :type channel_number: int
        :param publisher_confirms:
            if `True` the :func:`aio_pika.Exchange.publish` method will be return
            :class:`bool` after publish is complete. Otherwise the
            :func:`aio_pika.Exchange.publish` method will be return :class:`None`
        :type publisher_confirms: bool
        :param on_return_raises:
            raise an :class:`topika.exceptions.UnroutableError`
            when mandatory message will be returned
        :rtype: :class:`Generator[Any, None, Channel]`
        """
        with (yield self.__write_lock.acquire()):
            LOGGER.debug("Creating AMQP channel for conneciton: %r", self)

            channel = self.CHANNEL_CLASS(
                self,
                self.loop,
                self.future_store,
                channel_number=channel_number,
                publisher_confirms=publisher_confirms,
                on_return_raises=on_return_raises)
            yield channel.initialize()

            LOGGER.debug("Channel created: %r", channel)

            self._channels[channel.number] = channel

            raise gen.Return(channel)

    def close(self):
        """
        Close AMQP connection

        This method is idempotent and may be called multiple times without conflict
        """
        LOGGER.debug("Closing AMQP connection")

        if self.__close_started:
            return self.__closing

        @gen.coroutine
        def inner():
            if self._connection:
                self._connection.close()
            yield self.closing

        self.__close_started = True
        return tools.create_task(inner())

    #
    # Connections state properties
    #

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._connection.is_open

    def _channel_cleanup(self, channel):
        """
        :type channel: :class:`pika.channel.Channel`
        """
        ch = self._channels.pop(channel.channel_number)  # type: Channel
        ch._futures.reject_all(exceptions.ChannelClosed)

    def _on_connection_refused(self, future, connection, reason):
        """
        :type future: :class:`tornado.concurrent.Future`
        :type connection: :class:`pika.TornadoConnection`
        :type reason: Exception
        """
        self._on_connection_lost(future, connection, reason)

    def _on_connection_lost(self, future, connection, reason):
        """
        :type future: :class:`tornado.concurrent.Future`
        :type connection: :class:`pika.TornadoConnection`
        :type reason: Exception
        """
        if self.__closing and self.__closing.done():
            return

        if isinstance(reason, pika.exceptions.ConnectionClosedByClient) and \
                reason.reply_code == REPLY_SUCCESS:
            return self.__closing.set_result(reason)

        self.future_store.reject_all(reason)

        if future.done():
            return

        future.set_exception(reason)

    def _on_channel_cancel(self, channel):
        """
        :type channel: :class:`pika.channel.Channel`
        """
        ch = self._channels.pop(channel.channel_number)  # type: Channel
        ch._futures.reject_all(exceptions.ChannelClosed)

    #
    # Properties that reflect server capabilities for the current connection
    #

    @property
    def basic_nack_supported(self):
        """Specifies if the server supports basic.nack on the active connection.

        :rtype: bool

        """
        return self._connection.basic_nack

    @property
    def consumer_cancel_notify_supported(self):
        """Specifies if the server supports consumer cancel notification on the
        active connection.

        :rtype: bool

        """
        return self._connection.consumer_cancel_notify

    @property
    def exchange_exchange_bindings_supported(self):
        """Specifies if the active connection supports exchange to exchange
        bindings.

        :rtype: bool

        """
        return self._connection.exchange_exchange_bindings

    @property
    def publisher_confirms_supported(self):
        """Specifies if the active connection can use publisher confirmations.

        :rtype: bool

        """
        return self._connection.publisher_confirms

    # Legacy property names for backward compatibility
    basic_nack = basic_nack_supported
    consumer_cancel_notify = consumer_cancel_notify_supported
    exchange_exchange_bindings = exchange_exchange_bindings_supported
    publisher_confirms = publisher_confirms_supported

    def ensure_connected(self):
        if self.is_closed:
            raise RuntimeError("Connection closed")


@gen.coroutine
def connect(url=None,
            host='localhost',
            port=5672,
            login='guest',
            password='guest',
            virtualhost='/',
            loop=None,
            ssl_options=None,
            connection_class=Connection,
            **kwargs):
    """ Make connection to the broker.

    Example:

    .. code-block:: python

        import topika
        import tornado.gen

        @tornado.gen.coroutine
        def main():
            connection = yield topika.connect(
                "amqp://guest:guest@127.0.0.1/"
            )

    Connect to localhost with default credentials:

    .. code-block:: python

        import topika
        import tornado.gen

        @tornado.gen.coroutine
        def main():
            connection = yield topika.connect()

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
    if url:
        url = urlparse(str(url))
        host = url.hostname or host
        port = url.port or port
        login = url.username or login
        password = url.password or password
        virtualhost = url.path[1:] if len(url.path) > 1 else virtualhost

        ssl_keys = (
            'ca_certs',
            'cert_reqs',
            'certfile',
            'keyfile',
            'ssl_version',
        )

        for key in ssl_keys:
            if key not in url.query:
                continue

            ssl_options[key] = url.query[key]

    connection = connection_class(
        host=host,
        port=port,
        login=login,
        password=password,
        virtual_host=virtualhost,
        loop=loop,
        ssl_options=ssl_options,
        **kwargs)

    yield connection.connect()
    raise gen.Return(connection)
