import logging
import pika
import pika.exceptions
import tornado.ioloop
from tornado.concurrent import Future
from tornado.gen import coroutine, Return

from .channel import Channel
from . import common
from . import tools

__all__ = ('Connection', 'connect')

LOGGER = logging.getLogger(__name__)


class Connection(object):
    CHANNEL_CLASS = Channel

    # Default for future that will be used when user initiates connection close
    _close_future = None

    def __init__(self, pika_connection):
        """
        :param pika_connection: The pika tornado connection
        :type pika_connection: :class:`pika.TornadoConnection`
        """
        # The actual pika connection object
        self._connection = pika_connection

        # Store of futures for pending operations
        self._future_store = common.FutureStore(self.loop)

        self._connection.add_on_close_callback(self._on_close)

    # TODO: Look into this for python 3.5+
    # @coroutine
    # def __enter__(self):
    #     yield self.ensure_connected()
    #     raise Return(self)
    #
    # @coroutine
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     yield self.close()
    #     raise Return(False)  # Don't supress any exceptions

    @property
    def loop(self):
        return self._connection.ioloop

    def add_backpressure_callback(self, callback):
        return self._connection.add_backpressure_callback(common._CallbackWrapper(self, callback))

    def add_on_open_callback(self, callback):
        return self._connection.add_on_open_callback(common._CallbackWrapper(self, callback))

    def add_on_close_callback(self, callback):
        return self._connection.add_on_close_callback(common._CallbackWrapper(self, callback))

    def add_on_connection_blocked_callback(self, callback):
        self._connection.add_on_connection_blocked_callback(common._CallbackWrapper(self, callback))

    def add_on_connection_unblocked_callback(self, callback):
        self._connection.add_on_connection_unblocked_callback(common._CallbackWrapper(self, callback))

    def add_timeout(self, deadline, callback):
        """Create a single-shot timer to fire after deadline seconds. Do not
        confuse with Tornado's timeout where you pass in the time you want to
        have your callback called. Only pass in the seconds until it's to be
        called.

        NOTE: the timer callbacks are dispatched only in the scope of
        specially-designated methods: see
        `BlockingConnection.process_data_events` and
        `BlockingChannel.start_consuming`.

        :param float deadline: The number of seconds to wait to call callback
        :param callable callback: The callback method with the signature
            callback()

        :returns: opaque timer id

        """
        return self._connection.add_timeout(deadline, callback)

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible in the
        context of this connection's thread.

        NOTE: This is the only thread-safe method in `BlockingConnection`. All
         other manipulations of `BlockingConnection` must be performed from the
         connection's thread.

        For example, a thread may request a call to the
        `BlockingChannel.basic_ack` method of a `BlockingConnection` that is
        running in a different thread via

        ```
        connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=...))
        ```

        :param method callback: The callback method; must be callable

        """
        self._connection.add_callback_threadsafe(callback)

    def remove_timeout(self, timeout_id):
        """Remove a timer if it's still in the timeout stack

        :param timeout_id: The opaque timer id to remove

        """
        self._connection.remove_timeout(timeout_id)

    @coroutine
    def close(self, reply_code=200, reply_text='Normal shutdown'):
        if self.is_closing or self.is_closed:
            LOGGER.warning('Suppressing close request on %s', self)
            return

        self._close_future = Future()
        self._connection.close(reply_code, reply_text)
        yield self._close_future

    @coroutine
    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :rtype: :class:`Channel`
        """
        self.ensure_connected()

        with self._future_store.pending_future() as open_future:
            impl_channel = self._connection.channel(
                channel_number=channel_number,
                on_open_callback=open_future.set_result)

            # Wait until the channel is opened
            yield open_future

        # Create our proxy channel
        channel = self.CHANNEL_CLASS(impl_channel, self, self._future_store.create_child())

        # Link implementation channel with our proxy channel
        impl_channel._set_cookie(channel)

        raise Return(channel)

    #
    # Connections state properties
    #

    @property
    def is_closed(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._connection.is_closed

    @property
    def is_closing(self):
        """
        Returns True if connection is in the process of closing due to
        client-initiated `close` request, but closing is not yet complete.
        """
        return self._connection.is_closing

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._connection.is_open

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
            raise pika.exceptions.ConnectionClosed("Connection is closed")

    def _on_close(self, connection, reply_code, reply_text):
        LOGGER.info('Connection closed: (%s) %s', reply_code, reply_text)

        if self._close_future:
            # The user has requested a close
            self._close_future.set_result((reply_code, reply_text))

        # Set exceptions on all outstanding operations
        self._future_store.reject_all(pika.exceptions.ConnectionClosed(reply_code, reply_text))


@coroutine
def connect(connection_parameters, loop=None, connection_class=Connection, **kwargs):
    """ Make connection to the broker

    Example:

    .. code-block:: python

        import topika

        @tornado.gen.coroutine
        def main():
            connection = yield topika.connect(
                "amqp://guest:guest@127.0.0.1/"
            )

    Connect to localhost with default credentials:

    .. code-block:: python

        import topika

        @tornado.gen.coroutine
        async def main():
            connection = yield topika.connect()

    :param connection_parameters: Pika connection parameters
    :type connection_parameters: :class:`pika.Parameters`
    :param loop: Event loop (:func:`tornado.ioloop.IOLoop.current()` when :class:`None`)
    :param connection_class: Factory of a new connection
    :param kwargs: addition parameters which will be passed to the pika connection.
    :return: :class:`topika.connection.Connection`

    .. _pika documentation: https://goo.gl/TdVuZ9

    """
    loop = loop if loop else tornado.ioloop.IOLoop.current()

    open_future = tools.create_future(loop)

    def _on_close_error(conn, exc_or_msg):
        open_future.set_exception(tools.ensure_connection_exception(exc_or_msg))

    pika_connection = pika.TornadoConnection(
        parameters=connection_parameters,
        on_open_callback=open_future.set_result,
        on_open_error_callback=_on_close_error,
        # on_close_callback=self._on_close,
        custom_ioloop=loop
    )

    yield open_future
    connection = connection_class(pika_connection, **kwargs)
    raise Return(connection)
