from __future__ import absolute_import
import collections
import logging
import pika
import pika.exceptions
from tornado import gen, locks
from typing import Callable, Any, Generator, Union

from .common import BaseChannel
from . import compat
from .compat import Awaitable
from . import common
from . import exceptions
from . import exchange
from . import message
from . import queue
from . import tools
from . import transaction

LOGGER = logging.getLogger(__name__)

FunctionOrCoroutine = Union[Callable[[message.IncomingMessage], Any], Awaitable[message.IncomingMessage]]


class Channel(BaseChannel):
    """ Channel abstraction """

    QUEUE_CLASS = queue.Queue
    EXCHANGE_CLASS = exchange.Exchange

    __slots__ = ('_connection', '__closing', '_confirmations', '_delivery_tag', 'loop', '_futures', '_channel',
                 '_on_return_callbacks', 'default_exchange', '_write_lock', '_channel_number', '_publisher_confirms',
                 '_on_return_raises')

    def __init__(self,
                 connection,
                 loop,
                 future_store,
                 channel_number=None,
                 publisher_confirms=True,
                 on_return_raises=False):
        """
        Create a new instance of the Channel.  Don't call this directly, this should
        be constructed by the connection.

        :type connection: :class:`pika.TornadoConnection`
        :type loop: :class:`tornado.ioloop.IOLoop`
        :param future_store: The future store to use
        :type future_store: :class:`topika.common.FutureStore`
        :type channel_number: int
        :type publisher_confirms: bool
        :type on_return_raises: bool
        """
        super(Channel, self).__init__(loop, future_store.create_child())

        self._channel = None  # type: pika.channel.Channel
        self._connection = connection
        self._confirmations = {}
        self._on_return_callbacks = []
        self._delivery_tag = 0
        self._write_lock = locks.Lock()
        self._channel_number = channel_number
        self._publisher_confirms = publisher_confirms

        if not publisher_confirms and on_return_raises:
            raise RuntimeError('on_return_raises must be uses with publisher confirms')

        self._on_return_raises = on_return_raises

        self.default_exchange = self.EXCHANGE_CLASS(
            loop=self.loop,
            future_store=self._futures.create_child(),
            channel=self._channel,
            publish_method=self._publish,
            name='',
            type=exchange.ExchangeType.DIRECT,
            passive=None,
            durable=None,
            auto_delete=None,
            internal=None,
            arguments=None)

    @property
    def _channel_maker(self):
        assert self._connection
        return self._connection._connection.channel

    @property
    def number(self):
        return self._channel.channel_number

    def __str__(self):
        return "{0}".format(self.number if self._channel else "Not initialized channel")

    def __repr__(self):
        return '<%s "%s#%s">' % (self.__class__.__name__, self._connection, self)

    def _on_channel_close(self, channel, reason):
        """
        :type channel: :class:`pika.channel.Channel`
        :type reason: Exception
        :return:
        """
        # Check if we initiated the channel close
        if isinstance(reason, pika.exceptions.ChannelClosedByClient):
            self._closing.set_result(reason.reply_code)
            log_method = LOGGER.debug
        else:
            self._closing.set_exception(reason)
            log_method = LOGGER.error

        log_method("Channel %r closed: %s", channel, reason)

        self._futures.reject_all(reason)
        return reason

    def _on_return(self, channel, method, properties, body):
        """Called as the result of Basic.Return from broker in
        publisher-acknowledgements mode. Saves the info as a ReturnedMessage
        instance in self._puback_return.

        :param pika.Channel channel: our self._impl channel
        :param pika.spec.Basic.Return method:
        :param pika.spec.BasicProperties properties: message properties
        :param body: returned message body; empty string if no body
        :type body: str, unicode

        """
        msg = message.ReturnedMessage(channel=channel, body=body, envelope=method, properties=properties)

        for callback in self._on_return_callbacks:
            tools.create_task(callback(msg), loop=self.loop)

    def add_close_callback(self, callback):
        """
        :type callback: :class:`FunctionType`
        """
        self._closing.add_done_callback(lambda r: callback(r))

    def remove_close_callback(self, callback):
        """
        :type callback: :class:`FunctionType`
        """
        self._closing.remove_done_callback(callback)

    @property
    @gen.coroutine
    def closing(self):
        """ Return future which will be finished after channel close. """
        raise gen.Return((yield self._closing))

    def add_on_return_callback(self, callback):
        """
        :param callback: :class:`FunctionOrCoroutine`
        """
        self._on_return_callbacks.append(gen.coroutine(callback))

    @gen.coroutine
    def _create_channel(self, timeout=None):
        future = self._create_future(timeout=timeout)

        self._channel_maker(channel_number=self._channel_number, on_open_callback=future.set_result)

        channel = yield future  # type: pika.channel.Channel
        if self._publisher_confirms:
            channel.confirm_delivery(self._on_delivery_confirmation)

            if self._on_return_raises:
                channel.add_on_return_callback(self._on_return_delivery)

        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_return_callback(self._on_return)

        raise gen.Return(channel)

    @gen.coroutine
    def initialize(self, timeout=None):
        with (yield self._write_lock.acquire()):
            if self._closing.done():
                raise RuntimeError("Can't initialize closed channel")

            self._channel = yield self._create_channel(timeout)
            self._delivery_tag = 0

    def _on_return_delivery(self, channel, method_frame, properties, body):
        f = self._confirmations.pop(int(properties.headers.get('delivery-tag')))
        f.set_exception(exceptions.UnroutableError([body]))

    def _on_delivery_confirmation(self, method_frame):
        future = self._confirmations.pop(method_frame.method.delivery_tag, None)

        if not future:
            LOGGER.info("Unknown delivery tag %d for message confirmation \"%s\"", method_frame.method.delivery_tag,
                        method_frame.method.NAME)
            return

        try:
            confirmation_type = common.ConfirmationTypes(method_frame.method.NAME.split('.')[1].lower())

            if confirmation_type == common.ConfirmationTypes.ACK:
                future.set_result(True)
            elif confirmation_type == common.ConfirmationTypes.NACK:
                future.set_exception(exceptions.NackError(method_frame))
        except ValueError:
            future.set_exception(RuntimeError('Unknown method frame', method_frame))
        except Exception as e:
            future.set_exception(e)

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def declare_exchange(self,
                         name,
                         type=exchange.ExchangeType.DIRECT,
                         durable=None,
                         auto_delete=False,
                         internal=False,
                         passive=False,
                         arguments=None,
                         timeout=None):
        """
        :type name: str
        :type type: ExchangeType
        :type durable: Optional[bool]
        :type auto_delete: Optional[bool]
        :type internal: Optional[bool]
        :type passive: Optional[bool]
        :type arguments: dict or NoneType
        :type timeout: int
        :rtype: :class:`Generator[Any, None, exchange.Exchange]`
        """

        with (yield self._write_lock.acquire()):
            if auto_delete and durable is None:
                durable = False

            exchange = self.EXCHANGE_CLASS(
                loop=self.loop,
                future_store=self._futures.create_child(),
                channel=self._channel,
                publish_method=self._publish,
                name=name,
                type=type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                arguments=arguments)

            yield exchange.declare(timeout=timeout)

            LOGGER.debug("Exchange declared %r", exchange)

            raise gen.Return(exchange)

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def _publish(self, queue_name, routing_key, body, properties, mandatory, immediate):
        """
        :type properties: :class:`pika.BasicProperties`
        """
        with (yield self._write_lock.acquire()):
            while self._connection.is_closed:
                LOGGER.debug("Can't publish message because connection is inactive")
                yield gen.sleep(1)

            publish_future = self._create_future()
            self._delivery_tag += 1

            if self._on_return_raises:
                properties.headers = properties.headers or {}
                properties.headers['delivery-tag'] = str(self._delivery_tag)

            try:
                self._channel.basic_publish(queue_name, routing_key, body, properties, mandatory, immediate)
            except (AttributeError, RuntimeError) as exc:
                LOGGER.exception("Failed to send data to client (connection unexpectedly closed)")
                self._on_channel_close(self._channel, exc)
                self._connection._connection.close(reply_code=500, reply_text="Incorrect state")
            else:
                if self._publisher_confirms:
                    self._confirmations[self._delivery_tag] = publish_future
                else:
                    publish_future.set_result(None)

            raise gen.Return((yield publish_future))

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def declare_queue(self,
                      name=None,
                      durable=None,
                      exclusive=False,
                      passive=False,
                      auto_delete=False,
                      arguments=None,
                      timeout=None):
        """
        :param name: queue name
        :type name: str
        :param durable: Durability (queue survive broker restart)
        :type durable: bool
        :param exclusive: Makes this queue exclusive. Exclusive queues may only be \
        accessed by the current connection, and are deleted when that connection \
        closes. Passive declaration of an exclusive queue by other connections are not allowed.
        :type exclusive: bool
        :param passive: Only check to see if the queue exists.
        :type passive: bool
        :param auto_delete: Delete queue when channel will be closed.
        :type auto_delete: bool
        :param arguments: pika additional arguments
        :type arguments: Optional[dict]
        :param timeout: execution timeout
        :type timeout: int
        :rtype: :class:`topika.Queue`
        """

        with (yield self._write_lock.acquire()):
            if auto_delete and durable is None:
                durable = False

            queue = self.QUEUE_CLASS(self.loop, self._futures.create_child(), self._channel, name, durable, exclusive,
                                     auto_delete, arguments)

            yield queue.declare(timeout, passive=passive)
            raise gen.Return(queue)

    @gen.coroutine
    def close(self):
        if not self._channel:
            return

        with (yield self._write_lock.acquire()):
            self._channel.close()
            yield self.closing
            self._channel = None

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def set_qos(self, prefetch_count=0, prefetch_size=0, all_channels=False, timeout=None):
        """
        :type prefetch_count: int
        :type prefetch_size: int
        :type all_channels: bool
        :type timeout: int
        """

        with (yield self._write_lock.acquire()):
            f = self._create_future(timeout=timeout)

            self._channel.basic_qos(
                prefetch_size=prefetch_size,
                prefetch_count=prefetch_count,
                all_channels=all_channels,
                callback=f.set_result,
            )

            raise gen.Return((yield f))

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def queue_delete(self, queue_name, timeout=None, if_unused=False, if_empty=False):
        """
        :type queue_name: str
        :type timeout: int
        :type if_unused: bool
        :type if_empty: bool
        """
        with (yield self._write_lock.acquire()):
            f = self._create_future(timeout=timeout)

            self._channel.queue_delete(
                callback=f.set_result,
                queue=queue_name,
                if_unused=if_unused,
                if_empty=if_empty,
            )

            raise gen.Return((yield f))

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def exchange_delete(self, exchange_name, timeout=None, if_unused=False):
        """
        :type exchange_name: str
        :type timeout: int
        :type if_unused:bool
        """
        with (yield self._write_lock.acquire()):
            f = self._create_future(timeout=timeout)

            self._channel.exchange_delete(exchange=exchange_name, if_unused=if_unused, callback=f.set_result)

            raise gen.Return((yield f))

    def transaction(self):
        """
        :rtype: :class:`topika.Transaction`
        """
        if self._publisher_confirms:
            raise RuntimeError("Cannot create transaction when publisher " "confirms are enabled")

        tx = transaction.Transaction(self._channel, self._futures.create_child())

        self.add_close_callback(tx.on_close_callback)

        tx.closing.add_done_callback(lambda _: self.remove_close_callback(tx.on_close_callback))

        return tx


__all__ = ('Channel',)
