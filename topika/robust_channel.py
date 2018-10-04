from __future__ import absolute_import
from logging import getLogger
from typing import Callable, Any, Generator, Union
from tornado import gen

from . import compat
from . import exchange
from . import tools
from .compat import Awaitable
from .exchange import Exchange, ExchangeType
from .message import IncomingMessage
from .queue import Queue
from .common import BaseChannel, FutureStore
from .channel import Channel
from .robust_queue import RobustQueue
from .robust_exchange import RobustExchange

log = getLogger(__name__)

FunctionOrCoroutine = Union[Callable[[IncomingMessage], Any], Awaitable[IncomingMessage]]


class RobustChannel(Channel):
    """ Channel abstraction """

    QUEUE_CLASS = RobustQueue
    EXCHANGE_CLASS = RobustExchange

    def __init__(self,
                 connection,
                 loop,
                 future_store,
                 channel_number=None,
                 publisher_confirms=True,
                 on_return_raises=False):
        """

        :param connection: :class:`pika.TornadoConnection` instance
        :param loop: Event loop (:func:`tornado.ioloop.IOLoop.current()` when :class:`None`)
        :param future_store: :class:`topika.common.FutureStore` instance
        :param publisher_confirms: False if you don't need delivery confirmations (in pursuit of performance)
        """
        super(RobustChannel, self).__init__(
            loop=loop,
            future_store=future_store.create_child(),
            connection=connection,
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self._closed = False
        self._exchanges = dict()
        self._queues = dict()
        self._qos = 0, 0

    @gen.coroutine
    def on_reconnect(self, connection, channel_number):
        exc = compat.ConnectionError('Auto Reconnect Error')

        if not self._closing.done():
            self._closing.set_exception(exc)

        self._closing = tools.create_future(loop=self.loop)
        self._futures.reject_all(exc)
        self._connection = connection
        self._channel_number = channel_number

        yield self.initialize()

        for exchange in self._exchanges.values():
            yield exchange.on_reconnect(self)

        for queue in self._queues.values():
            yield queue.on_reconnect(self)

    @gen.coroutine
    def initialize(self, timeout=None):
        result = yield super(RobustChannel, self).initialize()

        prefetch_count, prefetch_size = self._qos

        yield self.set_qos(prefetch_count=prefetch_count, prefetch_size=prefetch_size)

        raise gen.Return(result)

    @gen.coroutine
    def set_qos(self, prefetch_count=0, prefetch_size=0, all_channels=False, timeout=None):
        if all_channels:
            raise NotImplementedError("Not available to RobustConnection")

        self._qos = prefetch_count, prefetch_size

        raise gen.Return((yield super(RobustChannel, self).set_qos(
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            timeout=timeout,
        )))

    @BaseChannel._ensure_channel_is_open
    @gen.coroutine
    def close(self):
        if self._closed:
            return

        with (yield self._write_lock.acquire()):
            self._closed = True
            self._channel.close()
            yield self.closing
            self._channel = None

    @gen.coroutine
    def declare_exchange(self,
                         name,
                         type=exchange.ExchangeType.DIRECT,
                         durable=None,
                         auto_delete=False,
                         internal=False,
                         passive=False,
                         arguments=None,
                         timeout=None,
                         robust=True):

        exchange = yield super(RobustChannel, self).declare_exchange(
            name=name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
            arguments=arguments,
            timeout=timeout,
        )

        if not internal and robust:
            self._exchanges[name] = exchange

        raise gen.Return(exchange)

    @gen.coroutine
    def exchange_delete(self, exchange_name, timeout=None, if_unused=False):
        result = yield super(RobustChannel, self).exchange_delete(
            exchange_name=exchange_name, timeout=timeout, if_unused=if_unused)

        self._exchanges.pop(exchange_name, None)

        raise gen.Return(result)

    @gen.coroutine
    def declare_queue(self,
                      name=None,
                      durable=None,
                      exclusive=False,
                      passive=False,
                      auto_delete=False,
                      arguments=None,
                      timeout=None,
                      robust=True):
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
        :type robust: bool
        :rtype: :class:`Generator[Any, None, Queue]`
        """

        queue = yield super(RobustChannel, self).declare_queue(
            name=name,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            auto_delete=auto_delete,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._queues[name] = queue

        raise gen.Return(queue)

    @gen.coroutine
    def queue_delete(self, queue_name, timeout=None, if_unused=False, if_empty=False):
        result = yield super(RobustChannel, self).queue_delete(
            queue_name=queue_name, timeout=timeout, if_unused=if_unused, if_empty=if_empty)

        self._queues.pop(queue_name, None)
        raise gen.Return(result)


__all__ = ('RobustChannel',)
