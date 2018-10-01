from __future__ import absolute_import
from pika.exceptions import (ProbableAuthenticationError, AMQPChannelError, AMQPConnectionError, AMQPError,
                             ChannelClosed, ChannelError, AuthenticationError, BodyTooLongError, ConnectionClosed,
                             ConsumerCancelled, DuplicateConsumerTag, IncompatibleProtocolError, InvalidChannelNumber,
                             InvalidFieldTypeException, InvalidFrameError, InvalidMaximumFrameSize,
                             InvalidMinimumFrameSize, MethodNotImplemented, NackError, NoFreeChannels,
                             ProbableAccessDeniedError, ProtocolSyntaxError, ProtocolVersionMismatch,
                             ShortStringTooLong, UnexpectedFrameError, UnroutableError, UnsupportedAMQPFieldException)


class AMQPException(Exception):
    pass


class MessageProcessError(AMQPException):
    pass


class QueueEmpty(AMQPException):
    pass


class TransactionClosed(AMQPException):
    pass


__all__ = (
    'AMQPChannelError',
    'AMQPConnectionError',
    'AMQPError',
    'AMQPException',
    'AuthenticationError',
    'BodyTooLongError',
    'ChannelClosed',
    'ChannelError',
    'ConnectionClosed',
    'ConsumerCancelled',
    'DuplicateConsumerTag',
    'IncompatibleProtocolError',
    'InvalidChannelNumber',
    'InvalidFieldTypeException',
    'InvalidFrameError',
    'InvalidMaximumFrameSize',
    'InvalidMinimumFrameSize',
    'MessageProcessError',
    'MethodNotImplemented',
    'NackError',
    'NoFreeChannels',
    'ProbableAccessDeniedError',
    'ProbableAuthenticationError',
    'ProtocolSyntaxError',
    'ProtocolVersionMismatch',
    'QueueEmpty',
    'ShortStringTooLong',
    'TransactionClosed',
    'UnexpectedFrameError',
    'UnroutableError',
    'UnsupportedAMQPFieldException',
)

try:
    ConnectionError
except NameError:

    class ConnectionError(Exception):
        pass

    class ConnectionRefusedError(ConnectionError):
        pass
