import pika.exceptions
import tornado.concurrent

__all__ = ('create_future',)


def create_future(loop):
    """ Helper for `create a new future`_ with backward compatibility for Python 3.4

    .. _create a new future: https://goo.gl/YrzGQ6
    """

    try:
        return loop.create_future()
    except AttributeError:
        # Compatibility with older tornado
        return tornado.concurrent.Future()


def ensure_connection_exception(exception_or_message):
    """
    If passed an exception this will be returned.  Otherwise it is assumed
    a string is passed giving the reason for the connection error

    :param exception_or_message:
    :return:
    """
    if isinstance(exception_or_message, Exception):
        return exception_or_message
    else:
        # We got a string message
        return pika.exceptions.AMQPConnectionError(exception_or_message)
