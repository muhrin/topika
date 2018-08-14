import contextlib

try:
    from typing import Awaitable
except ImportError:
    from future.utils import with_metaclass
    from typing import Generic, TypeVar
    from abc import ABCMeta, abstractmethod

    T_co = TypeVar('T_co', covariant=True)


    class _Awaitable(with_metaclass(ABCMeta)):

        __slots__ = ()

        @abstractmethod
        def __await__(self):
            yield


    class Awaitable(Generic[T_co], _Awaitable):
        __slots__ = ()

try:
    from contextlib import suppress
except ImportError:
    @contextlib.contextmanager
    def suppress(*exceptions):
        """
        An attempt to emulate python3's contextlib.suppress
        :param exceptions: The list of exceptions to suppress, all if none are supplied
        """
        exc = exceptions or Exception
        try:
            yield
        except exc:
            pass

try:
    # Either take the native ones
    ConnectionError = ConnectionError
    ConnectionRefusedError = ConnectionRefusedError
except NameError:
    # Or define these ourselves (python2)
    class ConnectionError(Exception):
        pass


    class ConnectionRefusedError(ConnectionError):
        pass

try:
    from contextlib import suppress
except ImportError:
    @contextlib.contextmanager
    def suppress(*exceptions):
        excs = exceptions or Exception
        try:
            yield
        except excs:
            pass

__all__ = ('ConnectionError', 'ConnectionRefusedError')
