import errno
import select
import socket
from typing import Callable, Dict, List

import BigWorld
from async import AsyncEvent, AsyncSemaphore, _Future, async, await, await_callback
from BWUtil import AsyncReturn
from debug_utils import LOG_CURRENT_EXCEPTION, LOG_ERROR, LOG_WARNING

DISCONNECTED = {
    errno.ECONNRESET,
    errno.WSAECONNRESET,
    errno.ENOTCONN,
    errno.WSAENOTCONN,
    errno.ESHUTDOWN,
    errno.WSAESHUTDOWN,
    errno.ECONNABORTED,
    errno.WSAECONNABORTED,
    errno.EPIPE,
    errno.EBADF,
    errno.WSAEBADF,
}

BLOCKS = {errno.EAGAIN, errno.EWOULDBLOCK, errno.WSAEWOULDBLOCK}


class SelectParkingLot(object):
    def __init__(self):
        self._closed = False
        self._readers = dict()  # type: Dict[int, AsyncEvent]
        self._writers = dict()  # type: Dict[int, AsyncEvent]

    @async
    def park_read(self, sock):
        # type: (socket.socket) -> _Future
        if not self._closed:
            yield await(self._park(self._readers, sock).wait())

    @async
    def park_write(self, sock):
        # type: (socket.socket) -> _Future
        if not self._closed:
            yield await(self._park(self._writers, sock).wait())

    def poll_sockets(self):
        # type: () -> None
        read_fds = self._readers.keys()
        write_fds = self._writers.keys()

        ready_read_fds, ready_write_fds, _ = select.select(read_fds, write_fds, [], 0)

        self._wake_up(self._readers, ready_read_fds)
        self._wake_up(self._writers, ready_write_fds)

    def close(self):
        self._closed = True
        self._wake_up(self._readers, self._readers.keys())
        self._wake_up(self._writers, self._writers.keys())

    @staticmethod
    def _wake_up(parked, ready_sock_fds):
        # type: (Dict[int, AsyncEvent], List[int]) -> None
        for ready in ready_sock_fds:
            if ready in parked:
                event = parked[ready]
                del parked[ready]
                event.set()

    @staticmethod
    def _park(parked, sock):
        # type: (Dict[int, AsyncEvent], socket.socket) -> AsyncEvent
        sock_fd = sock.fileno()
        if sock_fd not in parked:
            parked[sock_fd] = AsyncEvent()
        return parked[sock_fd]


class StreamClosed(Exception):
    pass


class Stream(object):
    def __init__(self, parking_lot, sock):
        # type: (SelectParkingLot, socket.socket) -> None
        self._parking_lot = parking_lot
        self._sock = sock
        self._write_mutex = AsyncSemaphore(1)
        self._addr = self._sock.getsockname()[:2]
        self._peer_addr = self._sock.getpeername()[:2]

    @property
    def addr(self):
        return self._addr

    @property
    def peer_addr(self):
        return self._peer_addr

    def close(self):
        self._sock.close()

    @async
    def receive(self, max_length):
        # type: (int) -> _Future
        while True:
            try:
                data = self._sock.recv(max_length)
            except socket.error as e:
                if e.args[0] in BLOCKS:
                    # socket not ready, wait until socket is ready
                    yield await(self._parking_lot.park_read(self._sock))
                elif e.args[0] in DISCONNECTED:
                    raise StreamClosed()
                else:
                    raise
            else:
                if not data:
                    raise StreamClosed()
                else:
                    raise AsyncReturn(data)

    @async
    def send(self, data):
        # type: (str) -> _Future
        yield await(self._write_mutex.acquire())
        try:
            yield await(self._do_send(data))
        finally:
            self._write_mutex.release()

    @async
    def _do_send(self, data):
        while data:
            try:
                bytes_sent = self._sock.send(data[:512])
            except socket.error as e:
                if e.args[0] in BLOCKS:
                    # socket not ready, wait until socket is ready
                    yield await(self._parking_lot.park_write(self._sock))
                elif e.args[0] in DISCONNECTED:
                    raise StreamClosed()
                else:
                    raise
            else:
                if bytes_sent < min(len(data), 512):
                    # not everything has been sent, wait until socket is ready again
                    yield await(self._parking_lot.park_write(self._sock))
                data = data[bytes_sent:]


class ServerClosed(Exception):
    pass


class Server(object):
    def __init__(self, protocol, port, host="localhost", connection_limit=8):
        # type: (Callable[[Server, Stream], _Future], int, str, int) -> None
        self._parking_lot = SelectParkingLot()
        self._listening_sock = create_listening_socket(host, port)
        self._connections = dict()  # type: Dict[int, socket.socket]
        self._connection_limit = connection_limit
        self._protocol = protocol
        self._closed = False
        self._start_accepting()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def closed(self):
        # type: () -> bool
        return self._closed

    def poll(self):
        # type: () -> None
        if self._closed:
            raise ServerClosed()

        self._parking_lot.poll_sockets()

    def close(self):
        # type: () -> None
        self._closed = True
        self._listening_sock.close()
        for sock in self._connections.itervalues():
            sock.close()

        # wake up waiting futures to clean up protocol instances
        self._parking_lot.close()

    @async
    def _start_accepting(self):
        # type: () -> _Future
        try:
            while True:
                yield await(self._parking_lot.park_read(self._listening_sock))
                sock, _ = self._listening_sock.accept()
                sock.setblocking(0)
                if len(self._connections) < self._connection_limit:
                    self._accept_connection(sock)
                else:
                    sock.close()
        except socket.error as e:
            if e.args[0] in DISCONNECTED:
                pass
        except Exception:
            LOG_ERROR("UNHANDLED ERROR IN ACCEPT LOOP:")
            LOG_CURRENT_EXCEPTION()
        finally:
            self.close()

    @async
    def _accept_connection(self, sock):
        # type: (socket.socket) -> _Future
        sock_fd = sock.fileno()
        stream = Stream(self._parking_lot, sock)
        self._connections[sock_fd] = sock
        try:
            yield await(self._protocol(self, stream))
        except StreamClosed:
            pass
        except Exception:
            LOG_WARNING("UNHANDLED ERROR IN PROTOCOL:")
            LOG_CURRENT_EXCEPTION()
        finally:
            del self._connections[sock_fd]
            sock.close()


def create_listening_socket(host, port):
    # type: (str, int) -> socket.socket
    fam, _, _, _, addr = socket.getaddrinfo(host, port)[0]
    sock = socket.socket(fam, socket.SOCK_STREAM)
    sock.setblocking(0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen(5)
    return sock


@async
def delay(timeout):
    def callback_wrapper(callback):
        BigWorld.callback(timeout, callback)

    yield await_callback(callback_wrapper)()
