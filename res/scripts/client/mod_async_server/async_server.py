import errno
import select
import socket
from contextlib import contextmanager
from typing import Any, Callable, ContextManager, Dict, List

import BigWorld
from async import (
    AsyncEvent,
    AsyncSemaphore,
    _Future,
    async,
    await,
    await_callback,
    delay,
)
from BWUtil import AsyncReturn

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

Protocol = Callable[[Callable[[int], _Future], Callable[[str], _Future], Any], None]


class DisconnectEvent(Exception):
    pass


class SelectParkingLot(object):
    def __init__(self):
        self._closed = False
        self._readers = dict()  # type: Dict[int, AsyncEvent]
        self._writers = dict()  # type: Dict[int, AsyncEvent]

    @async
    def park_read(self, sock, invalidate=True):
        # type: (socket.socket, bool) -> _Future
        if not self._closed:
            yield await(self._park(self._readers, sock, invalidate).wait())

    @async
    def park_write(self, sock, invalidate=True):
        # type: (socket.socket, bool) -> _Future
        if not self._closed:
            yield await(self._park(self._writers, sock, invalidate).wait())

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
            event = parked[ready]
            del parked[ready]
            event.set()

    @staticmethod
    def _park(parked, sock, invalidate):
        # type: (Dict[int, AsyncEvent], socket.socket, bool) -> AsyncEvent
        sock_fd = sock.fileno()
        if sock_fd not in parked:
            parked[sock_fd] = AsyncEvent()
        elif invalidate:
            parked[sock_fd].clear()
        return parked[sock_fd]


@async
def tick():
    def callback_wrapper(callback):
        BigWorld.callback(0, callback)

    yield await_callback(callback_wrapper)()


class PollingManager(object):
    def __init__(self, poll_callback):
        # type: (Callable[[], None]) -> None
        self._poll_callback = poll_callback

    def poll(self):
        # type: () -> None
        self._poll_callback()

    @async
    def poll_next_frame(self):
        # type: () -> _Future
        yield await(tick())
        self.poll()

    @async
    def poll_after(self, timeout):
        yield await(delay(timeout))
        self.poll()


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
def read(parking_lot, sock, max_length):
    # type: (SelectParkingLot, socket.socket, int) -> _Future
    invalidate = False
    while True:
        yield await(parking_lot.park_read(sock, invalidate))
        try:
            data = sock.recv(max_length)
        except socket.error as e:
            if e.args[0] in BLOCKS:
                invalidate = True
            elif e.args[0] in DISCONNECTED:
                raise DisconnectEvent()
            else:
                raise
        else:
            if not data:
                raise DisconnectEvent()
            raise AsyncReturn(data)


@async
def write(parking_lot, write_semaphore, sock, data):
    # type: (SelectParkingLot, AsyncSemaphore, socket.socket, str) -> _Future
    invalidate = False
    yield await(write_semaphore.acquire())
    try:
        while data:
            yield await(parking_lot.park_write(sock, invalidate))
            try:
                bytes_sent = sock.send(data[:512])
                if bytes_sent < min(len(data), 512):
                    invalidate = True
                data = data[bytes_sent:]
            except socket.error as e:
                if e.args[0] in BLOCKS:
                    invalidate = True
                elif e.args[0] in DISCONNECTED:
                    raise DisconnectEvent()
                else:
                    raise

    finally:
        write_semaphore.release()


@async
def run_accept_loop(parking_lot, listening_sock, accept_callback):
    # type: (SelectParkingLot, socket.socket, Callable[[socket.socket], Any]) -> _Future
    while True:
        yield await(parking_lot.park_read(listening_sock))
        try:
            sock, _ = listening_sock.accept()
        except socket.error as e:
            if e.args[0] in DISCONNECTED:
                return
            else:
                raise
        else:
            sock.setblocking(0)
            accept_callback(sock)


@async
def handle_socket(parking_lot, connected_socks, protocol, sock):
    # type: (SelectParkingLot, Dict[int, socket.socket], Protocol, socket.socket) -> _Future
    sock_fd = sock.fileno()
    connected_socks[sock_fd] = sock
    try:
        write_semaphore = AsyncSemaphore()
        yield await(
            protocol(
                lambda max_length: read(parking_lot, sock, max_length),
                lambda data: write(parking_lot, write_semaphore, sock, data),
                sock.getpeername(),
            )
        )
    except DisconnectEvent:
        pass
    finally:
        del connected_socks[sock_fd]
        sock.close()


@contextmanager
def open_server(protocol, port, host="localhost"):
    # type: (Protocol, int, str) -> ContextManager[PollingManager]
    listening_sock = create_listening_socket(host, port)
    connected_socks = dict()  # type: Dict[int, socket.socket]
    parking_lot = SelectParkingLot()
    try:
        run_accept_loop(parking_lot, listening_sock,
                        lambda sock: handle_socket(parking_lot, connected_socks, protocol, sock))
        yield PollingManager(parking_lot.poll_sockets)
    finally:
        listening_sock.close()
        for connected_sock in connected_socks.itervalues():
            connected_sock.close()

        # wake up waiting futures to clean up protocol instances
        parking_lot.close()
