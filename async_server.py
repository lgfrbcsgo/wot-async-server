import errno
import select
import socket
from contextlib import contextmanager
from functools import partial
from typing import Any, Callable, Dict

from async import AsyncEvent, AsyncSemaphore, _Future, async, await
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


class DisconnectEvent(Exception):
    pass


class SocketParkingLot(object):
    def __init__(self):
        self._readers = dict()  # type: Dict[int, AsyncEvent]
        self._writers = dict()  # type: Dict[int, AsyncEvent]

    @async
    def park_read(self, sock, invalidate=True):
        # type: (socket.socket, bool) -> _Future
        yield await(self._park(self._readers, sock, invalidate).wait())

    @async
    def park_write(self, sock, invalidate=True):
        # type: (socket.socket, bool) -> _Future
        yield await(self._park(self._writers, sock, invalidate).wait())

    def poll_sockets(self):
        # type: () -> None
        readers = self._readers.keys()
        writers = self._writers.keys()

        ready_readers, ready_writers, _ = select.select(readers, writers, [], 0)

        for reader in ready_readers:
            event = self._readers[reader]
            del self._readers[reader]
            event.set()

        for writer in ready_writers:
            event = self._writers[writer]
            del self._writers[writer]
            event.set()

    @staticmethod
    def _park(parked, sock, invalidate):
        # type: (Dict[int, AsyncEvent], socket.socket, bool) -> AsyncEvent
        if sock.fileno() not in parked:
            parked[sock.fileno()] = AsyncEvent()
        elif invalidate:
            parked[sock.fileno()].clear()
        return parked[sock.fileno()]


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
    # type: (SocketParkingLot, socket.socket, int) -> _Future
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
    # type: (SocketParkingLot, AsyncSemaphore, socket.socket, str) -> _Future
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
def accept_loop(listening_sock, parking_lot, accept_callback):
    # type: (socket.socket, SocketParkingLot, Callable[[socket.socket], Any]) -> _Future
    while True:
        yield await(parking_lot.park_read(listening_sock))
        sock, _ = listening_sock.accept()
        sock.setblocking(0)
        accept_callback(sock)


@async
def handle_socket(protocol, connected_socks, parking_lot, sock):
    # type: (Any, Dict[int, socket.socket], SocketParkingLot, socket.socket) -> _Future
    connected_socks[sock.fileno()] = sock
    try:
        reader = partial(read, parking_lot, sock)
        writer = partial(write, parking_lot, AsyncSemaphore(), sock)
        yield await(protocol(reader, writer, sock.getpeername()))
    except DisconnectEvent:
        pass
    finally:
        del connected_socks[sock.fileno()]
        sock.close()


@contextmanager
def open_server(protocol, port, host="localhost"):
    listening_sock = create_listening_socket(host, port)
    connected_socks = dict()  # type: Dict[int, socket.socket]
    try:
        parking_lot = SocketParkingLot()
        accept_loop(
            listening_sock,
            parking_lot,
            lambda sock: handle_socket(protocol, connected_socks, parking_lot, sock),
        )
        yield parking_lot.poll_sockets
    finally:
        listening_sock.close()
        for connected_sock in connected_socks.itervalues():
            connected_sock.close()
