import errno
import select
import socket
from typing import Optional

from mod_async import AsyncMutex, AsyncValue, Return, async_task, auto_run

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
        self._readers = dict()
        self._writers = dict()

    @async_task
    def park_read(self, sock):
        if not self._closed:
            yield self._park(self._readers, sock)

    @async_task
    def park_write(self, sock):
        if not self._closed:
            yield self._park(self._writers, sock)

    def poll_sockets(self):
        read_fds = self._readers.keys()
        write_fds = self._writers.keys()

        ready_read_fds, ready_write_fds, _ = select.select(read_fds, write_fds, [], 0)

        self._wake_up(self._readers, ready_read_fds)
        self._wake_up(self._writers, ready_write_fds)

    def close_socket(self, sock):
        sock_fd = sock.fileno()
        sock.close()
        self._wake_up(self._readers, (sock_fd,))
        self._wake_up(self._writers, (sock_fd,))

    def close(self):
        self._closed = True
        self._wake_up(self._readers, self._readers.keys())
        self._wake_up(self._writers, self._writers.keys())

    @staticmethod
    def _wake_up(parked, ready_sock_fds):
        for ready in ready_sock_fds:
            if ready in parked:
                deferred = parked[ready]
                del parked[ready]
                deferred.set()

    @staticmethod
    def _park(parked, sock):
        sock_fd = sock.fileno()
        if sock_fd not in parked:
            parked[sock_fd] = AsyncValue()
        return parked[sock_fd]


class StreamClosed(Exception):
    pass


class Stream(object):
    def __init__(self, parking_lot, sock):
        self._parking_lot = parking_lot
        self._sock = sock
        self._write_mutex = AsyncMutex()
        self._addr = self._sock.getsockname()[:2]
        self._peer_addr = self._sock.getpeername()[:2]

    @property
    def addr(self):
        return self._addr

    @property
    def peer_addr(self):
        return self._peer_addr

    def close(self):
        self._parking_lot.close_socket(self._sock)

    @async_task
    def receive(self, max_length):
        while True:
            try:
                data = self._sock.recv(max_length)
            except socket.error as e:
                if e.args[0] in BLOCKS:
                    # socket not ready, wait until socket is ready
                    yield self._parking_lot.park_read(self._sock)
                elif e.args[0] in DISCONNECTED:
                    raise StreamClosed()
                else:
                    raise
            else:
                if not data:
                    raise StreamClosed()
                else:
                    raise Return(data)

    @async_task
    def send(self, data):
        yield self._write_mutex.acquire()
        try:
            yield self._do_send(data)
        finally:
            self._write_mutex.release()

    @async_task
    def _do_send(self, data):
        data = memoryview(data)
        while data:
            try:
                bytes_sent = self._sock.send(data[:512])
            except socket.error as e:
                if e.args[0] in BLOCKS:
                    # socket not ready, wait until socket is ready
                    yield self._parking_lot.park_write(self._sock)
                elif e.args[0] in DISCONNECTED:
                    raise StreamClosed()
                else:
                    raise
            else:
                if bytes_sent < min(len(data), 512):
                    # not everything has been sent, wait until socket is ready again
                    yield self._parking_lot.park_write(self._sock)
                data = data[bytes_sent:]


class ServerClosed(Exception):
    pass


class Server(object):
    def __init__(self, protocol, port, host="localhost", connection_limit=8):
        self.port = port
        self.host = host
        self._parking_lot = None  # type: Optional[SelectParkingLot]
        self._listening_sock = None  # type: Optional[socket.socket]
        self._connections = dict()
        self._connection_limit = connection_limit
        self._protocol = protocol

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def closed(self):
        return self._listening_sock is None

    def open(self):
        if not self.closed:
            return

        self._listening_sock = create_listening_socket(self.host, self.port)
        self._parking_lot = SelectParkingLot()

        self._start_accepting()

    def poll(self):
        if self.closed:
            raise ServerClosed()

        self._parking_lot.poll_sockets()

    def close(self):
        if self.closed:
            return

        self._listening_sock.close()
        self._listening_sock = None

        for sock in self._connections.itervalues():
            sock.close()

        # wake up waiting futures to clean up protocol instances
        self._parking_lot.close()
        self._parking_lot = None

    @auto_run
    @async_task
    def _start_accepting(self):
        try:
            while True:
                yield self._parking_lot.park_read(self._listening_sock)
                if self.closed:
                    return

                sock, _ = self._listening_sock.accept()
                sock.setblocking(0)
                if len(self._connections) < self._connection_limit:
                    self._accept_connection(sock)
                else:
                    sock.close()
        except socket.error as e:
            if e.args[0] in DISCONNECTED:
                pass
        finally:
            self.close()

    @auto_run
    @async_task
    def _accept_connection(self, sock):
        sock_fd = sock.fileno()
        stream = Stream(self._parking_lot, sock)
        self._connections[sock_fd] = sock
        try:
            yield self._protocol(self, stream)
        except StreamClosed:
            pass
        finally:
            del self._connections[sock_fd]
            sock.close()


def create_listening_socket(host, port):
    fam, _, _, _, addr = socket.getaddrinfo(host, port)[0]
    sock = socket.socket(fam, socket.SOCK_STREAM)
    sock.setblocking(0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen(5)
    return sock
