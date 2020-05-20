from async import async, await
from debug_utils import LOG_NOTE
from mod_async_server import Server


@async
def echo(stream):
    # client connected
    host, port = stream.peer_addr
    LOG_NOTE("[{host}]:{port} connected".format(host=host, port=port))
    try:
        # run until the client closes the connection
        while True:
            # wait until we receive some data, at most 1024 bytes
            data = yield await(stream.read(1024))
            LOG_NOTE("Received data from [{host}]:{port}: {data}".format(host=host, port=port, data=data))
            # echo the data back to client, wait until everything has been sent
            yield await(stream.write(data))
    finally:
        # server closed or client disconnected
        LOG_NOTE("[{host}]:{port} disconnected".format(host=host, port=port))


@async
def serve_forever():
    # open server on localhost:4000 using the `echo` protocol for serving individual connections
    with Server(echo, 4000) as server:
        # serve forever, serve once per frame
        while True:
            yield await(server.poll_next_frame())


# start serving, does not block the current thread
serve_forever()
