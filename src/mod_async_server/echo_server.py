from debug_utils import LOG_NOTE
from mod_async import async_task, delay
from mod_async_server import Server


@async_task
def echo_protocol(server, stream):
    host, port = stream.peer_addr
    LOG_NOTE("[{host}]:{port} connected".format(host=host, port=port))
    try:
        # run until the client closes the connection.
        # the connection will be closed automatically when this function exits.
        # when the connection gets closed by the peer,
        # stream.read and stream.write will raise a StreamClosed exception.
        while True:
            # wait until we receive some data, at most 1024 bytes.
            data = yield stream.receive(1024)
            LOG_NOTE(
                "Received data from [{host}]:{port}: {data}".format(
                    host=host, port=port, data=data
                )
            )
            # echo the data back to client, wait until everything has been sent.
            yield stream.send(data)
    finally:
        # server closed or peer disconnected.
        LOG_NOTE("[{host}]:{port} disconnected".format(host=host, port=port))


@async_task
def serve_forever():
    # open server on localhost:4000 using the `echo_protocol` for serving individual connections.
    with Server(echo_protocol, 4000) as server:
        # serve forever, serve once per frame.
        while not server.closed:
            server.poll()
            # delay next loop iteration into the next frame.
            yield delay(0)
