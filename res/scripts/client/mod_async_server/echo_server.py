from async import async, await
from debug_utils import LOG_NOTE
from mod_async_server import open_server


@async
def echo(reader, writer, addr):
    LOG_NOTE("[{host}]:{port} connected".format(host=addr[0], port=addr[1]))
    try:
        while True:
            data = yield await(reader(1024))
            LOG_NOTE("Received data from [{host}]:{port}: {data}".format(host=addr[0], port=addr[1], data=data))
            yield await(writer(data))
    finally:
        LOG_NOTE("[{host}]:{port} disconnected".format(host=addr[0], port=addr[1]))


@async
def serve_forever(port):
    with open_server(echo, port) as poll:
        while True:
            yield await(poll.poll_next_frame())
