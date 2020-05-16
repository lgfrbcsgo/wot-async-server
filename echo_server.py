import BigWorld
from async import async, await, await_callback
from async_server import open_server
from debug_utils import LOG_NOTE


@async
def echo(reader, writer, addr):
    LOG_NOTE("[{host}]:{port} connected".format(host=addr[0], port=addr[1]))
    try:
        while True:
            data = yield await(reader(1024))
            LOG_NOTE("Echo: {data}".format(data=data))
            yield await(writer(data))
    finally:
        LOG_NOTE("[{host}]:{port} disconnected".format(host=addr[0], port=addr[1]))


@async
def tick():
    def callback_wrapper(callback):
        BigWorld.callback(0, callback)

    yield await_callback(callback_wrapper)()


@async
def serve():
    with open_server(echo, 4000) as poll:
        while True:
            poll()
            yield await(tick())


serve()
