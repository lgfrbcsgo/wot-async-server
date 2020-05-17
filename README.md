# WoT Async Server
A non blocking TCP server which makes use of the `async` / `await` primitives of WoT.

```python
from async import async, await
from async_server import open_server
from debug_utils import LOG_NOTE


@async
def echo(reader, writer, addr):
    # client connected
    LOG_NOTE("[{host}]:{port} connected".format(host=addr[0], port=addr[1]))
    try:
        # run until the client closes the connection
        while True:
            # wait until we receive some data, at most 1024 byte
            data = yield await(reader(1024))
            LOG_NOTE("Received: {data}".format(data=data))
            # echo data back to client, wait until all data has been sent
            yield await(writer(data))
    finally:
        # server closed or client disconnected
        LOG_NOTE("[{host}]:{port} disconnected".format(host=addr[0], port=addr[1]))


@async
def serve():
    # open server on localhost:4000 using the `echo` protocol for serving individual connections
    with open_server(echo, 4000) as poll:
        # serve forever, serve once per frame
        while True:
            yield await(poll.poll_next_frame())


# start serving
serve()
```