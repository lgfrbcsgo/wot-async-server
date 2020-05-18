# WoT Async Server
A single threaded, non blocking TCP server which makes use of the `async` / `await` primitives of WoT.

```python
from async import async, await
from debug_utils import LOG_NOTE
from mod_async_server import open_server


@async
def echo(reader, writer, addr):
    # client connected
    LOG_NOTE("[{host}]:{port} connected".format(host=addr[0], port=addr[1]))
    try:
        # run until the client closes the connection
        while True:
            # wait until we receive some data, at most 1024 bytes
            data = yield await(reader(1024))
            LOG_NOTE("Received: {data}".format(data=data))
            # echo the data back to client, wait until everything has been sent
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


# start serving, does not block the current thread
serve()
```