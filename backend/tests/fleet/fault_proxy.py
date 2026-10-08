"""A loopback TCP relay that cuts existing and future worker control connections."""

import asyncio
from contextlib import suppress


class FaultProxy:
    def __init__(self, upstream_host, upstream_port):
        self.upstream = (upstream_host, upstream_port)
        self.blocked = False
        self.writers = set()
        self.tasks = set()
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(self._connection, "127.0.0.1", 0)
        return "http://127.0.0.1:" + str(self.server.sockets[0].getsockname()[1])

    def cut(self):
        self.blocked = True
        for writer in tuple(self.writers):
            writer.close()

    def restore(self):
        self.blocked = False

    async def _connection(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        self.writers.add(writer)
        try:
            if self.blocked:
                return
            other_reader, upstream = await asyncio.open_connection(*self.upstream)
            self.writers.add(upstream)
            if self.blocked:
                return

            async def relay(source, target):
                while data := await source.read(65536):
                    target.write(data)
                    await target.drain()

            pair = [asyncio.create_task(relay(reader, upstream)), asyncio.create_task(relay(other_reader, writer))]
            try:
                await asyncio.wait(pair, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for pending in pair:
                    pending.cancel()
                await asyncio.gather(*pair, return_exceptions=True)
        except (OSError, ConnectionError):
            pass
        finally:
            for connection in (writer, upstream):
                if connection is not None:
                    self.writers.discard(connection)
                    connection.close()
                    with suppress(OSError, ConnectionError):
                        await connection.wait_closed()
            self.tasks.discard(task)

    async def close(self):
        self.cut()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
