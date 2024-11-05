"""MDP 0.2 Client implementation
"""

# Copyright (c) 2018 Shoppimon LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections.abc import AsyncGenerator
import logging
from types import TracebackType
from typing import Self


import zmq
import zmq.asyncio as azmq

from . import error as e
from . import protocol as p
from .util import TextOrBytes, text_to_ascii_bytes

DEFAULT_ZMQ_LINGER = 2000


class Client:
    """MDP 0.2 Client implementation

    :ivar _socket: zmq.Socket
    :type _socket: zmq.Socket
    """

    def __init__(self, broker_url: str, zmq_linger: int = DEFAULT_ZMQ_LINGER) -> None:
        self.broker_url = broker_url
        self._socket: azmq.Socket | None = None
        self._zmq_context = azmq.Context.instance()
        self._linger = zmq_linger
        self._log = logging.getLogger(__name__)
        self._expect_reply = False

    def connect(self, reconnect: bool = False) -> None:
        if self.is_connected():
            if not reconnect:
                return
            self._disconnect()

        # Set up socket
        self._socket = self._zmq_context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, self._linger)
        self._socket.connect(self.broker_url)
        self._log.debug("Connected to broker on ZMQ DEALER socket at %s", self.broker_url)
        self._expect_reply = False

    def close(self)->None:
        if not self.is_connected():
            return
        self._disconnect()

    def _disconnect(self)->None:
        if not self.is_connected():
            return
        assert self._socket is not None
        self._log.debug("Disconnecting from broker on ZMQ DEALER socket at %s", self.broker_url)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.disconnect(self.broker_url)
        self._socket.close()
        self._socket = None

    def is_connected(self) -> bool:
        """Tell whether we are currently connected"""
        return self._socket is not None

    async def send(self, service: TextOrBytes, *args: bytes) -> None:
        """Send a REQUEST command to the broker to be passed to the given service.

        Each additional argument will be sent as a request body frame.
        """
        assert self._socket is not None
        if self._expect_reply:
            raise e.StateError("Still expecting reply from broker, cannot send new request")

        service = text_to_ascii_bytes(service)
        self._log.debug("Sending REQUEST message to %s with %d frames in body", service, len(args))
        await self._socket.send_multipart((b"", p.CLIENT_HEADER, p.REQUEST, service) + args)
        self._expect_reply = True

    async def recv_part(self, timeout:float|None=None)->list[bytes]|None:
        """Receive a single part of the reply, partial or final

        Note that a "part" is actually a list in this case, as any reply part can contain multiple frames.

        If there are no more parts to receive, will return None
        """
        assert self._socket is not None
        if not self._expect_reply:
            return None

        timeout = int(timeout * 1000) if timeout else None

        poller = azmq.Poller()
        poller.register(self._socket, zmq.POLLIN)

        try:
            socks = dict(await poller.poll(timeout=timeout))
            if socks.get(self._socket) == zmq.POLLIN:
                message = await self._socket.recv_multipart()
                m_type, m_content = self._parse_message(message)
                if m_type == p.FINAL:
                    self._expect_reply = False
                return m_content
            else:
                raise e.Timeout("Timed out waiting for reply from broker")
        finally:
            poller.unregister(self._socket)

    async def recv_all(self, timeout: float | None = None) -> AsyncGenerator[list[bytes]]:
        """Return a generator allowing to iterate over all reply parts

        Note that `timeout` applies to each part, not to the full list of parts
        """
        while True:
            part = await self.recv_part(timeout)
            if part is None:
                break
            yield part

    async def recv_all_as_list(self, timeout: float | None = None) -> list[bytes]:
        """Return all reply parts as a single, flat list of frames"""
        return [frame async for part in self.recv_all(timeout) for frame in part]
        #return [frame for part in self.recv_all(timeout) for frame in part]

    @staticmethod
    def _parse_message(message:list[bytes])->tuple[bytes,list[bytes]]:
        """Parse and validate an incoming message"""
        if len(message) < 3:
            raise e.ProtocolError("Unexpected message length, expecting at least 3 frames, got {}".format(len(message)))

        if message.pop(0) != b"":
            raise e.ProtocolError("Expecting first message frame to be empty")

        if message[0] != p.CLIENT_HEADER:
            print(message)
            raise e.ProtocolError(
                "Unexpected protocol header [{}], expecting [{}]".format(
                    message[0].decode("utf8"), p.WORKER_HEADER.decode("utf8")
                )
            )

        if message[1] not in {p.PARTIAL, p.FINAL}:
            raise e.ProtocolError(
                "Unexpected message type [{}], expecting either PARTIAL or FINAL".format(message[1].decode("utf8"))
            )

        return message[1], message[2:]

    async def __aenter__(self)->Self:
        self.connect()
        return self

    async def __aexit__(self, exc_type:type[BaseException]|None, exc_val:BaseException|None, exc_tb:TracebackType|None)->bool:
        self.close()
        return False

    async def request(self, service:bytes, *frames:bytes,timeout:float|None=None)->list[bytes]:
        await self.send(service, *frames)
        return await self.recv_all_as_list(timeout=timeout)


async def main():
    import sys
    verbose = '-v' in sys.argv
    if verbose:
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG if verbose else logging.INFO)

    client = Client("tcp://localhost:5555")
    async with client:
        #await client.send(b"worker1",b"Hello World")
        count = 0
        while count < 100:
            request = b"Hello world"
            try:
                response = await client.request(b"echo", request)
                print(response)
            except KeyboardInterrupt:
                break

            count += 1
        print ("%i requests/replies processed" % count)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
