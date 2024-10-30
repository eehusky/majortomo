"""MDP 0.2 Worker implementation
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

from collections.abc import Iterable
import logging
import signal
import time
from collections.abc import Sequence
from types import TracebackType
from typing import Self

import zmq
import zmq.asyncio as azmq

from . import error
from . import protocol as p
from .util import TextOrBytes, text_to_ascii_bytes

DEFAULT_ZMQ_LINGER = 2500


class Worker:
    """MDP 0.2 Worker implementation"""

    def __init__(
        self,
        broker_url:str,
        service_name:TextOrBytes,
        heartbeat_interval:float=p.DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_timeout:float=p.DEFAULT_HEARTBEAT_TIMEOUT,
        zmq_linger:int=DEFAULT_ZMQ_LINGER,
    )->None:
        self.broker_url = broker_url
        self.service_name = text_to_ascii_bytes(service_name)
        self.heartbeat_interval = heartbeat_interval

        self._socket:azmq.Socket|None = None
        self._poller:azmq.Poller|None = None
        self._zmq_context = azmq.Context.instance()
        self._linger = zmq_linger
        self._log = logging.getLogger(__name__)
        self._heartbeat_timeout = heartbeat_timeout
        self._last_broker_hb = 0.0
        self._last_sent_message = 0.0

    async def connect(self, reconnect:bool=False)->None:
        if self.is_connected():
            if not reconnect:
                return
            self._disconnect()

        # Set up socket
        self._socket = self._zmq_context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, self._linger)
        self._socket.connect(self.broker_url)
        self._log.debug("Connected to broker on ZMQ DEALER socket at %s", self.broker_url)

        self._poller = azmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)

        await self._send_ready()
        self._last_broker_hb = time.time()

    async def wait_for_request(self)->tuple[bytes,list[bytes]]:
        """Wait for a REQUEST command from the broker and return the client address and message body frames.

        Will internally handle timeouts, heartbeats and check for protocol errors and disconnect commands.
        """
        command, frames = await self._receive()

        if command == p.DISCONNECT:
            self._log.debug("Got DISCONNECT from broker; Disconnecting")
            self._disconnect()
            raise error.Disconnected("Disconnected on message from broker")

        elif command != p.REQUEST:
            raise error.ProtocolError("Unexpected message type from broker: {}".format(command.decode("utf8")))

        if len(frames) < 3:
            raise error.ProtocolError(
                "Unexpected REQUEST message size, got {} frames, expecting at least 3".format(len(frames))
            )

        client_addr = frames[0]
        request = frames[2:]
        return client_addr, request

    async def send_reply_final(self, client:bytes, frames:Sequence[bytes])->None:
        """Send final reply to client

        FINAL reply means the client will not expect any additional parts to the reply. This should be used
        when the entire reply is ready to be delivered.
        """
        await self._send_to_client(client, p.FINAL, *frames)

    async def send_reply_partial(self, client:bytes, frames:Sequence[bytes])->None:
        """Send the given set of frames as a partial reply to client

        PARTIAL reply means the client will expect zero or more additional PARTIAL reply messages following
        this one, with exactly one terminating FINAL reply following. This should be used if parts of the
        reply are ready to be sent, and the client is capable of processing them while the worker is still
        at work on the rest of the reply.
        """
        await self._send_to_client(client, p.PARTIAL, *frames)

    async def send_reply_from_iterable(self, client:bytes, frames_iter:Iterable[Sequence[bytes]], final:Sequence[bytes]|None=None)->None:
        """Send multiple partial replies from an iterator as PARTIAL replies to client.

        If `final` is provided, it will be sent as the FINAL reply after all PARTIAL replies are sent.
        """
        for part in frames_iter:
            await self.send_reply_partial(client, part)
        if final:
            await self.send_reply_final(client, final)

    async def close(self)->None:
        if not self.is_connected():
            return
        await self._send_disconnect()
        self._disconnect()

    def is_connected(self)->bool:
        return self._socket is not None

    def _disconnect(self)->None:
        if not self.is_connected():
            return
        assert self._socket is not None
        self._socket.disconnect(self.broker_url)
        self._socket.close()
        self._socket = None
        self._last_sent_message -= self.heartbeat_interval

    async def _receive(self)->tuple[bytes,list[bytes]]:
        """Poll on the socket until a command is received

        Will handle timeouts and heartbeats internally without returning
        """
        while True:
            if self._socket is None:
                raise error.Disconnected("Worker is disconnected")
            if self._poller is None:
                raise error.Disconnected("Worker is disconnected")

            await self._check_send_heartbeat()
            poll_timeout = self._get_poll_timeout()

            try:
                socks = dict(await self._poller.poll(timeout=poll_timeout))
            except zmq.error.ZMQError:
                # Probably connection was explicitly closed
                if self._socket is None:
                    continue
                raise

            if socks.get(self._socket) == zmq.POLLIN:
                message = await self._socket.recv_multipart()
                self._log.debug("Got message of %d frames", len(message))
            else:
                self._log.debug("Receive timed out after %d ms", poll_timeout)
                if (time.time() - self._last_broker_hb) > self._heartbeat_timeout:
                    # We're not connected anymore?
                    self._log.info(
                        "Got no heartbeat in %d sec, disconnecting and reconnecting socket", self._heartbeat_timeout
                    )
                    await self.connect(reconnect=True)
                continue

            command, frames = self._verify_message(message)
            self._last_broker_hb = time.time()

            if command == p.HEARTBEAT:
                self._log.debug("Got heartbeat message from broker")
                continue

            return command, frames

    async def _send_ready(self)->None:
        await self._send(p.READY, self.service_name)

    async def _send_disconnect(self)->None:
        await self._send(p.DISCONNECT)

    async def _check_send_heartbeat(self)->None:
        if time.time() - self._last_sent_message >= self.heartbeat_interval:
            self._log.debug("Sending HEARTBEAT to broker")
            await self._send(p.HEARTBEAT)

    async def _send_to_client(self, client:bytes, message_type:bytes, *frames:bytes)->None:
        await self._send(message_type, client, b"", *frames)

    async def _send(self, message_type:bytes, *args:bytes)->None:
        assert self._socket is not None
        await self._socket.send_multipart((b"", p.WORKER_HEADER, message_type) + args)
        self._last_sent_message = time.time()

    def _get_poll_timeout(self)->int:
        """Return the poll timeout for the current iteration in milliseconds"""
        return max(0, int((time.time() - self._last_sent_message + self.heartbeat_interval) * 1000))

    @staticmethod
    def _verify_message(message:list[bytes])->tuple[bytes,list[bytes]]:
        if len(message) < 3:
            raise error.ProtocolError(
                "Unexpected message length, expecting at least 3 frames, got {}".format(len(message))
            )

        if message.pop(0) != b"":
            raise error.ProtocolError("Expecting first message frame to be empty")

        if message[0] != p.WORKER_HEADER:
            print(message)
            raise error.ProtocolError(
                "Unexpected protocol header [{}], expecting [{}]".format(
                    message[0].decode("utf8"), p.WORKER_HEADER.decode("utf8")
                )
            )

        if message[1] not in {p.DISCONNECT, p.HEARTBEAT, p.REQUEST}:
            raise error.ProtocolError(
                "Unexpected message type [{}], expecting either HEARTBEAT, REQUEST or "
                "DISCONNECT".format(message[1].decode("utf8"))
            )

        return message[1], message[2:]

    async def __aenter__(self)->Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type:type[Exception], exc_val:Exception, exc_tb:TracebackType)->bool:
        await self.close()
        return False



async def main():
    import sys
    verbose = '-v' in sys.argv
    if verbose:
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG if verbose else logging.INFO)

    worker = Worker("tcp://localhost:5555",b"echo")
    async with worker:
        reply = None
        while True:
            try:
                client, request = await worker.wait_for_request()
                await worker.send_reply_final(client,request)
            except (error.Error,asyncio.exceptions.CancelledError):
                print('disconneced')
                break
            except (asyncio.exceptions.CancelledError, KeyboardInterrupt):
                break


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
