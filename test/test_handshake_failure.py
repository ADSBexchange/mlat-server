# -*- mode: python; indent-tabs-mode: nil -*-

"""A connection whose handshake is denied is closed without entering the message loop.

Needs the Cython extensions built (python3 setup.py build_ext --inplace):
    python3 -m unittest test.test_handshake_failure
"""

import asyncio
import unittest

import ujson

from mlat import jsonclient


class _Transport:
    def __init__(self):
        self.closed = False

    def get_extra_info(self, name):
        return ('10.0.0.1', 40000) if name == 'peername' else None

    def close(self):
        self.closed = True


class _Writer:
    def __init__(self):
        self.transport = _Transport()
        self.lines = []

    def write(self, data):
        self.lines.append(data.decode('ascii'))


class _HandshakeLogger:
    def debug(self, line):
        pass


class _Coordinator:
    def __init__(self, loop):
        self.loop = loop
        self.handshake_logger = _HandshakeLogger()
        self.usernames = {}


class _Client(jsonclient.JsonClient):
    entered_message_loop = False

    async def handle_line_messages(self):
        _Client.entered_message_loop = True


class HandshakeFailureTests(unittest.TestCase):
    def _run(self, handshake_line):
        _Client.entered_message_loop = False
        exceptions = []

        async def go():
            reader = asyncio.StreamReader()
            reader.feed_data(handshake_line)
            reader.feed_eof()
            writer = _Writer()
            client = _Client(reader, writer, coordinator=_Coordinator(asyncio.get_running_loop()),
                             motd='', udp_protocol=None, udp_host=None, udp_port=None)
            client.logger.exception = lambda *a, **k: exceptions.append(a)
            try:
                await client._read_task
            except asyncio.CancelledError:
                pass  # close() cancels the connection's own read task
            return writer

        writer = asyncio.run(go())
        return writer, exceptions

    def _assert_denied_and_closed(self, writer, exceptions):
        self.assertEqual(exceptions, [])
        self.assertFalse(_Client.entered_message_loop)
        self.assertTrue(writer.transport.closed)
        self.assertEqual(len(writer.lines), 1)
        self.assertIn('deny', ujson.loads(writer.lines[0]))

    def test_malformed_handshake(self):
        self._assert_denied_and_closed(*self._run(b'not json\n'))

    def test_denied_after_compression_negotiated(self):
        hs = {'version': 3, 'user': 'tester', 'compress': ['none'],
              'lat': 95.0, 'lon': 10.0, 'alt': 100.0}
        self._assert_denied_and_closed(*self._run((ujson.dumps(hs) + '\n').encode('ascii')))


if __name__ == '__main__':
    unittest.main()
