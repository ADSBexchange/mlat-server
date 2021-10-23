# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-server: a Mode S multilateration server
# Copyright (C) 2015  Oliver Jowett <oliver@mutability.co.uk>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
JSON client protocol implementation.
"""

import asyncio
import zlib
import logging
import ujson
import struct
import time
import random
import socket
import inspect
import sys
import math
import re

from mlat import constants, geodesy
from mlat.server import net, util, connection, config


glogger = logging.getLogger("client")
random.seed()


class JsonClientListener(net.MonitoringListener):
    def __init__(self, host, tcp_port, udp_port, motd, coordinator):
        super().__init__(host, tcp_port, None, logger=glogger, description='JSON client handler')
        self.coordinator = coordinator
        self.udp_port = udp_port
        self.motd = motd

        self.udp_transport = None
        self.udp_protocol = None
        self.clients = []

    @asyncio.coroutine
    def _start(self):
        if self.udp_port:
            # asyncio's UDP binding is a bit strange (and different to TCP):
            # a host of None will bind to 127.0.0.1, not the wildcard address.
            bind_address = self.host if self.host else '0.0.0.0'
            dgram_coro = asyncio.get_event_loop().create_datagram_endpoint(protocol_factory=PackedMlatServerProtocol,
                                                                           family=socket.AF_INET,
                                                                           local_addr=(bind_address, self.udp_port))
            self.udp_transport, self.udp_protocol = (yield from dgram_coro)
            name = self.udp_transport.get_extra_info('sockname')
            self.logger.warning("{what} listening on {host}:{port} (UDP)".format(host=name[0],
                                                                              port=name[1],
                                                                              what=self.description))

        yield from super()._start()

    def _new_client(self, r, w):
        return JsonClient(r, w,
                          coordinator=self.coordinator,
                          motd=self.motd,
                          udp_protocol=self.udp_protocol,
                          udp_host=self.host,
                          udp_port=self.udp_port)

    def _close(self):
        super()._close()
        if self.udp_transport:
            self.udp_transport.abort()


class PackedMlatServerProtocol(asyncio.DatagramProtocol):
    TYPE_SYNC = 1
    TYPE_MLAT_SHORT = 2
    TYPE_MLAT_LONG = 3
    TYPE_SSYNC = 4
    TYPE_REBASE = 5
    TYPE_ABS_SYNC = 6

    STRUCT_HEADER = struct.Struct(">IHQ")
    STRUCT_SYNC = struct.Struct(">ii14s14s")
    STRUCT_MLAT_SHORT = struct.Struct(">i7s")
    STRUCT_MLAT_LONG = struct.Struct(">i14s")
    STRUCT_REBASE = struct.Struct(">Q")
    STRUCT_ABS_SYNC = struct.Struct(">QQ14s14s")

    def __init__(self):
        self.clients = {}
        self._r = random.SystemRandom()
        self.listen_address = None

    def add_client(self, sync_handler, mlat_handler):
        newkey = self._r.getrandbits(32)
        while newkey in self.clients:
            newkey = self._r.getrandbits(32)
        self.clients[newkey] = (sync_handler, mlat_handler)
        return newkey

    def remove_client(self, key):
        self.clients.pop(key, None)

    def connection_made(self, transport):
        self.listen_address = transport.get_extra_info('sockname')

    def datagram_received(self, data, addr):
        try:
            key, seq, base = self.STRUCT_HEADER.unpack_from(data, 0)
            sync_handler, mlat_handler = self.clients[key]  # KeyError on bad client key
            utc = time.time()

            i = self.STRUCT_HEADER.size
            while i < len(data):
                typebyte = data[i]
                i += 1

                if typebyte == self.TYPE_SYNC:
                    et, ot, em, om = self.STRUCT_SYNC.unpack_from(data, i)
                    i += self.STRUCT_SYNC.size
                    sync_handler(base + et, base + ot, em, om)

                elif typebyte == self.TYPE_MLAT_SHORT:
                    t, m = self.STRUCT_MLAT_SHORT.unpack_from(data, i)
                    i += self.STRUCT_MLAT_SHORT.size
                    mlat_handler(base + t, m, utc)

                elif typebyte == self.TYPE_MLAT_LONG:
                    t, m = self.STRUCT_MLAT_LONG.unpack_from(data, i)
                    i += self.STRUCT_MLAT_LONG.size
                    mlat_handler(base + t, m, utc)

                elif typebyte == self.TYPE_REBASE:
                    base, = self.STRUCT_REBASE.unpack_from(data, i)
                    i += self.STRUCT_REBASE.size

                elif typebyte == self.TYPE_ABS_SYNC:
                    et, ot, em, om = self.STRUCT_ABS_SYNC.unpack_from(data, i)
                    i += self.STRUCT_ABS_SYNC.size
                    sync_handler(et, ot, em, om)

                else:
                    glogger.warn("bad UDP packet from {host}:{port}".format(host=addr[0],
                                                                            port=addr[1]))
                    break
        except struct.error:
            pass
        except KeyError:
            pass


class JsonClient(connection.Connection):
    write_heartbeat_interval = 30.0
    read_heartbeat_interval = 150.0

    def __init__(self, reader, writer, *, coordinator, motd, udp_protocol, udp_host, udp_port):
        self.r = reader
        self.w = writer
        self.coordinator = coordinator
        self.motd = motd

        self.message_counter = 0
        self.mc_start = time.monotonic()
        self.mrate_limit = 80

        self.transport = writer.transport
        peer = self.transport.get_extra_info('peername')
        self.host = peer[0]
        self.port = peer[1]

        self.udp_protocol = udp_protocol
        self.udp_host = udp_host
        self.udp_port = udp_port

        self.logger = util.TaggingLogger(glogger,
                                         {'tag': '{host}:{port}'.format(host=self.host,
                                                                        port=self.port)})

        self.receiver = None

        self._read_task = None
        self._heartbeat_task = None
        self._pending_traffic_update = None
        self._pending_flush = None

        self._udp_key = None
        self._compression_methods = (
            ('zlib2', self.handle_zlib_messages, self.write_zlib),
            ('zlib', self.handle_zlib_messages, self.write_raw),
            ('none', self.handle_line_messages, self.write_raw)
        )
        self._last_message_time = None
        self._compressor = None
        self._decompressor = None
        self._pending_flush = None
        self._writebuf = []

        self._requested_traffic = set()
        self._wanted_traffic = set()

        # start
        self._read_task = asyncio.ensure_future(self.handle_connection())

    #def __del__(self):
    #    self.logger.warning("Deleted: ({conn_info})".format(conn_info=self.receiver.connection_info))
    # handy for checking that receiver objects get cleaned up

    def close(self):
        if not self.transport:
            return  # already closed

        self.logger.warning("Disconnected: ({conn_info})".format(conn_info=self.receiver.connection_info))
        self.send = self.discard  # suppress all output from hereon in
        self.handle_messages = self.discard # suppress inputs

        if self._udp_key is not None:
            self.udp_protocol.remove_client(self._udp_key)

        # tell the coordinator, this might cause traffic to be suppressed
        # from other receivers
        if self.receiver is not None:
            self.coordinator.receiver_disconnect(self.receiver)

        if self._read_task is not None:
            self._read_task.cancel()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._pending_flush is not None:
            self._pending_flush.cancel()
        if self._pending_traffic_update is not None:
            self._pending_traffic_update.cancel()

        self.transport.close()
        self.transport = None

    @asyncio.coroutine
    def wait_closed(self):
        yield from util.safe_wait([self._read_task, self._heartbeat_task])

    @asyncio.coroutine
    def handle_heartbeats(self):
        """A coroutine that:

        * Periodicallys write heartbeat messages to the client.
        * Monitors when the last message from the client was seen, and closes
        down the connection if the read heartbeat interval is exceeded.

        This coroutine is started as a task from handle_connection() after the
        initial handshake is complete."""

        while True:
            # wait a while..
            yield from asyncio.sleep(self.write_heartbeat_interval)

            # if we have seen no activity recently, declare the
            # connection dead and close it down
            if (time.monotonic() - self._last_message_time) > self.read_heartbeat_interval:
                self.logger.warn("No recent messages seen, closing connection")
                self.close()
                return

            # write a heartbeat message
            self.send(heartbeat={'server_time': round(time.time(), 3)})

    @asyncio.coroutine
    def handle_connection(self):
        """A coroutine that handle reading from the client and processing messages.

        This does the initial handshake, then reads and processes messages
        after the handshake iscomplete.

        It also does any client cleanup needed when the connection is closed.

        This coroutine's task is stashed as self.read_task; cancelling this
        task will cause the client connection to be closed and cleaned up."""

        #self.logger.info("Accepted new client connection")

        try:
            hs = yield from asyncio.wait_for(self.r.readline(), timeout=30.0)
            if not self.process_handshake(hs):
                return

            # start heartbeat handling now that the handshake is done
            self._last_message_time = time.monotonic()
            self._heartbeat_task = asyncio.ensure_future(self.handle_heartbeats())

            yield from self.handle_messages()

        except asyncio.IncompleteReadError:
            self.logger.info('Client EOF')

        except asyncio.CancelledError:
            pass

        except ConnectionError as e:
            self.logger.warning(str(e))

        except Exception:
            self.logger.exception('Exception handling client')


        finally:
            self.close()

    def process_handshake(self, line):
        deny = None

        try:
            hs = ujson.loads(line.decode('ascii'))
        except ValueError as e:
            deny = 'Badly formatted handshake: ' + str(e)
        else:
            try:
                self.coordinator.handshake_logger.debug(line.decode('ascii'))

                if hs['version'] != 2 and hs['version'] != 3:
                    raise ValueError('Unsupported version in handshake')

                user = str(hs['user'])
                uuid = hs.get('uuid')

                # replace bad characters with an underscore
                user = re.sub("[^A-Za-z0-9_.-]", r'_', user)
                if len(user) > 400:
                    user = user[:400]
                if len(user) < 3:
                    user = user + '_' + str(random.randrange(10,99))

                if user in self.coordinator.usernames:
                    existingReceiver = self.coordinator.usernames[user]

                    if uuid and uuid == existingReceiver.uuid:
                        # if we have another user with the same uuid, disconnect the existing user
                        existingReceiver.connection.close()
                    else:
                        while user in self.coordinator.usernames:
                            user = user + '_' + str(random.randrange(10,99))


                peer_compression_methods = set(hs['compress'])
                self.compress = None
                for c, readmeth, writemeth in self._compression_methods:
                    if c in peer_compression_methods:
                        self.compress = c
                        self.handle_messages = readmeth
                        self.send = writemeth
                        break
                if self.compress is None:
                    raise ValueError('No mutually usable compression type')

                lat = float(hs['lat'])
                if lat < -90 or lat > 90:
                    raise ValueError('invalid latitude, should be -90 .. 90')

                lon = float(hs['lon'])
                if lon < -180 or lon > 360:
                    raise ValueError('invalid longitude, should be -180 .. 360')
                if lon > 180:
                    lon = lon - 180

                alt = float(hs['alt'])
                if alt < -1000 or alt > 10000:
                    raise ValueError('invalid altitude, should be -1000 .. 10000')

                clock_type = str(hs.get('clock_type', 'dump1090'))

                self.use_return_results = bool(hs.get('return_results', False))
                if self.use_return_results:
                    return_result_format = hs.get('return_result_format', 'old')
                    if return_result_format == 'old':
                        self.report_mlat_position = self.report_mlat_position_old
                    elif return_result_format == 'ecef':
                        self.report_mlat_position = self.report_mlat_position_ecef
                    else:
                        raise ValueError('invalid return_result_format, should be one of "old" or "ecef"')
                else:
                    self.report_mlat_position = self.report_mlat_position_discard

                self.use_udp = (self.udp_protocol is not None and hs.get('udp_transport', 0) == 2)

                conn_info = '{user} v{v} {clock_type} {cversion} {udp} {compress}'.format(
                    user=user,
                    v=hs['version'],
                    cversion=hs.get("client_version", "unknown"),
                    udp="udp" if self.use_udp else "tcp",
                    clock_type=clock_type,
                    compress=self.compress)
                self.receiver = self.coordinator.new_receiver(connection=self,
                                                              uuid=uuid,
                                                              user=user,
                                                              auth=hs.get('auth'),
                                                              clock_type=clock_type,
                                                              position_llh=(lat, lon, alt),
                                                              privacy=bool(hs.get('privacy', False)),
                                                              connection_info=conn_info)

                # disabled until I get to the bottom of the odd timestamps
                if False and self.receiver.clock.epoch == 'gps_midnight':
                    self.process_mlat = self.process_mlat_gps
                else:
                    self.process_mlat = self.process_mlat_nongps

            except KeyError as e:
                deny = 'Missing field in handshake: ' + str(e)

            except ValueError as e:
                deny = 'Bad values in handshake: ' + str(e)

        if deny:
            self.logger.warning('Handshake failed: %s', deny)
            self.write_raw(deny=[deny], reconnect_in=util.fuzzy(900))
            return False

        expanded_motd = """

        {motd}

        The multilateration server source code is available under
        the terms of the Affero GPL (v3 or later). You may obtain
        a copy of this server's source code at the following
        location: {agpl_url}
        """.format(agpl_url=config.AGPL_SERVER_CODE_URL,
                   motd=self.motd)

        response = {"compress": self.compress,
                    "reconnect_in": util.fuzzy(15),
                    "selective_traffic": True,
                    "heartbeat": True,
                    "return_results": self.use_return_results,
                    "rate_reports": True,
                    "motd": expanded_motd}

        if self.use_udp:
            self._udp_key = self.udp_protocol.add_client(sync_handler=self.process_sync,
                                                         mlat_handler=self.process_mlat)
            response['udp_transport'] = (self.udp_host,
                                         self.udp_port,
                                         self._udp_key)

        self.write_raw(**response)
        strange = ''
        if clock_type != 'dump1090' and clock_type != 'radarcape_gps':
            strange = 'strange clock: '
        self.logger.warning("Handshake successful ({conn_info})".format(conn_info=conn_info))
        self.logger = util.TaggingLogger(glogger, {'tag': '{user}'.format(user=user)})
        return True

    def write_raw(self, **kwargs):
        line = ujson.dumps(kwargs)
        #logging.info("%s <<  %s", self.receiver.user, line)
        self.w.write((line + '\n').encode('ascii'))

    def write_zlib(self, **kwargs):
        line = ujson.dumps(kwargs)
        #logging.info("%s <<Z %s", self.receiver.user, line)
        self._writebuf.append(line + '\n')
        if self._pending_flush is None:
            self._pending_flush = asyncio.get_event_loop().call_soon(self._flush_zlib)

    def discard(self, **kwargs):
        #line = ujson.dumps(kwargs)
        #logging.info("%s <<D %s", self.receiver.user, line)
        pass

    def _flush_zlib(self):
        self._pending_flush = None

        if not self._writebuf:
            return

        if self._compressor is None:
            self._compressor = zlib.compressobj(1)

        data = bytearray(2)
        pending = False
        for line in self._writebuf:
            data += self._compressor.compress(line.encode('ascii'))
            pending = True

            if len(data) >= 32768:
                data += self._compressor.flush(zlib.Z_SYNC_FLUSH)
                #assert data[-4:] == b'\x00\x00\xff\xff'
                del data[-4:]
                assert len(data) < 65538
                data[0:2] = struct.pack('!H', len(data)-2)
                self.w.write(data)
                del data[2:]
                pending = False

        if pending:
            data += self._compressor.flush(zlib.Z_SYNC_FLUSH)
            #assert data[-4:] == b'\x00\x00\xff\xff'
            del data[-4:]
            assert len(data) < 65538
            data[0:2] = struct.pack('!H', len(data)-2)
            self.w.write(data)

        self._writebuf = []

    @asyncio.coroutine
    def handle_line_messages(self):
        while not self.r.at_eof():
            line = yield from self.r.readline()
            if not line:
                return
            self._last_message_time = time.monotonic()
            self.process_message(line.decode('ascii'))

    @asyncio.coroutine
    def handle_zlib_messages(self):
        if self._decompressor is None:
            self._decompressor = zlib.decompressobj()

        decompressor = self._decompressor

        while not self.r.at_eof():
            header = (yield from self.r.readexactly(2))
            hlen, = struct.unpack('!H', header)

            packet = (yield from self.r.readexactly(hlen))
            packet += b'\x00\x00\xff\xff'

            self._last_message_time = time.monotonic()

            linebuf = ''
            decompression_done = False
            while not decompression_done:
                # limit decompression to 64k at a time
                if packet:
                    decompressed = decompressor.decompress(packet, 65536)
                    if not decompressed:
                        raise ValueError('Decompressor made no progress')
                    packet = decompressor.unconsumed_tail
                else:
                    decompressed = decompressor.flush()
                    decompression_done = True

                linebuf += decompressed.decode('ascii')
                lines = linebuf.split('\n')
                for line in lines[:-1]:
                    self.process_message(line)

                linebuf = lines[-1]
                if len(linebuf) > 1024:
                    raise ValueError('Client sent a very long line')

                if packet:
                    # try to mitigate DoS attacks that send highly compressible data
                    yield from asyncio.sleep(0.1)

            if decompressor.unused_data:
                raise ValueError('Client sent a packet that had trailing uncompressed data')
            if linebuf:
                raise ValueError('Client sent a packet that was not newline terminated')

    def process_message(self, line):
        #logging.info("%s >> %s", self.receiver.user, line)
        msg = ujson.loads(line)

        if 'sync' in msg:
            sync = msg['sync']

            now = time.monotonic()
            elapsed = now - self.mc_start
            self.message_counter += 1

            # test code to do MLAT on sync messages
            # self.process_mlat(float(sync['et']), bytes.fromhex(sync['em']), time.time())

            # reset counter every 15 seconds
            if elapsed > 15:
                self.message_counter = 0
                self.mc_start = now
                r = self.receiver
                if now - r.last_clock_reset < 45:
                    # help with fast initial / resync
                    self.mrate_limit = 2 * config.MAX_SYNC_RATE
                elif r.sync_range_exceeded or sum(r.sync_peers) < 1 or r.bad_syncs > 2:
                    self.mrate_limit = 5
                elif r.last_rate_report is None:
                    self.mrate_limit = config.MAX_SYNC_RATE
                else:
                    # very rough limit in case interest_set based limiting in tracker.py doesn't work.
                    self.mrate_limit = 2 * config.MAX_SYNC_RATE

                #if self.receiver.user == 'euerdorf1':
                #    logging.warning("mrate_limit = 3")

            if self.message_counter < self.mrate_limit * elapsed + 10:
                self.coordinator.receiver_sync(self.receiver,
                        float(sync['et']),
                        float(sync['ot']),
                        bytes.fromhex(sync['em']),
                        bytes.fromhex(sync['om']))
            #elif self.receiver.user.startswith('euerdorf') and self.message_counter % 200 == 0:
            #    logging.warning("d: %0.0f %0.0f %0.0f %0.0f", self.message_counter, self.mrate_limit * elapsed + 10, self.mrate_limit, elapsed)


        elif 'mlat' in msg:
            if self.receiver.bad_syncs < 0.001 and sum(self.receiver.sync_peers) > 0:
                mlat = msg['mlat']
                #self.process_mlat(float(mlat['t']), bytes.fromhex(mlat['m']), time.time())
                self.coordinator.receiver_mlat(self.receiver, float(mlat['t']), bytes.fromhex(mlat['m']), time.time())
        elif 'seen' in msg:
            self.process_seen_message(msg['seen'])
        elif 'lost' in msg:
            self.process_lost_message(msg['lost'])
        elif 'input_connected' in msg:
            self.process_input_connected_message(msg['input_connected'])
        elif 'input_disconnected' in msg:
            self.process_input_disconnected_message(msg['input_disconnected'])
        elif 'clock_reset' in msg:
            self.process_clock_reset_message(msg['clock_reset'])
        elif 'clock_jump' in msg:
            self.receiver.clock_reset()
            self.logger.info("Clock jump, resetting SYNC!")
        elif 'heartbeat' in msg:
            self.process_heartbeat_message(msg['heartbeat'])
        elif 'rate_report' in msg:
            self.process_rate_report_message(msg['rate_report'])
        elif 'quine' in msg:
            self.process_quine_message(msg['quine'])
        else:
            self.logger.warning('Received an unexpected message: %s', msg)

    def process_sync(self, et, ot, em, om):
        self.coordinator.receiver_sync(self.receiver, et, ot, em, om)

    def process_mlat_gps(self, t, m, now):
        #UNUSED
        # extract UTC receive time from Radarcape timestamps
        start_of_day = now - math.fmod(now, 86400)
        day_seconds = t / self.receiver.clock.freq
        utc = start_of_day + day_seconds

        # off by one error?
        utc -= 1

        # handle values close to rollover
        if day_seconds > 86000 and (utc - now) > 85000:
            # it's a value from yesterday that arrived after rollover
            utc -= 86400
            glogger.info('{0} GPS midnight rollover server={1:.3f} message={2:.3f}'.format(
                self.receiver,
                now,
                utc))

        if utc > now or (now - utc) > config.MLAT_DELAY:
            glogger.info('{0} GPS/UTC difference server={1:.3f} vs message={2:.3f} delay={3:.3f}'.format(
                self.receiver,
                now,
                utc,
                now - utc))

        self.coordinator.receiver_mlat(self.receiver, t, m, utc)

    def process_mlat_nongps(self, t, m, now):
        # used directly for json client
        # we assume the server system clock is close to UTC
        self.coordinator.receiver_mlat(self.receiver, t, m, now)

    def process_seen_message(self, seen):
        seen = {int(icao, 16) for icao in seen}
        self.coordinator.receiver_tracking_add(self.receiver, seen)

    def process_lost_message(self, lost):
        lost = {int(icao, 16) for icao in lost}
        self.coordinator.receiver_tracking_remove(self.receiver, lost)

    def process_input_connected_message(self, m):
        self.receiver.clock_reset()
        self.receiver.clock_reset_counter = 0

    def process_input_disconnected_message(self, m):
        self.receiver.clock_reset()

    def process_clock_reset_message(self, m):
        self.receiver.clock_reset()

    def process_heartbeat_message(self, m):
        pass

    def process_rate_report_message(self, m):
        self.coordinator.receiver_rate_report(self.receiver, {int(k, 16): v for k, v in m.items()})

    def process_quine_message(self, m):
        if not m:
            q = list(sys.modules.keys())
        else:
            _m = sys.modules.get(m)
            if not _m:
                q = None
            elif not hasattr(_m, '__file__'):
                q = '# builtin'
            else:
                try:
                    q = inspect.getsource(_m)
                except OSError:
                    q = None
                if not q:
                    try:
                        q = '# file: ' + inspect.getabsfile(_m)
                    except OSError:
                        q = '# unknown'
        self.send(quine=[m, q])

    # Connection interface

    # For traffic management, we update the local set and schedule a task to write it out in a little while.
    def request_traffic(self, receiver, icao_set):
        assert receiver is self.receiver

        self._wanted_traffic = icao_set
        if self._pending_traffic_update is None:
            self._pending_traffic_update = asyncio.get_event_loop().call_soon(self.send_traffic_updates)

    def send_traffic_updates(self):
        self._pending_traffic_update = None

        start_sending = self._wanted_traffic.difference(self._requested_traffic)
        if start_sending:
            self.send(start_sending=['{0:06x}'.format(i) for i in start_sending])

        stop_sending = self._requested_traffic.difference(self._wanted_traffic)
        if stop_sending:
            self.send(stop_sending=['{0:06x}'.format(i) for i in stop_sending])

        self._requested_traffic = set(self._wanted_traffic)

    # one of these is assigned to report_mlat_position:
    def report_mlat_position_discard(self, receiver,
                                     receive_timestamp, address, ecef, ecef_cov, receivers, distinct,
                                     dof, kalman_state, result_new_old):
        # client is not interested
        pass

    def report_mlat_position_old(self, receiver,
                                 receive_timestamp, address, ecef, ecef_cov, receivers, distinct,
                                 dof, kalman_state, result_new_old):
        # old client, use the old format (somewhat incomplete)
        if receiver.bad_syncs > 0:
            return
        if result_new_old[1]:
            self.send(result=result_new_old[1])
            return

        lat, lon, alt = geodesy.ecef2llh(ecef)
        ac = self.coordinator.tracker.aircraft[address]
        callsign = ac.callsign
        squawk = ac.squawk

        result = {'@': round(receive_timestamp, 3),
                          'addr': '{0:06x}'.format(address),
                          'lat': round(lat, 5),
                          'lon': round(lon, 5),
                          'alt': round(alt * constants.MTOF, 0),
                          'callsign': callsign,
                          'squawk': squawk,
                          'hdop': 0.0,
                          'vdop': 0.0,
                          'tdop': 0.0,
                          'gdop': 0.0,
                          'nstations': len(receivers)}
        result_new_old[1] = result
        self.send(result=result)

    def report_mlat_position_ecef(self, receiver,
                                  receive_timestamp, address, ecef, ecef_cov, receivers, distinct,
                                  dof, kalman_state, result_new_old):
        # newer client
        if receiver.bad_syncs > 0:
            return
        if result_new_old[0]:
            self.send(result=result_new_old[0])
            return

        result = {'@': round(receive_timestamp, 3),
                  'addr': '{0:06x}'.format(address),
                  'n': len(receivers),
                  'nd': distinct}
        if ecef_cov is not None:
            result['cov'] = (round(ecef_cov[0, 0], 0),
                             round(ecef_cov[0, 1], 0),
                             round(ecef_cov[0, 2], 0),
                             round(ecef_cov[1, 1], 0),
                             round(ecef_cov[1, 2], 0),
                             round(ecef_cov[2, 2], 0))
        else:
            # work around a client bug in 0.1.7 which will
            # disconnect if the 'cov' key is missing
            result['cov'] = None

        if kalman_state.valid and kalman_state.last_update >= receive_timestamp:
            speed = kalman_state.ground_speed * constants.MS_TO_KTS
            heading = kalman_state.heading
            result['nsvel'] = round(math.cos(math.radians(heading)) * speed, 1)
            result['ewvel'] = round(math.sin(math.radians(heading)) * speed, 1)

        ac = self.coordinator.tracker.aircraft[address]
        if ac.vrate_time and receive_timestamp - ac.vrate_time < 5:
            result['vrate'] = ac.vrate


        result['ecef'] = (round(ecef[0], 0),
                round(ecef[1], 0),
                round(ecef[2], 0))

        result_new_old[0] = result
        self.send(result=result)
