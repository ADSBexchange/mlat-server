#!/usr/bin/env python3
"""
Synthetic load generator for mlat-server.

Connects N fake feeders, sends realistic seen/sync/mlat traffic, and
exercises failure modes (connection churn, abrupt TCP drops) to validate
memory-leak fixes before production deployment.

Usage:
    python3 synthetic_load.py --host mlat-server.example.com --port 31090 --feeders 50
    python3 synthetic_load.py --host localhost --port 31090 --feeders 20 --churn-interval 30

The generator does NOT attempt real multilateration — it sends plausible
message traffic so the server exercises its clock-sync, tracking, cohort
batching, and cleanup paths.
"""

import argparse
import asyncio
import json
import logging
import math
import os
import random
import signal
import struct
import sys
import time
import zlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-7s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('loadgen')


# ---------------------------------------------------------------------------
# Fake ADS-B message generation
# ---------------------------------------------------------------------------

# Common Mode-S DF17 (ADS-B) extended squitter: 14 bytes
# We generate random but structurally valid-looking hex messages
def random_adsb_msg():
    """Return 14 random bytes as a hex string (like a DF17 extended squitter)."""
    return os.urandom(14).hex()


def random_short_msg():
    """Return 7 random bytes as a hex string (like a DF11 short message)."""
    return os.urandom(7).hex()


# Generate a pool of fake ICAO addresses
def make_aircraft_pool(n):
    """Return n random 24-bit ICAO addresses as hex strings."""
    return [format(random.randint(0x100000, 0xFFFFFF), '06x') for _ in range(n)]


# ---------------------------------------------------------------------------
# Fake feeder (simulates a mlat-client connecting to mlat-server)
# ---------------------------------------------------------------------------

class FakeFeeder:
    """A single fake mlat-client connection."""

    def __init__(self, feeder_id, host, port, aircraft_pool, use_compression=False):
        self.feeder_id = feeder_id
        self.host = host
        self.port = port
        self.aircraft_pool = aircraft_pool
        self.use_compression = use_compression

        self.user = f'loadgen-{feeder_id:04d}'
        # Spread feeders across a ~200km area (US midwest-ish)
        self.lat = 39.0 + random.uniform(-1.0, 1.0)
        self.lon = -94.0 + random.uniform(-1.0, 1.0)
        self.alt = random.uniform(100, 500)

        self.reader = None
        self.writer = None
        self.connected = False
        self._stop = False
        self._task = None

        # dump1090-style 12 MHz clock
        self.clock_freq = 12e6
        self.clock_base = random.randint(0, int(86400 * self.clock_freq))

        # aircraft this feeder currently "sees"
        self.visible_aircraft = set()

        # stats
        self.messages_sent = 0
        self.connections_made = 0

    async def start(self):
        self._stop = False
        self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._disconnect()

    def _disconnect(self):
        if self.writer:
            try:
                self.writer.close()
            except Exception:
                pass
        self.writer = None
        self.reader = None
        self.connected = False

    async def _run(self):
        while not self._stop:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.debug('Feeder %s error: %s', self.user, e)
            finally:
                self._disconnect()

            if not self._stop:
                await asyncio.sleep(random.uniform(1, 5))

    async def _connect_and_run(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=10
        )
        self.connections_made += 1

        # --- handshake ---
        compress_methods = ['zlib2', 'zlib', 'none'] if self.use_compression else ['none']
        handshake = {
            'version': 2,
            'user': self.user,
            'lat': self.lat,
            'lon': self.lon,
            'alt': self.alt,
            'clock_type': 'dump1090',
            'compress': compress_methods,
            'uuid': f'loadgen-uuid-{self.feeder_id}',
            'client_version': 'loadgen-0.1',
        }
        self.writer.write((json.dumps(handshake) + '\n').encode('ascii'))
        await self.writer.drain()

        # read handshake response
        resp_line = await asyncio.wait_for(self.reader.readline(), timeout=10)
        if not resp_line:
            raise ConnectionError('Empty handshake response')
        resp = json.loads(resp_line.decode('ascii'))

        if 'deny' in resp:
            raise ConnectionError(f'Handshake denied: {resp["deny"]}')

        negotiated_compress = resp.get('compress', 'none')
        self.connected = True
        log.debug('Feeder %s connected (compress=%s)', self.user, negotiated_compress)

        if negotiated_compress in ('zlib', 'zlib2'):
            await self._message_loop_zlib(negotiated_compress)
        else:
            await self._message_loop_raw()

    async def _message_loop_raw(self):
        """Send/receive loop using raw (uncompressed) JSON lines."""
        # Initial: report some aircraft as seen
        self._update_visible_aircraft()
        self._send_raw({'seen': list(self.visible_aircraft)})

        read_task = asyncio.ensure_future(self._read_loop_raw())
        try:
            tick = 0
            while not self._stop:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                tick += 1

                # Send sync messages (most common)
                if random.random() < 0.7:
                    self._send_sync_raw()

                # Send mlat messages
                if random.random() < 0.3:
                    self._send_mlat_raw()

                # Periodic seen/lost updates
                if tick % 10 == 0:
                    self._update_visible_aircraft()
                    self._send_raw({'seen': list(self.visible_aircraft)})

                # Heartbeat
                if tick % 30 == 0:
                    self._send_raw({'heartbeat': {'server_time': round(time.time(), 3)}})

                # Rate report
                if tick % 60 == 0:
                    rate = {icao: round(random.uniform(0.5, 5.0), 1) for icao in self.visible_aircraft}
                    self._send_raw({'rate_report': rate})

        finally:
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass

    async def _read_loop_raw(self):
        """Read and discard server messages (traffic updates, heartbeats)."""
        try:
            while not self._stop:
                line = await self.reader.readline()
                if not line:
                    return
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def _send_raw(self, msg):
        if self.writer and not self._stop:
            try:
                self.writer.write((json.dumps(msg) + '\n').encode('ascii'))
                self.messages_sent += 1
            except Exception:
                pass

    def _get_clock_timestamp(self):
        """Return a fake dump1090-style 12MHz timestamp."""
        # Simulate clock advancing with real time
        self.clock_base += int(random.uniform(0.001, 0.1) * self.clock_freq)
        return self.clock_base

    def _send_sync_raw(self):
        """Send a sync message with two correlated timestamps + messages."""
        if len(self.visible_aircraft) < 2:
            return
        et = self._get_clock_timestamp()
        ot = self._get_clock_timestamp()
        em = random_adsb_msg()
        om = random_adsb_msg()
        self._send_raw({
            'sync': {
                'et': et,
                'ot': ot,
                'em': em,
                'om': om,
            }
        })

    def _send_mlat_raw(self):
        """Send an mlat message."""
        t = self._get_clock_timestamp()
        m = random_adsb_msg()
        self._send_raw({
            'mlat': {
                't': t,
                'm': m,
            }
        })

    def _update_visible_aircraft(self):
        """Randomly adjust which aircraft this feeder sees."""
        pool = self.aircraft_pool
        target_count = random.randint(5, min(30, len(pool)))

        # lose some
        if self.visible_aircraft and random.random() < 0.3:
            to_lose = random.sample(list(self.visible_aircraft),
                                    k=min(3, len(self.visible_aircraft)))
            for icao in to_lose:
                self.visible_aircraft.discard(icao)

        # gain some
        while len(self.visible_aircraft) < target_count:
            self.visible_aircraft.add(random.choice(pool))

    # --- zlib message loop (for zlib/zlib2 compression) ---
    async def _message_loop_zlib(self, compress_mode):
        """Send/receive loop with zlib compression."""
        compressor = zlib.compressobj(1)
        decompressor = zlib.decompressobj()

        self._update_visible_aircraft()
        self._send_zlib(compressor, {'seen': list(self.visible_aircraft)})

        read_task = asyncio.ensure_future(self._read_loop_zlib(decompressor, compress_mode))
        try:
            tick = 0
            while not self._stop:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                tick += 1

                if random.random() < 0.7:
                    self._send_sync_zlib(compressor)

                if random.random() < 0.3:
                    self._send_mlat_zlib(compressor)

                if tick % 10 == 0:
                    self._update_visible_aircraft()
                    self._send_zlib(compressor, {'seen': list(self.visible_aircraft)})

                if tick % 30 == 0:
                    self._send_zlib(compressor, {'heartbeat': {'server_time': round(time.time(), 3)}})

        finally:
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass

    def _send_zlib(self, compressor, msg):
        if self.writer and not self._stop:
            try:
                line = (json.dumps(msg) + '\n').encode('ascii')
                compressed = compressor.compress(line) + compressor.flush(zlib.Z_SYNC_FLUSH)
                # strip the trailing 00 00 ff ff sync marker
                if compressed[-4:] == b'\x00\x00\xff\xff':
                    compressed = compressed[:-4]
                frame = struct.pack('!H', len(compressed)) + compressed
                self.writer.write(frame)
                self.messages_sent += 1
            except Exception:
                pass

    def _send_sync_zlib(self, compressor):
        if len(self.visible_aircraft) < 2:
            return
        et = self._get_clock_timestamp()
        ot = self._get_clock_timestamp()
        self._send_zlib(compressor, {
            'sync': {'et': et, 'ot': ot, 'em': random_adsb_msg(), 'om': random_adsb_msg()}
        })

    def _send_mlat_zlib(self, compressor):
        self._send_zlib(compressor, {
            'mlat': {'t': self._get_clock_timestamp(), 'm': random_adsb_msg()}
        })

    async def _read_loop_zlib(self, decompressor, compress_mode):
        """Read zlib-compressed server messages."""
        try:
            if compress_mode == 'zlib2':
                while not self._stop:
                    header = await self.reader.readexactly(2)
                    hlen, = struct.unpack('!H', header)
                    packet = await self.reader.readexactly(hlen)
                    # decompress and discard
                    decompressor.decompress(packet + b'\x00\x00\xff\xff')
            else:
                # zlib mode: server writes raw, we read lines
                while not self._stop:
                    line = await self.reader.readline()
                    if not line:
                        return
        except asyncio.CancelledError:
            pass
        except asyncio.IncompleteReadError:
            pass
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Connection churn manager
# ---------------------------------------------------------------------------

class ChurnManager:
    """Periodically kills and reconnects random feeders to exercise cleanup."""

    def __init__(self, feeders, interval):
        self.feeders = feeders
        self.interval = interval
        self._task = None

    async def start(self):
        self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        while True:
            await asyncio.sleep(self.interval)
            if not self.feeders:
                continue

            # Kill 10-20% of feeders abruptly
            n_kill = max(1, len(self.feeders) // random.randint(5, 10))
            victims = random.sample(self.feeders, k=min(n_kill, len(self.feeders)))

            for feeder in victims:
                if feeder.connected:
                    log.info('Churn: killing feeder %s', feeder.user)
                    # Abrupt disconnect (no close handshake)
                    if feeder.writer:
                        feeder.writer.transport.abort()
                    feeder._disconnect()

            # They'll automatically reconnect via their _run loop


# ---------------------------------------------------------------------------
# Stats reporter
# ---------------------------------------------------------------------------

async def report_stats(feeders, interval=15):
    while True:
        await asyncio.sleep(interval)
        connected = sum(1 for f in feeders if f.connected)
        total_msgs = sum(f.messages_sent for f in feeders)
        total_conns = sum(f.connections_made for f in feeders)
        log.info(
            'Stats: %d/%d connected, %d total messages sent, %d total connections',
            connected, len(feeders), total_msgs, total_conns,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(args):
    aircraft_pool = make_aircraft_pool(args.aircraft)
    log.info('Created pool of %d fake aircraft', len(aircraft_pool))

    feeders = []
    for i in range(args.feeders):
        f = FakeFeeder(
            feeder_id=i,
            host=args.host,
            port=args.port,
            aircraft_pool=aircraft_pool,
            use_compression=args.compress,
        )
        feeders.append(f)

    # Start feeders with staggered connects
    log.info('Starting %d feeders against %s:%d ...', args.feeders, args.host, args.port)
    for i, feeder in enumerate(feeders):
        await feeder.start()
        if i < len(feeders) - 1:
            await asyncio.sleep(args.connect_delay)

    log.info('All feeders started')

    # Start optional churn
    churn = None
    if args.churn_interval > 0:
        churn = ChurnManager(feeders, args.churn_interval)
        await churn.start()
        log.info('Churn enabled: killing random feeders every %ds', args.churn_interval)

    # Start stats reporter
    stats_task = asyncio.ensure_future(report_stats(feeders, interval=15))

    # Wait for Ctrl+C
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    log.info('Shutting down...')

    stats_task.cancel()
    if churn:
        await churn.stop()

    await asyncio.gather(*[f.stop() for f in feeders], return_exceptions=True)
    log.info('Done. Total connections: %d, Total messages: %d',
             sum(f.connections_made for f in feeders),
             sum(f.messages_sent for f in feeders))


def parse_args():
    p = argparse.ArgumentParser(description='Synthetic load generator for mlat-server')
    p.add_argument('--host', default='localhost', help='mlat-server host (default: localhost)')
    p.add_argument('--port', type=int, default=31090, help='mlat-server port (default: 31090)')
    p.add_argument('--feeders', type=int, default=50, help='number of fake feeders (default: 50)')
    p.add_argument('--aircraft', type=int, default=200, help='number of fake aircraft in pool (default: 200)')
    p.add_argument('--churn-interval', type=int, default=0,
                   help='seconds between random feeder kills (0=disabled, default: 0)')
    p.add_argument('--connect-delay', type=float, default=0.1,
                   help='delay between feeder connections in seconds (default: 0.1)')
    p.add_argument('--compress', action='store_true', help='use zlib compression')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    asyncio.run(main(args))
