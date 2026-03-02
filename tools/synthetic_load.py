#!/usr/bin/env python3
"""
Synthetic load generator for mlat-server.

Connects N fake feeders, sends realistic seen/sync/mlat traffic, and
exercises failure modes (connection churn, abrupt TCP drops) to validate
memory-leak fixes before production deployment.

Supports correlated messages: multiple feeders send the same valid DF17
ADS-B message so the server builds real clock pairings and exercises the
solver / cohort / normalization code paths.

Usage:
    python3 synthetic_load.py --host mlat-server.example.com --port 31090 --feeders 50
    python3 synthetic_load.py --host localhost --port 31090 --feeders 200 --churn-interval 15
    python3 synthetic_load.py --host localhost --port 31090 --geometric

By default, the generator does NOT produce geometrically accurate
multilateration — positions won't solve correctly. Use --geometric to
enable TDOA-consistent timestamps that produce solvable positions over
Baffin Bay (~72N, -65W).
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
# Mode-S DF17 message generation with valid CRC
# ---------------------------------------------------------------------------

def _make_crc_table():
    """Build CRC24 lookup table (Mode S polynomial 0xFFF409)."""
    poly = 0xFFF409
    t = []
    for i in range(256):
        c = i << 16
        for _ in range(8):
            c = ((c << 1) ^ poly) if (c & 0x800000) else (c << 1)
        t.append(c & 0xFFFFFF)
    return t

_CRC_TABLE = _make_crc_table()


def _crc24(data_bytes):
    """Compute Mode-S CRC24 over raw bytes (no PI field)."""
    t = _CRC_TABLE
    rem = t[data_bytes[0]]
    for b in data_bytes[1:]:
        rem = ((rem & 0xFFFF) << 8) ^ t[b ^ (rem >> 16)]
    return rem & 0xFFFFFF


def _encode_altitude_ac12(alt_ft):
    """Encode altitude in feet to 12-bit AC12 field (Q-bit method, 25ft resolution)."""
    n = (alt_ft + 1000) // 25
    n = max(0, min(n, 0x7FF))  # 11-bit value
    # Map N to AC13 bit layout: top6 | M=0 | B1(n[4]) | Q=1 | bottom4
    ac13 = ((n >> 5) << 7) | ((n & 0x10) << 1) | 0x0010 | (n & 0x0F)
    # Convert AC13 to AC12 (remove M-bit)
    ac12 = ((ac13 >> 1) & 0x0FC0) | (ac13 & 0x003F)
    return ac12


def _encode_cpr_lat_lon(lat, lon, f_flag):
    """Encode lat/lon to 17-bit CPR values for even (f=0) or odd (f=1) frame.

    Returns (cpr_lat, cpr_lon) as integers.
    """
    nz = 15  # number of latitude zones (for NUC >= 7)
    d_lat = 360.0 / (60 - f_flag)

    # latitude encoding
    lat_cpr = lat / d_lat
    yz = int(round((2**17) * (lat_cpr % 1.0))) & 0x1FFFF

    # longitude encoding
    rlat = d_lat * (int(lat_cpr) + yz / (2**17))
    # NL function (simplified for mid-latitudes)
    if abs(rlat) < 10.47:
        nl = 59
    elif abs(rlat) < 14.82:
        nl = 54
    elif abs(rlat) < 18.19:
        nl = 49
    elif abs(rlat) < 21.03:
        nl = 45
    elif abs(rlat) < 23.55:
        nl = 42
    elif abs(rlat) < 25.83:
        nl = 39
    elif abs(rlat) < 27.94:
        nl = 37
    elif abs(rlat) < 29.91:
        nl = 35
    elif abs(rlat) < 31.77:
        nl = 33
    elif abs(rlat) < 33.54:
        nl = 31
    elif abs(rlat) < 35.23:
        nl = 30
    elif abs(rlat) < 36.85:
        nl = 28
    elif abs(rlat) < 38.41:
        nl = 27
    elif abs(rlat) < 39.92:
        nl = 26
    elif abs(rlat) < 41.39:
        nl = 25
    elif abs(rlat) < 42.83:
        nl = 24
    elif abs(rlat) < 44.23:
        nl = 23
    elif abs(rlat) < 45.6:
        nl = 22
    elif abs(rlat) < 46.95:
        nl = 21
    elif abs(rlat) < 48.28:
        nl = 20
    elif abs(rlat) < 49.6:
        nl = 19
    elif abs(rlat) < 50.9:
        nl = 18
    elif abs(rlat) < 52.19:
        nl = 17
    elif abs(rlat) < 53.47:
        nl = 16
    elif abs(rlat) < 54.75:
        nl = 15
    elif abs(rlat) < 56.03:
        nl = 14
    elif abs(rlat) < 57.32:
        nl = 13
    elif abs(rlat) < 58.63:
        nl = 12
    elif abs(rlat) < 59.95:
        nl = 11
    elif abs(rlat) < 61.31:
        nl = 10
    elif abs(rlat) < 62.72:
        nl = 9
    elif abs(rlat) < 64.18:
        nl = 8
    elif abs(rlat) < 65.73:
        nl = 7
    elif abs(rlat) < 67.39:
        nl = 6
    elif abs(rlat) < 69.22:
        nl = 5
    elif abs(rlat) < 71.32:
        nl = 4
    elif abs(rlat) < 73.91:
        nl = 3
    elif abs(rlat) < 77.61:
        nl = 2
    else:
        nl = 1

    n_lon = max(1, nl - f_flag)
    d_lon = 360.0 / n_lon
    lon_cpr = lon / d_lon
    xz = int(round((2**17) * (lon_cpr % 1.0))) & 0x1FFFF

    return yz, xz


def build_df17_airborne_position(icao_addr, alt_ft, lat, lon, f_flag):
    """Build a valid 14-byte DF17 airborne position message with correct CRC.

    icao_addr: 24-bit ICAO address (int)
    alt_ft: altitude in feet
    lat, lon: position
    f_flag: 0 for even CPR frame, 1 for odd CPR frame

    Returns hex string of the 14-byte message.
    """
    # Byte 0: DF=17 (10001), CA=5 (101) = 0x8D
    b0 = 0x8D

    # Bytes 1-3: ICAO address
    b1 = (icao_addr >> 16) & 0xFF
    b2 = (icao_addr >> 8) & 0xFF
    b3 = icao_addr & 0xFF

    # Byte 4: TC=11 (01011), SS=0, SAF=0 = 0x58
    b4 = 0x58

    # Bytes 5-6: altitude (AC12, 12 bits) + T=0, F=f_flag
    ac12 = _encode_altitude_ac12(alt_ft)
    b5 = (ac12 >> 4) & 0xFF
    # b6: low 4 bits of ac12 | T(1 bit) | F(1 bit) | top 2 bits of LAT
    cpr_lat, cpr_lon = _encode_cpr_lat_lon(lat, lon, f_flag)
    b6 = ((ac12 & 0x0F) << 4) | (0 << 3) | (f_flag << 2) | ((cpr_lat >> 15) & 0x03)

    # Bytes 7-8: remaining LAT bits (15) and top 1 bit of LON
    b7 = (cpr_lat >> 7) & 0xFF
    b8 = ((cpr_lat & 0x7F) << 1) | ((cpr_lon >> 16) & 0x01)

    # Bytes 9-10: remaining LON bits (16)
    b9 = (cpr_lon >> 8) & 0xFF
    b10 = cpr_lon & 0xFF

    # Compute CRC over first 11 bytes
    msg_data = bytes([b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10])
    crc_val = _crc24(msg_data)

    # Bytes 11-13: CRC
    b11 = (crc_val >> 16) & 0xFF
    b12 = (crc_val >> 8) & 0xFF
    b13 = crc_val & 0xFF

    full_msg = bytes([b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13])
    return full_msg.hex()


class AircraftState:
    """Represents a simulated aircraft that multiple feeders can see."""

    def __init__(self, icao_hex):
        self.icao_hex = icao_hex
        self.icao_int = int(icao_hex, 16)
        # random position in US midwest area
        self.lat = 39.0 + random.uniform(-3.0, 3.0)
        self.lon = -94.0 + random.uniform(-3.0, 3.0)
        self.alt_ft = random.randint(5000, 42000)
        # generate even/odd message pair
        self._regenerate_messages()

    def _regenerate_messages(self):
        """Generate a new even/odd DF17 message pair for this aircraft."""
        self.even_msg = build_df17_airborne_position(
            self.icao_int, self.alt_ft, self.lat, self.lon, f_flag=0)
        self.odd_msg = build_df17_airborne_position(
            self.icao_int, self.alt_ft, self.lat, self.lon, f_flag=1)

    def step(self):
        """Move the aircraft slightly and regenerate messages."""
        self.lat += random.uniform(-0.01, 0.01)
        self.lon += random.uniform(-0.01, 0.01)
        self.alt_ft += random.randint(-500, 500)
        self.alt_ft = max(1000, min(45000, self.alt_ft))
        self._regenerate_messages()


def make_aircraft_pool(n):
    """Return n AircraftState objects with random ICAO addresses."""
    icaos = [format(random.randint(0x100000, 0xFFFFFF), '06x') for _ in range(n)]
    return [AircraftState(icao) for icao in icaos]


# ---------------------------------------------------------------------------
# Geometric mode — TDOA-consistent timestamps for solvable positions
# ---------------------------------------------------------------------------

CAIR = 299792458 / 1.00032  # speed of radio in air (m/s), from mlat/constants.py

# WGS84 ellipsoid parameters (matching mlat/geodesy.pyx)
_WGS84_A = 6378137.0
_WGS84_F = 1.0 / 298.257223563
_WGS84_B = _WGS84_A * (1 - _WGS84_F)
_WGS84_ECC_SQ = 1 - _WGS84_B * _WGS84_B / (_WGS84_A * _WGS84_A)


def llh2ecef_pure(lat, lon, alt_m):
    """WGS84 LLH to ECEF (pure Python, matching mlat/geodesy.pyx)."""
    lat_r = math.radians(lat)
    lon_r = math.radians(lon)
    slat = math.sin(lat_r)
    clat = math.cos(lat_r)
    slon = math.sin(lon_r)
    clon = math.cos(lon_r)
    d = math.sqrt(1 - slat * slat * _WGS84_ECC_SQ)
    rn = _WGS84_A / d
    x = (rn + alt_m) * clat * clon
    y = (rn + alt_m) * clat * slon
    z = (rn * (1 - _WGS84_ECC_SQ) + alt_m) * slat
    return (x, y, z)


def ecef_distance_pure(p0, p1):
    """Euclidean distance between two ECEF points."""
    return math.sqrt((p0[0] - p1[0])**2 + (p0[1] - p1[1])**2 + (p0[2] - p1[2])**2)


class SimulatedClock:
    """Models a dump1090 12MHz clock with per-receiver PPM error and epoch offset."""

    NOMINAL_FREQ = 12e6

    def __init__(self, ppm_error=None):
        if ppm_error is None:
            ppm_error = random.uniform(-50, 50)
        self.true_freq = self.NOMINAL_FREQ * (1 + ppm_error * 1e-6)
        self.epoch = random.randint(0, int(86400 * self.NOMINAL_FREQ))

    def timestamp_for(self, wall_time_s):
        """Convert wall-clock seconds to raw dump1090 clock value."""
        return int(self.epoch + wall_time_s * self.true_freq)


class GeometricAircraftState(AircraftState):
    """Aircraft with known position, heading-based movement, and ECEF coordinates."""

    def __init__(self, icao_hex, lat, lon, alt_ft, heading_deg, speed_kts):
        self.icao_hex = icao_hex
        self.icao_int = int(icao_hex, 16)
        self.lat = lat
        self.lon = lon
        self.alt_ft = alt_ft
        self.heading_deg = heading_deg
        self.speed_kts = speed_kts
        self.ecef = llh2ecef_pure(lat, lon, alt_ft * 0.3048)
        self._regenerate_messages()

    def step(self):
        """Move along heading at speed, update ECEF, regenerate DF17 messages."""
        step_time = 5.0
        speed_mps = self.speed_kts * 0.514444
        distance_m = speed_mps * step_time
        lat_r = math.radians(self.lat)
        self.lat += (distance_m * math.cos(math.radians(self.heading_deg))) / 111320.0
        self.lon += (distance_m * math.sin(math.radians(self.heading_deg))) / (111320.0 * math.cos(lat_r))
        self.ecef = llh2ecef_pure(self.lat, self.lon, self.alt_ft * 0.3048)
        self._regenerate_messages()


# Baffin Bay area — 6 receivers on coastlines, 4 aircraft over the bay
BAFFIN_RECEIVERS = [
    # (lat, lon, alt_m, name)
    (71.3, -65.7, 50, 'clyde-river'),
    (72.0, -63.5, 30, 'cape-dyer'),
    (72.8, -66.0, 80, 'pond-inlet-s'),
    (71.0, -67.0, 120, 'broughton-island'),
    (73.2, -64.0, 40, 'greenland-w1'),
    (71.7, -63.0, 60, 'greenland-w2'),
]

BAFFIN_AIRCRAFT = [
    # (icao_hex, lat, lon, alt_ft, heading_deg, speed_kts)
    ('A00001', 72.0, -65.0, 35000, 45, 450),
    ('A00002', 71.5, -64.5, 28000, 120, 420),
    ('A00003', 72.5, -66.0, 39000, 270, 460),
    ('A00004', 71.8, -63.8, 33000, 190, 440),
]


class TransmissionScheduler:
    """Computes geometrically correct TDOA timestamps based on distance and clock models."""

    def __init__(self, feeder_clocks):
        """feeder_clocks: {feeder_id: SimulatedClock}"""
        self.feeder_clocks = feeder_clocks
        self._wall_time_offset = time.time()

    def _wall_time(self):
        return time.time() - self._wall_time_offset

    def compute_sync_timestamps(self, aircraft, feeders):
        """Compute per-feeder (et, ot) timestamps for a sync message pair.

        Returns {feeder_id: (et, ot)} with propagation-delay-correct values.
        """
        wall_t = self._wall_time()
        odd_offset = random.uniform(0.5, 2.0)
        result = {}
        for f in feeders:
            dist = ecef_distance_pure(aircraft.ecef, f.ecef)
            prop_delay = dist / CAIR
            clock = self.feeder_clocks[f.feeder_id]
            et = clock.timestamp_for(wall_t + prop_delay)
            ot = clock.timestamp_for(wall_t + odd_offset + prop_delay)
            # dump1090 jitter: ~500ns = ~6 ticks at 12MHz
            et += random.randint(-6, 6)
            ot += random.randint(-6, 6)
            result[f.feeder_id] = (et, ot)
        return result

    def compute_mlat_timestamps(self, aircraft, feeders):
        """Compute per-feeder timestamps for a single mlat message.

        Returns {feeder_id: t} with propagation-delay-correct values.
        """
        wall_t = self._wall_time()
        result = {}
        for f in feeders:
            dist = ecef_distance_pure(aircraft.ecef, f.ecef)
            prop_delay = dist / CAIR
            clock = self.feeder_clocks[f.feeder_id]
            t = clock.timestamp_for(wall_t + prop_delay)
            t += random.randint(-6, 6)
            result[f.feeder_id] = t
        return result


class GeometricBroadcaster:
    """Centralized coordinator that sends TDOA-consistent sync/mlat messages.

    All in-range feeders receive the same DF17 message with their own
    propagation-delay-correct timestamp, ensuring the server can match
    messages and solve positions.
    """

    def __init__(self, feeders, aircraft, scheduler, mlat_warmup=25.0):
        self.feeders = feeders
        self.aircraft = aircraft
        self.scheduler = scheduler
        self.mlat_warmup = mlat_warmup
        self._tasks = []

    async def start(self):
        self._tasks.append(asyncio.ensure_future(self._sync_loop()))
        self._tasks.append(asyncio.ensure_future(self._mlat_loop()))

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def _connected_feeders(self):
        return [f for f in self.feeders if f.connected]

    async def _sync_loop(self):
        while True:
            await asyncio.sleep(random.uniform(0.3, 0.8))
            connected = self._connected_feeders()
            if len(connected) < 2:
                continue
            ac = random.choice(self.aircraft)
            timestamps = self.scheduler.compute_sync_timestamps(ac, connected)
            for f in connected:
                if f.feeder_id in timestamps:
                    et, ot = timestamps[f.feeder_id]
                    f.send_geometric_sync(ac, et, ot)

    async def _mlat_loop(self):
        await asyncio.sleep(self.mlat_warmup)
        log.info('Geometric: mlat warmup complete, starting mlat messages')
        while True:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            connected = self._connected_feeders()
            if len(connected) < 3:
                continue
            ac = random.choice(self.aircraft)
            timestamps = self.scheduler.compute_mlat_timestamps(ac, connected)
            use_even = random.random() < 0.5
            for f in connected:
                if f.feeder_id in timestamps:
                    f.send_geometric_mlat(ac, timestamps[f.feeder_id], use_even)


# ---------------------------------------------------------------------------
# Fake feeder (simulates a mlat-client connecting to mlat-server)
# ---------------------------------------------------------------------------

class FakeFeeder:
    """A single fake mlat-client connection."""

    def __init__(self, feeder_id, host, port, aircraft_pool, use_compression=False,
                 geometric_mode=False, sim_clock=None, lat=None, lon=None, alt=None):
        self.feeder_id = feeder_id
        self.host = host
        self.port = port
        self.aircraft_pool = aircraft_pool  # list of AircraftState
        self.use_compression = use_compression
        self.geometric_mode = geometric_mode
        self.sim_clock = sim_clock

        self.user = f'loadgen-{feeder_id:04d}'
        if lat is not None:
            self.lat = lat
            self.lon = lon
            self.alt = alt
        else:
            # Spread feeders across a ~200km area (US midwest-ish)
            self.lat = 39.0 + random.uniform(-1.0, 1.0)
            self.lon = -94.0 + random.uniform(-1.0, 1.0)
            self.alt = random.uniform(100, 500)
        self.ecef = llh2ecef_pure(self.lat, self.lon, self.alt) if geometric_mode else None

        self.reader = None
        self.writer = None
        self.connected = False
        self._stop = False
        self._task = None
        self._compressor = None  # set during zlib negotiation

        # dump1090-style 12 MHz clock
        self.clock_freq = 12e6
        self.clock_base = random.randint(0, int(86400 * self.clock_freq))

        # aircraft this feeder currently "sees" (AircraftState objects)
        self.visible_aircraft = []

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
        self._send_raw({'seen': [ac.icao_hex for ac in self.visible_aircraft]})

        read_task = asyncio.ensure_future(self._read_loop_raw())
        try:
            tick = 0
            while not self._stop and self.writer is not None:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                tick += 1

                if not self.geometric_mode:
                    # Send correlated sync messages (most common) — uses shared
                    # DF17 message pairs so multiple feeders create clock pairings
                    if random.random() < 0.7:
                        self._send_correlated_sync_raw()

                    # Send correlated mlat messages — same message from multiple
                    # feeders exercises the cohort/solver path
                    if random.random() < 0.3:
                        self._send_correlated_mlat_raw()

                # Periodic seen/lost updates
                if tick % 10 == 0:
                    self._update_visible_aircraft()
                    self._send_raw({'seen': [ac.icao_hex for ac in self.visible_aircraft]})

                # Heartbeat
                if tick % 30 == 0:
                    self._send_raw({'heartbeat': {'server_time': round(time.time(), 3)}})

                # Rate report
                if tick % 60 == 0:
                    rate = {ac.icao_hex: round(random.uniform(0.5, 5.0), 1)
                            for ac in self.visible_aircraft}
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
        if self.writer and not self._stop and not self.writer.transport.is_closing():
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

    def _send_correlated_sync_raw(self):
        """Send a sync message using shared DF17 even/odd message pairs.

        Multiple feeders sending the same (em, om) pair causes the server to
        build SyncPoints and ClockPairing objects between receiver pairs.
        """
        if len(self.visible_aircraft) < 1:
            return
        ac = random.choice(self.visible_aircraft)
        et = self._get_clock_timestamp()
        ot = self._get_clock_timestamp()
        self._send_raw({
            'sync': {
                'et': et,
                'ot': ot,
                'em': ac.even_msg,
                'om': ac.odd_msg,
            }
        })

    def _send_correlated_mlat_raw(self):
        """Send an mlat message using a shared DF17 message.

        Multiple feeders sending the same message creates MessageGroups
        with 3+ copies, exercising the cohort → normalize → solver path.
        """
        if not self.visible_aircraft:
            return
        ac = random.choice(self.visible_aircraft)
        t = self._get_clock_timestamp()
        # randomly pick even or odd message
        m = ac.even_msg if random.random() < 0.5 else ac.odd_msg
        self._send_raw({
            'mlat': {
                't': t,
                'm': m,
            }
        })

    def send_message(self, msg):
        """Send a message, dispatching to raw or zlib as negotiated."""
        if self._compressor is not None:
            self._send_zlib(self._compressor, msg)
        else:
            self._send_raw(msg)

    def send_geometric_sync(self, aircraft, et, ot):
        """Send a sync message with broadcaster-computed timestamps."""
        self.send_message({
            'sync': {'et': et, 'ot': ot, 'em': aircraft.even_msg, 'om': aircraft.odd_msg}
        })

    def send_geometric_mlat(self, aircraft, t, use_even):
        """Send an mlat message with broadcaster-computed timestamp."""
        m = aircraft.even_msg if use_even else aircraft.odd_msg
        self.send_message({'mlat': {'t': t, 'm': m}})

    def _update_visible_aircraft(self):
        """Randomly adjust which aircraft this feeder sees.

        Aircraft are drawn from the shared pool so multiple feeders see
        the same aircraft and send correlated messages.
        In geometric mode, all aircraft are always visible.
        """
        if self.geometric_mode:
            self.visible_aircraft = list(self.aircraft_pool)
            return
        pool = self.aircraft_pool
        target_count = random.randint(5, min(30, len(pool)))

        # lose some
        if self.visible_aircraft and random.random() < 0.3:
            to_lose = random.sample(self.visible_aircraft,
                                    k=min(3, len(self.visible_aircraft)))
            for ac in to_lose:
                self.visible_aircraft.remove(ac)

        # gain some from shared pool
        current_set = set(id(ac) for ac in self.visible_aircraft)
        candidates = [ac for ac in pool if id(ac) not in current_set]
        while len(self.visible_aircraft) < target_count and candidates:
            pick = random.choice(candidates)
            self.visible_aircraft.append(pick)
            candidates.remove(pick)

    # --- zlib message loop (for zlib/zlib2 compression) ---
    async def _message_loop_zlib(self, compress_mode):
        """Send/receive loop with zlib compression."""
        compressor = zlib.compressobj(1)
        self._compressor = compressor  # store for geometric broadcaster access
        decompressor = zlib.decompressobj()

        self._update_visible_aircraft()
        self._send_zlib(compressor, {'seen': [ac.icao_hex for ac in self.visible_aircraft]})

        read_task = asyncio.ensure_future(self._read_loop_zlib(decompressor, compress_mode))
        try:
            tick = 0
            while not self._stop and self.writer is not None:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                tick += 1

                if not self.geometric_mode:
                    if random.random() < 0.7:
                        self._send_correlated_sync_zlib(compressor)

                    if random.random() < 0.3:
                        self._send_correlated_mlat_zlib(compressor)

                if tick % 10 == 0:
                    self._update_visible_aircraft()
                    self._send_zlib(compressor, {'seen': [ac.icao_hex for ac in self.visible_aircraft]})

                if tick % 30 == 0:
                    self._send_zlib(compressor, {'heartbeat': {'server_time': round(time.time(), 3)}})

        finally:
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass

    def _send_zlib(self, compressor, msg):
        if self.writer and not self._stop and not self.writer.transport.is_closing():
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

    def _send_correlated_sync_zlib(self, compressor):
        if len(self.visible_aircraft) < 1:
            return
        ac = random.choice(self.visible_aircraft)
        et = self._get_clock_timestamp()
        ot = self._get_clock_timestamp()
        self._send_zlib(compressor, {
            'sync': {'et': et, 'ot': ot, 'em': ac.even_msg, 'om': ac.odd_msg}
        })

    def _send_correlated_mlat_zlib(self, compressor):
        if not self.visible_aircraft:
            return
        ac = random.choice(self.visible_aircraft)
        m = ac.even_msg if random.random() < 0.5 else ac.odd_msg
        self._send_zlib(compressor, {
            'mlat': {'t': self._get_clock_timestamp(), 'm': m}
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

async def move_aircraft(aircraft_pool, interval=5.0):
    """Periodically move aircraft and regenerate their DF17 messages."""
    while True:
        await asyncio.sleep(interval)
        for ac in aircraft_pool:
            ac.step()


async def main(args):
    broadcaster = None

    if args.geometric:
        # Geometric mode: fixed Baffin Bay positions with TDOA-correct timestamps
        aircraft_pool = [
            GeometricAircraftState(icao, lat, lon, alt, hdg, spd)
            for icao, lat, lon, alt, hdg, spd in BAFFIN_AIRCRAFT
        ]
        log.info('Geometric mode: %d aircraft over Baffin Bay', len(aircraft_pool))

        feeder_clocks = {}
        feeders = []
        for i, (rlat, rlon, ralt, rname) in enumerate(BAFFIN_RECEIVERS):
            clock = SimulatedClock()
            feeder_clocks[i] = clock
            f = FakeFeeder(
                feeder_id=i,
                host=args.host,
                port=args.port,
                aircraft_pool=aircraft_pool,
                use_compression=args.compress,
                geometric_mode=True,
                sim_clock=clock,
                lat=rlat, lon=rlon, alt=ralt,
            )
            feeders.append(f)
            log.info('  Receiver %d: %s (%.1f, %.1f)', i, rname, rlat, rlon)

        scheduler = TransmissionScheduler(feeder_clocks)
        broadcaster = GeometricBroadcaster(feeders, aircraft_pool, scheduler)
    else:
        aircraft_pool = make_aircraft_pool(args.aircraft)
        log.info('Created pool of %d fake aircraft with valid DF17 messages', len(aircraft_pool))

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
    log.info('Starting %d feeders against %s:%d ...', len(feeders), args.host, args.port)
    for i, feeder in enumerate(feeders):
        await feeder.start()
        if i < len(feeders) - 1:
            await asyncio.sleep(args.connect_delay)

    log.info('All feeders started')

    # Start geometric broadcaster if enabled
    if broadcaster:
        await broadcaster.start()
        log.info('Geometric broadcaster started (mlat warmup: %.0fs)', broadcaster.mlat_warmup)

    # Start aircraft movement (regenerates DF17 messages periodically)
    aircraft_task = asyncio.ensure_future(move_aircraft(aircraft_pool, interval=5.0))

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
    aircraft_task.cancel()
    if broadcaster:
        await broadcaster.stop()
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
    p.add_argument('--geometric', action='store_true',
                   help='enable TDOA-consistent timestamps for solvable positions over Baffin Bay')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    asyncio.run(main(args))
