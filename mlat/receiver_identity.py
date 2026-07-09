# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-server.
#
# Helpers for recognising a reconnecting MLAT client as the same physical receiver.
#
# A client that supplies no uuid in its handshake cannot be matched to an existing connection by
# uuid, so a feeder that reconnects before its previous session is reaped is given a fresh random
# name each time. Comparing the source IP together with the reported position lets the handshake
# recognise a feeder reconnecting to itself (and replace the stale session, keeping the name)
# without merging two genuinely distinct feeders that share one public IP (NAT/VPN) but sit at
# different locations. Both signals must agree: IP alone would merge co-located NAT feeders, and
# position alone would merge unrelated feeders that happen to report the same spot.
#
# Kept to the standard library so the decision is easy to read and test without building the
# Cython extensions in this package.

import math

# Two same-IP connections whose reported positions are within this distance are treated as the
# same physical receiver. Kept small so distinct feeders behind one IP at different locations are
# not merged; a feeder that reports a widely varying position is left to downstream dedup rather
# than collapsed here.
SAME_RECEIVER_MAX_KM = 1.0

_EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres between two lat/lon points given in degrees."""
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def is_same_receiver(existing_source_ip, existing_lat, existing_lon,
                     new_source_ip, new_lat, new_lon,
                     max_km=SAME_RECEIVER_MAX_KM):
    """Return True when a no-uuid reconnect is, on balance, the same physical receiver as an
    existing session: identical source IP and a reported position within max_km. Any missing
    piece returns False, since the match cannot be corroborated."""
    if not existing_source_ip or not new_source_ip:
        return False
    if existing_source_ip != new_source_ip:
        return False
    if None in (existing_lat, existing_lon, new_lat, new_lon):
        return False
    return haversine_km(existing_lat, existing_lon, new_lat, new_lon) <= max_km
