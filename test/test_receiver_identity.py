# -*- mode: python; indent-tabs-mode: nil -*-

"""Unit tests for mlat.receiver_identity.is_same_receiver.

Stdlib-only (unittest) so it runs without building the Cython extensions:
    python3 -m unittest test.test_receiver_identity
"""

import unittest

from mlat import receiver_identity as ri

IP = "5.128.111.11"
LAT, LON = 55.02, 82.93


class IsSameReceiverTests(unittest.TestCase):
    def test_same_ip_identical_position_is_same(self):
        self.assertTrue(ri.is_same_receiver(IP, LAT, LON, IP, LAT, LON))

    def test_same_ip_within_threshold_is_same(self):
        # ~0.5 km north (0.0045 deg latitude is roughly 0.5 km)
        self.assertTrue(ri.is_same_receiver(IP, LAT, LON, IP, LAT + 0.0045, LON))

    def test_same_ip_far_apart_is_not_same(self):
        # Same IP but ~11 km apart: treated as distinct receivers (e.g. two feeders behind one
        # NAT at different locations), not collapsed onto one identity.
        self.assertFalse(ri.is_same_receiver(IP, LAT, LON, IP, LAT + 0.10, LON))

    def test_different_ip_same_position_is_not_same(self):
        self.assertFalse(ri.is_same_receiver(IP, LAT, LON, "203.0.113.7", LAT, LON))

    def test_missing_position_is_not_same(self):
        self.assertFalse(ri.is_same_receiver(IP, LAT, LON, IP, None, None))
        self.assertFalse(ri.is_same_receiver(IP, None, None, IP, LAT, LON))

    def test_missing_ip_is_not_same(self):
        self.assertFalse(ri.is_same_receiver(None, LAT, LON, IP, LAT, LON))
        self.assertFalse(ri.is_same_receiver("", LAT, LON, IP, LAT, LON))

    def test_haversine_one_degree_latitude_is_about_111km(self):
        self.assertAlmostEqual(ri.haversine_km(0.0, 0.0, 1.0, 0.0), 111.19, places=1)


if __name__ == "__main__":
    unittest.main()
