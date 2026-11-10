# -*- mode: python; indent-tabs-mode: nil -*-

"""Tests that a disconnected receiver leaves no references in tracker state.

Needs the Cython extensions built (python3 setup.py build_ext --inplace):
    python3 -m unittest test.test_tracker_cleanup
"""

import gc
import unittest
import weakref

from mlat import tracker
from mlat.coordinator import Coordinator, Receiver, clocktrack


class _Loop:
    def call_soon(self, *args, **kwargs):
        pass

    def call_later(self, *args, **kwargs):
        pass


class _Conn:
    def request_traffic(self, receiver, icao_set):
        pass


class _Coordinator:
    """Just enough of Coordinator for Receiver.__init__."""

    def receiver_location_update(self, receiver, position_llh):
        pass


def _receiver(uid):
    return Receiver(uid, 'r%d' % uid, _Conn(), 'dump1090', position_llh=(48.0, 15.0, 300.0),
                    privacy=False, connection_info='test', uuid=None,
                    coordinator=_Coordinator(), clock_tracker=None)


class RemoveAllTests(unittest.TestCase):
    def setUp(self):
        self.tracker = tracker.Tracker(coordinator=None, partition=(1, 1), loop=_Loop())

    def _interest_from_rate_report(self, receiver, tracked, reported):
        # Interest sets can include aircraft that appear in a rate report but are
        # not in receiver.tracking.
        self.tracker.add(receiver, tracked)
        acs = {self.tracker.aircraft[i] for i in reported}
        receiver.update_interest_sets(acs, acs, acs)
        receiver.refresh_traffic_requests()

    def test_disconnect_clears_interest_links_outside_tracking(self):
        a, b = _receiver(1), _receiver(2)
        self.tracker.add(b, {0x100, 0x200})
        self._interest_from_rate_report(a, tracked={0x100}, reported={0x100, 0x200})

        self.tracker.remove_all(a)

        for ac in self.tracker.aircraft.values():
            for s in (ac.tracking, ac.sync_interest, ac.mlat_interest, ac.adsb_seen):
                self.assertNotIn(a, s)
        for s in (a.tracking, a.sync_interest, a.mlat_interest, a.adsb_seen, a.requested):
            self.assertEqual(len(s), 0)

    def test_disconnected_receiver_is_collectable(self):
        a, b = _receiver(1), _receiver(2)
        self.tracker.add(b, {0x100, 0x200})
        self._interest_from_rate_report(a, tracked={0x100}, reported={0x100, 0x200})
        ref = weakref.ref(a)

        self.tracker.remove_all(a)
        del a
        gc.collect()

        self.assertIsNone(ref())

    def test_untracked_aircraft_is_dropped_when_last_link_goes(self):
        a = _receiver(1)
        self.tracker.add(a, {0x100})
        self.tracker.remove_all(a)
        self.assertNotIn(0x100, self.tracker.aircraft)

    def test_replaced_aircraft_entry_is_kept(self):
        # An aircraft object linked to the receiver may already have been replaced in
        # tracker.aircraft by a newer object for the same icao; that entry stays.
        a, b = _receiver(1), _receiver(2)
        self.tracker.add(a, {0x100})
        stale = self.tracker.aircraft[0x100]
        self.tracker.remove(a, {0x100})
        a.requested.add(stale)
        self.tracker.add(b, {0x100})
        current = self.tracker.aircraft[0x100]

        self.tracker.remove_all(a)

        self.assertIs(self.tracker.aircraft.get(0x100), current)


class DropAircraftTests(unittest.TestCase):
    def setUp(self):
        self.tracker = tracker.Tracker(coordinator=None, partition=(1, 1), loop=_Loop())
        self.a, self.b = _receiver(1), _receiver(2)
        self.tracker.add(self.b, {0x100, 0x200})
        # a tracks only 0x100 but has interest in 0x200 from its rate report
        self.tracker.add(self.a, {0x100})
        acs = {self.tracker.aircraft[i] for i in (0x100, 0x200)}
        self.a.update_interest_sets(acs, acs, acs)
        self.a.refresh_traffic_requests()

    def _assert_unlinked(self, ac):
        for s in (self.a.sync_interest, self.a.mlat_interest, self.a.adsb_seen, self.a.requested):
            self.assertNotIn(ac, s)
        for s in (ac.sync_interest, ac.mlat_interest, ac.adsb_seen):
            self.assertEqual(len(s), 0)

    def test_remove_unlinks_dropped_aircraft_from_other_receivers(self):
        ac = self.tracker.aircraft[0x200]

        self.tracker.remove(self.b, {0x200})

        self.assertNotIn(0x200, self.tracker.aircraft)
        self._assert_unlinked(ac)
        self.assertIn(self.tracker.aircraft[0x100], self.a.sync_interest)

    def test_dropped_aircraft_is_collectable(self):
        ref = weakref.ref(self.tracker.aircraft[0x200])

        self.tracker.remove(self.b, {0x200})
        gc.collect()

        self.assertIsNone(ref())

    def test_remove_all_unlinks_dropped_aircraft_from_other_receivers(self):
        ac = self.tracker.aircraft[0x200]

        self.tracker.remove_all(self.b)

        self.assertNotIn(0x200, self.tracker.aircraft)
        self._assert_unlinked(ac)

    def test_prune_unlinks_stale_aircraft(self):
        ac = self.tracker.aircraft[0x200]
        self.tracker.remove(self.b, {0x200})
        # re-add an untracked entry with interest links, as left by older state
        self.tracker.aircraft[0x200] = ac
        ac.seen = 0
        ac.sync_interest.add(self.a)
        self.a.sync_interest.add(ac)

        class _Coord:
            stats_pruned_aircraft_total = 0
        coord = _Coord()
        coord.tracker = self.tracker
        Coordinator._prune_stale_aircraft(coord)

        self.assertNotIn(0x200, self.tracker.aircraft)
        self._assert_unlinked(ac)
        self.assertEqual(coord.stats_pruned_aircraft_total, 1)


class ClockTrackerReceiverTests(unittest.TestCase):
    def setUp(self):
        self.ct = clocktrack.ClockTracker(coordinator=None, loop=_Loop())
        self.r = [_receiver(i) for i in range(4)]
        for i in range(4):
            for j in range(i + 1, 4):
                base, peer = self.r[i], self.r[j]
                pairing = clocktrack.ClockPairing(base, peer, 1)
                self.ct.clock_pairs[(base, peer)] = pairing
                base.sync_peers[1] += 1
                peer.sync_peers[1] += 1

    def test_clock_reset_keeps_all_pairs(self):
        self.ct.receiver_clock_reset(self.r[0])
        self.assertEqual(len(self.ct.clock_pairs), 6)

    def test_disconnect_removes_only_that_receivers_pairs(self):
        self.ct.receiver_disconnect(self.r[0])
        self.assertEqual(len(self.ct.clock_pairs), 3)
        for k in self.ct.clock_pairs:
            self.assertNotIn(self.r[0], k)
        self.assertEqual(self.r[0].sync_peers[1], 0)
        for r in self.r[1:]:
            self.assertEqual(r.sync_peers[1], 2)


if __name__ == '__main__':
    unittest.main()
