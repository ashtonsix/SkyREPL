"""Tests for spot.py — spot interruption detection and handling."""

import json
import threading
import time
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import MagicMock, call, patch


# ---------------------------------------------------------------------------
# IMDS helpers
# ---------------------------------------------------------------------------

class TestGetImdsToken(unittest.TestCase):
    def test_returns_none_when_imds_unavailable(self):
        """No IMDS endpoint → get_imds_token returns None."""
        # Patch urlopen to raise ConnectionRefusedError (typical in non-EC2 envs)
        with patch("spot.urllib.request.urlopen", side_effect=ConnectionRefusedError()):
            from spot import get_imds_token
            result = get_imds_token()
        self.assertIsNone(result)

    def test_returns_token_on_success(self):
        """When IMDS responds with a token, it is returned as a string."""
        class FakeResp:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                return b"test-token-abc"

        with patch("spot.urllib.request.urlopen", return_value=FakeResp()):
            from spot import get_imds_token
            token = get_imds_token()
        self.assertEqual(token, "test-token-abc")

    def test_returns_none_on_timeout(self):
        """Timeout during token fetch → None (not an exception)."""
        import socket
        with patch("spot.urllib.request.urlopen", side_effect=socket.timeout()):
            from spot import get_imds_token
            result = get_imds_token()
        self.assertIsNone(result)


class TestIsSpotInstance(unittest.TestCase):
    def test_returns_false_when_imds_unavailable(self):
        """Default test environment has no IMDS → returns False."""
        with patch("spot.urllib.request.urlopen", side_effect=ConnectionRefusedError()):
            from spot import is_spot_instance
            self.assertFalse(is_spot_instance())

    def test_returns_false_when_lifecycle_is_on_demand(self):
        """on-demand lifecycle value → returns False."""
        class FakeResp:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                return b"on-demand"

        with patch("spot.urllib.request.urlopen", return_value=FakeResp()):
            from spot import is_spot_instance
            self.assertFalse(is_spot_instance())

    def test_returns_true_when_lifecycle_is_spot(self):
        """spot lifecycle value → returns True."""
        call_count = [0]

        class FakeResp:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                call_count[0] += 1
                # First call: return token; second call: return lifecycle
                if call_count[0] == 1:
                    return b"test-token"
                return b"spot"

        with patch("spot.urllib.request.urlopen", return_value=FakeResp()):
            from spot import is_spot_instance
            self.assertTrue(is_spot_instance())


class TestCheckSpotInterruption(unittest.TestCase):
    def test_returns_none_on_404(self):
        """404 from IMDS (no interruption notice) → returns None."""
        err = urllib.error.HTTPError(
            url="http://169.254.169.254/...",
            code=404,
            msg="Not Found",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,  # type: ignore[arg-type]
        )
        with patch("spot.urllib.request.urlopen", side_effect=err):
            from spot import check_spot_interruption
            result = check_spot_interruption("dummy-token")
        self.assertIsNone(result)

    def test_returns_none_when_connection_refused(self):
        """Connection error → None (IMDS not available)."""
        with patch("spot.urllib.request.urlopen", side_effect=ConnectionRefusedError()):
            from spot import check_spot_interruption
            result = check_spot_interruption("dummy-token")
        self.assertIsNone(result)

    def test_returns_action_dict_on_interruption(self):
        """When IMDS returns an interruption notice, parse and return it."""
        notice = {"action": "terminate", "time": "2026-02-19T12:00:00Z"}

        class FakeResp:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                return json.dumps(notice).encode()

        with patch("spot.urllib.request.urlopen", return_value=FakeResp()):
            from spot import check_spot_interruption
            result = check_spot_interruption("test-token")
        self.assertIsNotNone(result)
        self.assertEqual(result["action"], "terminate")
        self.assertEqual(result["time"], "2026-02-19T12:00:00Z")

    def test_returns_none_on_non_404_http_error(self):
        """Non-404 HTTP errors (500, etc.) → None (best-effort)."""
        err = urllib.error.HTTPError(
            url="http://169.254.169.254/...",
            code=500,
            msg="Internal Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,  # type: ignore[arg-type]
        )
        with patch("spot.urllib.request.urlopen", side_effect=err):
            from spot import check_spot_interruption
            result = check_spot_interruption("dummy-token")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# SpotMonitor
# ---------------------------------------------------------------------------

class TestSpotMonitorStart(unittest.TestCase):
    def test_does_not_start_thread_when_not_spot(self):
        """SpotMonitor.start() is a no-op on non-spot instances."""
        from spot import SpotMonitor

        executor = MagicMock()
        executor.current_run_id = None
        shutdown = threading.Event()

        with patch("spot.is_spot_instance", return_value=False):
            monitor = SpotMonitor(
                instance_id=1,
                control_url="http://localhost:3000",
                executor=executor,
                shutdown_event=shutdown,
            )
            monitor.start()

        self.assertIsNone(monitor._thread)

    def test_starts_daemon_thread_on_spot_instance(self):
        """SpotMonitor.start() creates a daemon thread on spot instances."""
        from spot import SpotMonitor

        executor = MagicMock()
        executor.current_run_id = None
        shutdown = threading.Event()

        with patch("spot.is_spot_instance", return_value=True), \
             patch("spot.get_imds_token", return_value="token"), \
             patch("spot.check_spot_interruption", return_value=None):
            monitor = SpotMonitor(
                instance_id=42,
                control_url="http://localhost:3000",
                executor=executor,
                shutdown_event=shutdown,
            )
            monitor.start()
            # Give thread a moment to start
            time.sleep(0.05)
            # Signal shutdown
            shutdown.set()
            # Join with timeout
            if monitor._thread:
                monitor._thread.join(timeout=1.0)

        self.assertIsNotNone(monitor._thread)
        self.assertTrue(monitor._thread.daemon)  # type: ignore[union-attr]
        self.assertEqual(monitor._thread.name, "spot-monitor")  # type: ignore[union-attr]


class TestSpotMonitorHandleInterruption(unittest.TestCase):
    """Tests for SpotMonitor._handle_interruption()."""

    def _make_monitor(self, executor=None):
        from spot import SpotMonitor
        if executor is None:
            executor = MagicMock()
            executor.current_run_id = None
        shutdown = threading.Event()
        monitor = SpotMonitor(
            instance_id=99,
            control_url="http://control:3000",
            executor=executor,
            shutdown_event=shutdown,
        )
        return monitor, shutdown

    def test_sets_shutdown_event(self):
        """After handling interruption, shutdown_event must be set."""
        monitor, shutdown = self._make_monitor()

        with patch.object(monitor, "_notify_control_plane"):
            monitor._handle_interruption({"action": "terminate"})

        self.assertTrue(shutdown.is_set())

    def test_calls_notify_control_plane_start_and_complete(self):
        """Both 'start' and 'complete' notifications are sent."""
        monitor, _ = self._make_monitor()

        with patch.object(monitor, "_notify_control_plane") as mock_notify:
            monitor._handle_interruption({"action": "terminate", "time": "2026-02-19T12:00:00Z"})

        calls = mock_notify.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], call("start", {"action": "terminate", "time": "2026-02-19T12:00:00Z"}))
        self.assertEqual(calls[1], call("complete", {"action": "terminate", "time": "2026-02-19T12:00:00Z"}))

    def test_cancels_active_run_via_executor(self):
        """When a run is active, cancel_run is called with the correct run_id."""
        executor = MagicMock()
        executor.current_run_id = 42
        monitor, _ = self._make_monitor(executor=executor)

        # Make current_run_id return None after cancel_run is called
        def side_effect(msg):
            executor.current_run_id = None

        executor.cancel_run.side_effect = side_effect

        with patch.object(monitor, "_notify_control_plane"):
            monitor._handle_interruption({"action": "terminate"})

        executor.cancel_run.assert_called_once_with(
            {"run_id": 42, "reason": "spot_interruption"}
        )

    def test_no_cancel_when_no_active_run(self):
        """When executor has no active run, cancel_run is NOT called."""
        executor = MagicMock()
        executor.current_run_id = None
        monitor, _ = self._make_monitor(executor=executor)

        with patch.object(monitor, "_notify_control_plane"):
            monitor._handle_interruption({"action": "terminate"})

        executor.cancel_run.assert_not_called()


class TestSpotMonitorNotifyControlPlane(unittest.TestCase):
    """Tests for SpotMonitor._notify_control_plane()."""

    def _make_monitor(self):
        from spot import SpotMonitor
        executor = MagicMock()
        executor.current_run_id = None
        shutdown = threading.Event()
        return SpotMonitor(
            instance_id=7,
            control_url="http://control:3000",
            executor=executor,
            shutdown_event=shutdown,
        )

    def test_posts_to_correct_url(self):
        """Notification posts to /v1/agent/spot-interrupt-{phase}."""
        monitor = self._make_monitor()

        opened_urls: list = []

        class FakeResp:
            status = 200
            def read(self):
                return b"{}"

        def fake_urlopen(req, timeout=None):
            opened_urls.append(req.full_url)
            return FakeResp()

        with patch("spot.urllib.request.urlopen", side_effect=fake_urlopen):
            monitor._notify_control_plane("start", {"action": "terminate"})

        self.assertEqual(len(opened_urls), 1)
        self.assertIn("spot-interrupt-start", opened_urls[0])

    def test_sends_correct_payload(self):
        """Payload includes instance_id, action, and action_time."""
        monitor = self._make_monitor()

        sent_bodies: list = []

        class FakeResp:
            def read(self):
                return b"{}"

        def fake_urlopen(req, timeout=None):
            sent_bodies.append(json.loads(req.data.decode()))
            return FakeResp()

        action = {"action": "terminate", "time": "2026-02-19T12:00:00Z"}
        with patch("spot.urllib.request.urlopen", side_effect=fake_urlopen):
            monitor._notify_control_plane("start", action)

        self.assertEqual(len(sent_bodies), 1)
        payload = sent_bodies[0]
        self.assertEqual(payload["instance_id"], 7)
        self.assertEqual(payload["action"], "terminate")
        self.assertEqual(payload["action_time"], "2026-02-19T12:00:00Z")

    def test_does_not_raise_on_connection_error(self):
        """Control plane unreachable → best-effort, no exception raised."""
        monitor = self._make_monitor()

        with patch("spot.urllib.request.urlopen", side_effect=ConnectionRefusedError()):
            # Must not raise
            monitor._notify_control_plane("complete", {"action": "terminate"})


if __name__ == "__main__":
    unittest.main()
