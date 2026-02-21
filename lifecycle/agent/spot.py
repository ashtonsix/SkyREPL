"""Spot interruption detection and handling for AWS EC2 spot instances."""

import json
import logging
import threading
import time
import urllib.error
import urllib.request

logger = logging.getLogger("skyrepl.spot")

IMDS_TOKEN_URL = "http://169.254.169.254/latest/api/token"
IMDS_SPOT_URL = "http://169.254.169.254/latest/meta-data/spot/instance-action"
IMDS_INSTANCE_LIFE_URL = "http://169.254.169.254/latest/meta-data/instance-life-cycle"
POLL_INTERVAL = 5  # seconds
CHECKPOINT_BUDGET_S = 90  # seconds for checkpoint before termination

# Token refresh period (refresh 1 minute before it would expire)
TOKEN_TTL_S = 300
TOKEN_REFRESH_EARLY_S = 60


def get_imds_token(ttl: int = TOKEN_TTL_S) -> "str | None":
    """
    Get IMDSv2 session token via PUT to token URL.

    IMDSv2 requires a session token for all metadata requests.
    Returns None if IMDS is unavailable (e.g., not on EC2, or test env).
    """
    try:
        req = urllib.request.Request(
            IMDS_TOKEN_URL,
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": str(ttl)},
        )
        with urllib.request.urlopen(req, timeout=1) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception:
        return None


def is_spot_instance() -> bool:
    """
    Check if running on a spot instance via IMDSv2.

    Queries instance-life-cycle, returns True only when the value is "spot".
    Returns False on any error (IMDS unavailable, non-EC2 environment, etc.).
    """
    token = get_imds_token()
    if not token:
        return False
    try:
        req = urllib.request.Request(
            IMDS_INSTANCE_LIFE_URL,
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req, timeout=1) as resp:
            lifecycle = resp.read().decode("utf-8").strip()
            return lifecycle == "spot"
    except Exception:
        return False


def check_spot_interruption(token: str) -> "dict | None":
    """
    Check for spot interruption notice via IMDSv2.

    Returns a dict like {"action": "terminate", "time": "2024-..."} when
    an interruption notice is present, or None when the endpoint returns
    404 (no notice) or on any connection error.
    """
    try:
        req = urllib.request.Request(
            IMDS_SPOT_URL,
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req, timeout=1) as resp:
            body = resp.read().decode("utf-8").strip()
            return json.loads(body)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # 404 = no interruption notice, this is normal
            return None
        logger.debug("IMDS spot check HTTP error: %s", e.code)
        return None
    except Exception:
        # Connection refused, timeout, etc. — IMDS not available
        return None


class SpotMonitor:
    """Monitors for AWS spot interruption notices and triggers graceful shutdown."""

    def __init__(
        self,
        instance_id: int,
        control_url: str,
        executor: object,
        shutdown_event: threading.Event,
    ) -> None:
        self.instance_id = instance_id
        self.control_url = control_url
        self.executor = executor  # RunExecutor reference for cancel
        self.shutdown_event = shutdown_event
        self._thread: "threading.Thread | None" = None
        self._interrupted = False

    def start(self) -> None:
        """Start the spot monitor daemon thread.

        Skipped entirely when not running on an EC2 spot instance so non-AWS
        deployments and local development pay zero overhead.
        """
        if not is_spot_instance():
            logger.info("Not a spot instance, spot monitor disabled")
            return
        self._thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="spot-monitor",
        )
        self._thread.start()
        logger.info(
            "Spot interruption monitor started (poll every %ds)", POLL_INTERVAL
        )

    def _monitor_loop(self) -> None:
        """Poll IMDS every POLL_INTERVAL seconds for a spot interruption notice."""
        token = get_imds_token()
        token_refresh_at = time.time() + TOKEN_TTL_S - TOKEN_REFRESH_EARLY_S

        while not self.shutdown_event.is_set():
            # Refresh token before it expires
            if time.time() >= token_refresh_at:
                token = get_imds_token()
                token_refresh_at = time.time() + TOKEN_TTL_S - TOKEN_REFRESH_EARLY_S

            if token:
                action = check_spot_interruption(token)
                if action and not self._interrupted:
                    self._interrupted = True
                    self._handle_interruption(action)
                    return  # Monitor's job is done after handling

            self.shutdown_event.wait(POLL_INTERVAL)

    def _handle_interruption(self, action: dict) -> None:
        """
        Handle a spot interruption notice.

        Sequence:
        1. Notify control plane (start phase)
        2. Cancel any active run via executor (give it up to CHECKPOINT_BUDGET_S)
        3. Notify control plane (complete phase)
        4. Signal main shutdown
        """
        logger.warning("SPOT INTERRUPTION: action=%s", action.get("action"))

        # 1. Notify control plane: interruption starting
        self._notify_control_plane("start", action)

        # 2. Cancel current run if active
        if getattr(self.executor, "current_run_id", None):
            run_id = self.executor.current_run_id  # type: ignore[attr-defined]
            logger.info(
                "Cancelling active run %d due to spot interruption", run_id
            )
            self.executor.cancel_run(  # type: ignore[attr-defined]
                {"run_id": run_id, "reason": "spot_interruption"}
            )

            # Wait for executor to finish (up to checkpoint budget)
            deadline = time.time() + CHECKPOINT_BUDGET_S
            while (
                getattr(self.executor, "current_run_id", None)
                and time.time() < deadline
            ):
                time.sleep(1)

        # 3. Notify control plane: interruption handling complete
        self._notify_control_plane("complete", action)

        # 4. Signal main shutdown
        self.shutdown_event.set()

    def _notify_control_plane(self, phase: str, action: dict) -> None:
        """POST spot interrupt notification to control plane (best-effort, 5s timeout)."""
        url = f"{self.control_url}/v1/agent/spot-interrupt-{phase}"
        payload = json.dumps(
            {
                "instance_id": self.instance_id,
                "action": action.get("action", "unknown"),
                "action_time": action.get("time"),
            }
        ).encode()
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            logger.error(
                "Failed to notify control plane (%s): %s", phase, e
            )
