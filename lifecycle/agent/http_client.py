"""Shared HTTP client for agent modules."""
import http.client
import json
from urllib.parse import urlparse

_control_plane_url: str = ""
_auth_token: str = ""


def configure(control_plane_url: str, auth_token: str = "") -> None:
    """Set connection parameters. Called once at startup."""
    global _control_plane_url, _auth_token
    _control_plane_url = control_plane_url
    _auth_token = auth_token


def get_control_plane_url() -> str:
    """Return configured control plane URL."""
    return _control_plane_url


def http_post(path: str, payload: object, timeout: int = 10) -> http.client.HTTPResponse:
    """POST JSON to control plane."""
    parsed = urlparse(_control_plane_url)
    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)

    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}

    # Add auth header if token is set
    if _auth_token:
        headers["Authorization"] = f"Bearer {_auth_token}"

    try:
        conn.request("POST", path, body=body, headers=headers)
        return conn.getresponse()
    except Exception:
        conn.close()
        raise
