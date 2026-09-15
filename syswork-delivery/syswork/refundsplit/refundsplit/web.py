"""A tiny local web surface for the refund allocator.

Standard library only, on purpose: `python3 -m refundsplit` has to work on a
laptop with no virtualenv, no install step and no network. The page is the
demo's face; the arithmetic it shows comes from `allocate`, untouched.
"""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .allocate import Line, allocate
from .money import MoneyError, format_money, from_cents, to_cents

STATIC = Path(__file__).parent / "static"
MAX_BODY_BYTES = 64 * 1024


def allocation_report(payload: dict) -> dict:
    """Run one allocation and describe it for the page.

    Returns the per-line shares plus the reconciliation check the whole demo
    turns on: does the sum of the parts equal the refund total?
    """
    total_cents = to_cents(str(payload["total"]))
    lines = [
        Line(line_id=str(item["line_id"]), amount_cents=to_cents(str(item["amount"])))
        for item in payload["lines"]
    ]
    shares = allocate(total_cents, lines)
    allocated = sum(share.amount_cents for share in shares)
    return {
        "total": str(from_cents(total_cents)),
        "total_display": format_money(total_cents),
        "shares": [
            {
                "line_id": share.line_id,
                "amount": str(from_cents(share.amount_cents)),
                "display": format_money(share.amount_cents),
            }
            for share in shares
        ],
        "allocated": str(from_cents(allocated)),
        "allocated_display": format_money(allocated),
        "difference_display": format_money(allocated - total_cents),
        "reconciled": allocated == total_cents,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "refundsplit"

    def do_GET(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        if self.path in ("/", "/index.html"):
            page = (STATIC / "app.html").read_bytes()
            self._send(HTTPStatus.OK, page, "text/html; charset=utf-8")
        elif self.path == "/healthz":
            self._send_json(HTTPStatus.OK, {"ok": True})
        else:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        if self.path != "/api/allocate":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY_BYTES:
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "body too large"})
            return
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
            report = allocation_report(payload)
        except (MoneyError, KeyError, TypeError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        self._send_json(HTTPStatus.OK, report)

    def log_message(self, fmt: str, *args: object) -> None:
        """Quiet by default -- a chatty server is noise during a live demo."""

    def _send_json(self, status: HTTPStatus, body: dict) -> None:
        self._send(status, json.dumps(body).encode(), "application/json")

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"refundsplit running on http://{host}:{port}  (ctrl-c to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        httpd.server_close()
