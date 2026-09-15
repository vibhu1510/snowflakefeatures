import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from refundsplit.web import Handler


@pytest.fixture
def base_url():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


def post_allocate(base_url, payload):
    request = urllib.request.Request(
        f"{base_url}/api/allocate",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def test_healthz(base_url):
    with urllib.request.urlopen(f"{base_url}/healthz") as response:
        assert json.loads(response.read()) == {"ok": True}


def test_serves_the_page(base_url):
    with urllib.request.urlopen(base_url + "/") as response:
        assert b"refundsplit" in response.read()


def test_allocates_over_http(base_url):
    report = post_allocate(
        base_url,
        {"total": "100.00", "lines": [{"line_id": "L-001", "amount": "50.00"},
                                      {"line_id": "L-002", "amount": "50.00"}]},
    )
    assert [s["display"] for s in report["shares"]] == ["$50.00", "$50.00"]
    assert report["reconciled"] is True


def test_rejects_bad_input(base_url):
    with pytest.raises(urllib.error.HTTPError) as caught:
        post_allocate(base_url, {"total": "not-money", "lines": []})
    assert caught.value.code == 400
