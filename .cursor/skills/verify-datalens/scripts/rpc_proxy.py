#!/usr/bin/env python3
"""Counting JSON-RPC proxy: forwards every request to an upstream RPC and appends one
JSONL record per JSON-RPC call (batch items counted individually)."""

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCK = threading.Lock()
HTTP_REQUESTS = [0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = b'{"status":"ok","role":"verify-datalens-rpc-proxy"}'
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            raw = self.rfile.read(int(self.headers.get("content-length", "0")))
            started = time.time()
            status = 502
            payload = b""
            try:
                request = urllib.request.Request(
                    args.upstream,
                    data=raw,
                    headers={"content-type": "application/json", "user-agent": "verify-datalens"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=60) as response:
                    status = response.status
                    payload = response.read()
            except urllib.error.HTTPError as error:
                status = error.code
                payload = error.read()
            except Exception as error:  # upstream unreachable
                payload = json.dumps(
                    {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": f"proxy: {error}"}}
                ).encode()
            elapsed_ms = round((time.time() - started) * 1000, 1)
            record_calls(raw, payload, status, elapsed_ms, args.log)
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args) -> None:
            return

    server = ThreadingHTTPServer((args.listen, args.port), Handler)
    print(f"rpc proxy listening on {args.listen}:{args.port} -> {args.upstream}", flush=True)
    server.serve_forever()


def record_calls(raw: bytes, payload: bytes, status: int, elapsed_ms: float, log_path: str) -> None:
    try:
        calls = json.loads(raw)
    except ValueError:
        calls = {"method": "<invalid-json>", "params": None}
    try:
        results = json.loads(payload)
    except ValueError:
        results = None
    calls = calls if isinstance(calls, list) else [calls]
    results_by_id = {}
    if isinstance(results, list):
        results_by_id = {item.get("id"): item for item in results if isinstance(item, dict)}
    elif isinstance(results, dict):
        results_by_id = {results.get("id"): results}
    now = time.time()
    with LOCK, open(log_path, "a", encoding="utf-8") as log:
        HTTP_REQUESTS[0] += 1
        for call in calls:
            result = results_by_id.get(call.get("id"), {})
            error = result.get("error") if isinstance(result, dict) else None
            log.write(
                json.dumps(
                    {
                        "ts": round(now, 3),
                        "http_request": HTTP_REQUESTS[0],
                        "method": call.get("method"),
                        "params": call.get("params"),
                        "http_status": status,
                        "error": error,
                        "batch_size": len(calls),
                        "elapsed_ms": elapsed_ms,
                    }
                )
                + "\n"
            )


if __name__ == "__main__":
    main()
