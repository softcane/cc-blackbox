#!/usr/bin/env python3
"""Deterministic Anthropic Messages API stand-in for cc-blackbox E2E tests."""

from __future__ import annotations

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _request_text(body: dict) -> str:
    return json.dumps(body, sort_keys=True)


def _has_context_1m_beta(headers) -> bool:
    beta_values = headers.get_all("anthropic-beta", [])
    for value in beta_values:
        for beta in value.split(","):
            if beta.strip().lower().startswith("context-1m-"):
                return True
    return False


def _has_empty_beta(headers) -> bool:
    beta_values = headers.get_all("anthropic-beta", [])
    for value in beta_values:
        if value.strip() == "":
            return True
        for beta in value.split(","):
            if beta.strip() == "":
                return True
    return False


def _has_beta(headers, expected: str) -> bool:
    beta_values = headers.get_all("anthropic-beta", [])
    for value in beta_values:
        for beta in value.split(","):
            if beta.strip().lower() == expected.lower():
                return True
    return False


def _scenario(body: dict) -> tuple[str, list[tuple[str, dict]], str]:
    text = _request_text(body)
    if "pipeline-review" in text:
        return (
            "fixture-skills:pipeline-review",
            [
                ("Skill", {"skill_name": "fixture-skills:pipeline-review"}),
                ("Glob", {"pattern": ".github/workflows/*.yaml"}),
            ],
            "fake pipeline review complete",
        )

    return (
        "fixture-skills:config-review",
        [
            ("Skill", {"skill_name": "fixture-skills:config-review"}),
            ("Read", {"file_path": "fixtures/service/config.yaml"}),
        ],
        "fake config review complete",
    )


def _sse(payload: object) -> bytes:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_plain(200, "ok\n")
            return
        self._send_plain(404, "not found\n")

    def do_POST(self) -> None:
        if self.path != "/v1/messages":
            self._send_plain(404, "not found\n")
            return

        length = int(self.headers.get("content-length", "0") or "0")
        raw_body = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            body = {}

        request_text = _request_text(body)
        model = body.get("model") or "claude-sonnet-4-6-20250514"
        if self.headers.get("accept-encoding"):
            self._send_json(
                400,
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "accept-encoding leaked to upstream",
                    },
                },
            )
            return
        if _has_empty_beta(self.headers):
            self._send_json(
                400,
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "empty anthropic-beta leaked to upstream",
                    },
                },
            )
            return
        if "proxy-empty-beta" in request_text and not _has_beta(
            self.headers, "fine-grained-tool-streaming-2025-05-14"
        ):
            self._send_json(
                400,
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "valid anthropic-beta was not preserved",
                    },
                },
            )
            return
        if "[1m]" in str(model).lower():
            self._send_json(
                400,
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "model alias was not normalized before upstream",
                    },
                },
            )
            return
        if _has_context_1m_beta(self.headers):
            self._send_json(
                400,
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": "retired context-1m beta header leaked upstream",
                    },
                },
            )
            return

        skill_name, tools, summary = _scenario(body)
        if "proxy-delayed" in request_text:
            tools = [("Bash", {"command": "echo proxy-delayed-live"})] + tools
            summary = f"{summary} with delayed héllo chunk"

        if body.get("stream") is False:
            self._send_json(
                200,
                {
                    "id": f"msg_fake_{skill_name.replace(':', '_')}",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"toolu_fake_{index}",
                            "name": tool_name,
                            "input": tool_input,
                        }
                        for index, (tool_name, tool_input) in enumerate(tools)
                    ]
                    + [{"type": "text", "text": summary}],
                    "usage": {
                        "input_tokens": 1200,
                        "cache_creation_input_tokens": 320,
                        "cache_creation": {
                            "ephemeral_5m_input_tokens": 220,
                            "ephemeral_1h_input_tokens": 100,
                        },
                        "cache_read_input_tokens": 80,
                        "output_tokens": 90,
                    },
                },
            )
            return

        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "close")
        self.end_headers()

        chunks = [
            {
                "type": "message_start",
                "message": {
                    "id": f"msg_fake_{skill_name.replace(':', '_')}",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "usage": {
                        "input_tokens": 1200,
                        "cache_creation_input_tokens": 320,
                        "cache_creation": {
                            "ephemeral_5m_input_tokens": 220,
                            "ephemeral_1h_input_tokens": 100,
                        },
                        "cache_read_input_tokens": 80,
                        "output_tokens": 0,
                    },
                },
            }
        ]

        for index, (tool_name, tool_input) in enumerate(tools):
            chunks.append(
                {
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": f"toolu_fake_{index}",
                        "name": tool_name,
                        "input": {},
                    },
                }
            )
            chunks.append(
                {
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps(tool_input, separators=(",", ":")),
                    },
                }
            )
            chunks.append({"type": "content_block_stop", "index": index})

        text_index = len(tools)
        chunks.extend(
            [
                {
                    "type": "content_block_start",
                    "index": text_index,
                    "content_block": {"type": "text", "text": ""},
                },
                {
                    "type": "content_block_delta",
                    "index": text_index,
                    "delta": {"type": "text_delta", "text": summary},
                },
                {"type": "content_block_stop", "index": text_index},
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 90},
                },
                {"type": "message_stop"},
            ]
        )

        for chunk in chunks:
            encoded = _sse(chunk)
            if "proxy-delayed" in request_text:
                midpoint = max(1, len(encoded) // 2)
                self.wfile.write(encoded[:midpoint])
                self.wfile.flush()
                time.sleep(0.05)
                self.wfile.write(encoded[midpoint:])
            else:
                self.wfile.write(encoded)
            self.wfile.flush()
            if "proxy-delayed" in request_text:
                time.sleep(0.10)
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()
        self.close_connection = True

    def _send_plain(self, status: int, body: str) -> None:
        encoded = body.encode()
        self.send_response(status)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(encoded)
        self.close_connection = True

    def _send_json(self, status: int, payload: dict) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(encoded)
        self.close_connection = True

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("fake-anthropic: " + fmt % args + "\n")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    print("fake-anthropic listening on :8000", flush=True)
    server.serve_forever()
