# Copyright (C) 2026 Frederik Pasch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0

"""
Minimal msgpack-based wire protocol for scenario-execution-remote/server.

Message format (msgpack-encoded dict):
  { "cmd": "<command>", "payload": { ... } }

Response format:
  { "status": "ok" | "error", ... }   # error includes "message"

Commands (client → server):
  init      payload: { action_id, plugin_key, init_args }
  setup     payload: { action_id, tick_period, output_dir }
  execute   payload: { action_id, exec_args }
  update    payload: { action_id }
  terminate payload: { action_id, new_status }
  reset     payload: {}
  heartbeat payload: {}   — sent every 1 s; resets server watchdog; client
                            checks ack within heartbeat_failure_timeout seconds
"""

import msgpack


def encode(cmd: str, payload: dict) -> bytes:
    """Encode a command message to bytes."""
    return msgpack.packb({"cmd": cmd, "payload": payload}, use_bin_type=True)


def decode(data: bytes) -> dict:
    """Decode a received message from bytes."""
    return msgpack.unpackb(data, raw=False)


def encode_response(status: str, **kwargs) -> bytes:
    """Encode a response to bytes."""
    msg = {"status": status}
    msg.update(kwargs)
    return msgpack.packb(msg, use_bin_type=True)


def decode_response(data: bytes) -> dict:
    """Decode a response from bytes."""
    return msgpack.unpackb(data, raw=False)
