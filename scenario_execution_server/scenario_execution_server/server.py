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
scenario-execution-server — passive ZMQ REP server that executes action
plugins on behalf of a remote scenario-execution client.

Architecture
------------
* One ZMQ REP socket bound on tcp://*:<port>.
* Fully synchronous REQ/REP cycle: the server waits for a request,
  processes it, sends a response, then waits for the next request.
* All remote-modifier instances on the client each have their own REQ
  socket (one connection per remote action), but they all talk to the
  same REP socket sequentially (ZMQ serialises them at the socket level).

Supported commands
------------------
  init      { action_id, plugin_key, init_args }
  setup     { action_id, tick_period, output_dir }
  execute   { action_id, exec_args }
  update    { action_id }  → returns action_status string
  terminate { action_id, new_status }
  reset     { action_id }  — terminates+shutdowns a single action
  reset_all {}             — terminates+shutdowns all actions
"""

import argparse
import logging
import signal
import sys
import time

import zmq

from scenario_execution_remote import protocol
from scenario_execution_server.action_runner import ActionRunner


def _setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stdout)


class ScenarioExecutionServer:
    """
    Passive ZMQ REP server.  The client calls init/setup/execute/update/
    terminate as needed; this server only responds.
    """

    def __init__(self, port: int = 7613, socket_path: str = None, watchdog: int = 30, connect_timeout: int = 15):
        self.port = port
        self.socket_path = socket_path
        self._runner = ActionRunner()
        self._context: zmq.Context = None
        self._socket: zmq.Socket = None
        self._running = False
        self._watchdog = watchdog
        self._connect_timeout = connect_timeout  # seconds until first message; 0 = disabled
        self._last_msg_time: float = None
        self._start_time: float = None
        self._log = logging.getLogger(__name__)

    def start(self):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        if self.socket_path:
            bind_addr = f"ipc://{self.socket_path}"
        else:
            bind_addr = f"tcp://*:{self.port}"
        self._socket.bind(bind_addr)
        self._log.info(f"scenario-execution-server listening on {bind_addr}")

        self._start_time = time.monotonic()
        self._running = True
        try:
            self._loop()
        except KeyboardInterrupt:
            self._log.info("Interrupted, shutting down…")
        finally:
            self._cleanup()

    def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _loop(self):
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while self._running:
            try:
                events = dict(poller.poll(500))  # 500 ms timeout to check _running
            except zmq.ZMQError as exc:
                if self._running:
                    self._log.error(f"ZMQ poll error: {exc}")
                break

            if self._socket not in events:
                now = time.monotonic()
                # No first contact yet — check connect timeout
                if (
                    self._connect_timeout > 0
                    and self._last_msg_time is None
                    and now - self._start_time > self._connect_timeout
                ):
                    self._log.info(
                        f"Connect timeout: no client connected within {self._connect_timeout}s, stopping."
                    )
                    self._running = False
                # Already connected — check inactivity watchdog
                elif (
                    self._watchdog > 0
                    and self._last_msg_time is not None
                    and now - self._last_msg_time > self._watchdog
                ):
                    self._log.info(
                        f"Watchdog: no message for {self._watchdog}s, stopping."
                    )
                    self._running = False
                continue

            self._last_msg_time = time.monotonic()
            data = self._socket.recv()
            try:
                msg = protocol.decode(data)
                response = self._dispatch(msg)
            except Exception as exc:  # noqa: BLE001
                self._log.exception("Error handling request")
                response = protocol.encode_response("error", message=str(exc))

            self._socket.send(response)

    def _dispatch(self, msg: dict) -> bytes:
        cmd = msg.get("cmd", "")
        payload = msg.get("payload", {})
        if cmd not in  ("heartbeat", "update"):
            self._log.info(f"cmd={cmd} action_id={str(payload.get('action_id', ''))[:8]}")  # always visible
        else:
            self._log.debug(f"cmd={cmd} action_id={str(payload.get('action_id', ''))[:8]}")  # only debug

        if cmd == "init":
            self._runner.init_action(
                action_id=payload["action_id"],
                plugin_key=payload["plugin_key"],
                init_args=payload.get("init_args") or {},
                output_dir=payload.get("output_dir", ""),
            )
            return protocol.encode_response("ok")

        elif cmd == "setup":
            self._runner.setup_action(
                action_id=payload["action_id"],
                tick_period=payload.get("tick_period"),
                output_dir=payload.get("output_dir", ""),
                scenario_file_directory=payload.get("scenario_file_directory", ""),
            )
            return protocol.encode_response("ok")

        elif cmd == "execute":
            self._runner.execute_action(
                action_id=payload["action_id"],
                exec_args=payload.get("exec_args") or {},
            )
            return protocol.encode_response("ok")

        elif cmd == "update":
            action_status = self._runner.update_action(payload["action_id"])
            return protocol.encode_response("ok", action_status=action_status)

        elif cmd == "terminate":
            self._runner.terminate_action(
                action_id=payload["action_id"],
                new_status_str=payload.get("new_status", "invalid"),
            )
            return protocol.encode_response("ok")

        elif cmd == "reset":
            action_id = payload.get("action_id")
            if action_id:
                self._runner.reset_action(action_id)
            else:
                self._runner.reset_all()
            return protocol.encode_response("ok")

        elif cmd == "heartbeat":
            # Just refresh the watchdog timer (already done by _loop); reply ok.
            return protocol.encode_response("ok")

        elif cmd == "quit":
            self._log.info("Client requested quit, stopping server.")
            self._running = False
            return protocol.encode_response("ok")

        else:
            return protocol.encode_response("error", message=f"Unknown command: '{cmd}'")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def _cleanup(self):
        self._log.info("Cleaning up…")
        try:
            self._runner.reset_all()
        except Exception as exc:  # noqa: BLE001
            self._log.warning(f"Error during runner cleanup: {exc}")
        if self._socket is not None:
            self._socket.close()
        if self._context is not None:
            self._context.term()
        self._log.info("Server stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="scenario-execution-server — remote action execution server"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=7613,
        help="TCP port to listen on (default: 7613); ignored when --socket is used",
    )
    parser.add_argument(
        "--socket", "-s",
        default=None,
        metavar="PATH",
        help="Unix domain socket path (e.g. /tmp/se.sock); takes precedence over --port",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--watchdog", "-w",
        type=int,
        default=10,
        metavar="SECONDS",
        help="Exit if no message received for this many seconds after first contact (0=disabled, default: 10)",
    )
    parser.add_argument(
        "--connect-timeout", "-c",
        type=int,
        default=15,
        metavar="SECONDS",
        help="Exit if no client connects within this many seconds of startup (0=disabled, default: 15)",
    )
    args = parser.parse_args()

    _setup_logging(args.verbose)

    server = ScenarioExecutionServer(port=args.port, socket_path=args.socket, watchdog=args.watchdog, connect_timeout=args.connect_timeout)

    def _sigterm_handler(_sig, _frame):
        logging.getLogger(__name__).info("SIGTERM received, stopping…")
        server.stop()

    # SIGTERM: graceful stop; SIGINT: let Python raise KeyboardInterrupt naturally
    signal.signal(signal.SIGTERM, _sigterm_handler)

    server.start()


if __name__ == "__main__":
    main()
