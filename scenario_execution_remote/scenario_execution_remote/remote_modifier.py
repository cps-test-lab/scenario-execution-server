# Copyright (C) 2025 Frederik Pasch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0

"""
RemoteModifier — py_trees Behaviour that forwards action lifecycle to a
scenario-execution-server instance via ZMQ REQ/REP + msgpack.

Design notes
------------
* Extends py_trees.behaviour.Behaviour as a plain leaf node.
  The child action is NEVER ticked locally; only the RemoteModifier lifecycle
  methods run during a tick cycle.

* The child action is stored as self.decorated purely for metadata:
    - child._external_plugin_key  : which action plugin the server must load
    - child._external_init_args   : init-time args (already resolved)
    - child._model                : OSC2 model for live blackboard resolution
    - child.execute_skip_args     : args consumed by __init__ (skip in execute)

* Each RemoteModifier instance owns its own zmq.Context + REQ socket so
  that parallel remote actions do not share state or contend on a socket.

* The server is fully passive (REP). One REQ/REP cycle per command.
"""

import threading
import time
import uuid
import copy
import py_trees
import zmq

from scenario_execution_remote import protocol


class RemoteModifier(py_trees.behaviour.Behaviour):
    """
    Replaces a local action with remote execution on a scenario-execution-server.

    The *child* action node is used only for metadata — it is never ticked.
    """

    # per-command timeout during execution (generous, actions may take time)
    _EXEC_TIMEOUT_MS = 30_000
    # heartbeat is sent every second from the client to the server
    _HEARTBEAT_INTERVAL_S = 1.0
    # how long to wait for the server's heartbeat ack before considering it missing
    _HEARTBEAT_ACK_TIMEOUT_MS = 2_000

    def __init__(self, child: py_trees.behaviour.Behaviour, endpoint: str,
                 heartbeat_failure_timeout: float = 5.0, setup_timeout: float = 5.0):
        self._zmq_endpoint = _make_zmq_endpoint(endpoint)
        self._setup_timeout = setup_timeout
        super().__init__(name=f"remote({self._zmq_endpoint})")
        # keep child for metadata only — NOT in self.children
        self.decorated = child
        self.logger = copy.copy(child.logger)
        self.logger.name = f"remote({self._zmq_endpoint})"
        self._action_id = str(uuid.uuid4())
        # If no successful heartbeat ack is received for this many seconds,
        # update() returns FAILURE (server presumed dead).
        self._heartbeat_failure_timeout = heartbeat_failure_timeout
        self._last_heartbeat_ok_time: float = None
        self._server_dead: bool = False
        self._context: zmq.Context = None
        self._socket: zmq.Socket = None
        self._socket_lock = threading.Lock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread = None

    # ------------------------------------------------------------------
    # py_trees setup — called once by the tree walker
    # ------------------------------------------------------------------

    def setup(self, **kwargs):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)          # never block on close/term
        # Allow sending a new request even if a previous recv() timed out;
        # this keeps the heartbeat loop working after a transient ack timeout.
        self._socket.setsockopt(zmq.REQ_RELAXED, 1)
        # Fail fast if the server is unreachable during setup
        self._socket.setsockopt(zmq.SNDTIMEO, int(self._setup_timeout * 1000))
        self._socket.setsockopt(zmq.RCVTIMEO, int(self._setup_timeout * 1000))

        self.logger.info(
            f"Connecting to scenario-execution-server at "
            f"{self._zmq_endpoint} "
            f"(plugin='{self.decorated._external_plugin_key}', "
            f"action_id={self._action_id[:8]}, "
            f"timeout={self._setup_timeout}s) ..."
        )
        self._socket.connect(self._zmq_endpoint)

        try:
            # Tell server to instantiate the action plugin
            resp = self._send("init", {
                "action_id": self._action_id,
                "plugin_key": self.decorated._external_plugin_key,
                "init_args": self.decorated._external_init_args,
                "output_dir": kwargs.get("output_dir", ""),
            })
            self._check_response(resp, "init")

            # Tell server to call action.setup()
            from scenario_execution.scenario_execution_base import ScenarioExecutionConfig
            resp = self._send("setup", {
                "action_id": self._action_id,
                "tick_period": kwargs.get("tick_period"),
                "output_dir": kwargs.get("output_dir", ""),
                "scenario_file_directory": ScenarioExecutionConfig().scenario_file_directory or "",
            })
            self._check_response(resp, "setup")
        except zmq.Again:
            self._socket.close()
            self._context.term()
            self._socket = None
            self._context = None
            raise RuntimeError(
                f"Cannot reach scenario-execution-server at "
                f"{self._zmq_endpoint} "
                f"(timeout {self._setup_timeout}s). Is the server running?"
            )

        # Switch to a longer timeout for the actual execution commands
        self._socket.setsockopt(zmq.SNDTIMEO, self._EXEC_TIMEOUT_MS)
        self._socket.setsockopt(zmq.RCVTIMEO, self._EXEC_TIMEOUT_MS)
        self.logger.info(
            f"Connected to scenario-execution-server at {self._zmq_endpoint} OK"
        )

        # Initialise heartbeat timestamp so update() doesn't fail before the
        # first heartbeat has been sent.
        self._last_heartbeat_ok_time = time.monotonic()
        # Start background heartbeat thread (1 Hz) to keep the server watchdog alive.
        # The heartbeat also serves as the liveness probe: if the server stops
        # acknowledging, update() will return FAILURE after heartbeat_failure_timeout.
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="remote-heartbeat"
        )
        self._heartbeat_thread.start()

    # ------------------------------------------------------------------
    # py_trees tick lifecycle
    # ------------------------------------------------------------------

    def initialise(self):
        """Called on each non-RUNNING → RUNNING transition. Resolves exec args
        from the child's blackboard state and forwards them to the server."""
        exec_args = self._resolve_exec_args()
        resp = self._send("execute", {
            "action_id": self._action_id,
            "exec_args": exec_args,
        })
        self._check_response(resp, "execute")

    def update(self) -> py_trees.common.Status:
        """Called every tick while RUNNING. Poll the server for action status."""
        # Heartbeat liveness check: if the server has not acked a heartbeat
        # recently, treat it as gone and return FAILURE immediately.
        if (self._last_heartbeat_ok_time is not None
                and time.monotonic() - self._last_heartbeat_ok_time
                > self._heartbeat_failure_timeout):
            self.logger.error(
                f"No heartbeat ack from server for >"
                f"{self._heartbeat_failure_timeout:.0f}s — server presumed dead, "
                f"returning FAILURE"
            )
            self._server_dead = True
            return py_trees.common.Status.FAILURE
        try:
            # Use heartbeat_failure_timeout as the recv timeout for the update
            # command. The server should respond immediately (it just polls the
            # action status), so waiting the full _EXEC_TIMEOUT_MS (30 s) would
            # delay failure detection by nearly 30 s when the server disappears.
            update_timeout_ms = int(self._heartbeat_failure_timeout * 1000)
            resp = self._send("update", {"action_id": self._action_id},
                              rcvtimeo=update_timeout_ms)
        except zmq.Again:
            self.logger.error(f"update timed out (server unreachable?) — returning FAILURE")
            self._server_dead = True
            return py_trees.common.Status.FAILURE
        if resp.get("status") == "error":
            self.logger.error(f"update error: {resp.get('message')}")
            return py_trees.common.Status.FAILURE
        status_str = resp.get("action_status", "failure")
        return {
            "running": py_trees.common.Status.RUNNING,
            "success": py_trees.common.Status.SUCCESS,
            "failure": py_trees.common.Status.FAILURE,
        }.get(status_str, py_trees.common.Status.FAILURE)

    def terminate(self, new_status: py_trees.common.Status):
        """Called when the behaviour stops being ticked."""
        if self._socket is not None and not self._server_dead:
            try:
                self._send("terminate", {
                    "action_id": self._action_id,
                    "new_status": new_status.value,
                })
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"terminate failed: {exc}")

    def shutdown(self):
        """Called on scenario shutdown. Clean up server state and socket."""
        # Stop heartbeat thread first so it no longer touches the socket
        if self._heartbeat_thread is not None:
            self._heartbeat_stop.set()
            self._heartbeat_thread.join(timeout=2.0)
            self._heartbeat_thread = None

        if self._socket is not None:
            if not self._server_dead:
                try:
                    self._send("reset", {"action_id": self._action_id})
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning(f"shutdown failed: {exc}")
            self._socket.close()
            self._context.term()
            self._socket = None
            self._context = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_exec_args(self) -> dict:
        """Resolve execute() args from the child's current blackboard state."""
        child = self.decorated
        if child.execute_method is None or child._model is None:
            return {}

        blackboard = child.get_blackboard_client()
        if child.resolve_variable_reference_arguments_in_execute:
            args = child._model.get_resolved_value(blackboard, skip_keys=child.execute_skip_args)
        else:
            args = child._model.get_resolved_value_with_variable_references(
                blackboard, skip_keys=child.execute_skip_args)

        if child._model.actor:
            args["associated_actor"] = child._model.actor.get_resolved_value(blackboard)
            args["associated_actor"]["name"] = child._model.actor.name

        return args

    def _heartbeat_loop(self):
        """Background thread: sends a heartbeat every second while connected.

        Uses a short per-heartbeat receive timeout (_HEARTBEAT_ACK_TIMEOUT_MS)
        so that a dead server is detected within that window rather than waiting
        the full _EXEC_TIMEOUT_MS (30 s).  The socket is opened with REQ_RELAXED,
        which allows the next send() to proceed even after a recv() timeout.

        _last_heartbeat_ok_time is updated only when the server acknowledges.
        update() monitors this timestamp and returns FAILURE when it goes stale.
        """
        while not self._heartbeat_stop.wait(timeout=self._HEARTBEAT_INTERVAL_S):
            if self._socket is None:
                break
            try:
                with self._socket_lock:
                    if self._socket is None:
                        break
                    # Temporarily lower the recv timeout so we detect a missing
                    # server quickly instead of waiting _EXEC_TIMEOUT_MS.
                    self._socket.setsockopt(zmq.RCVTIMEO, self._HEARTBEAT_ACK_TIMEOUT_MS)
                    try:
                        self._socket.send(protocol.encode("heartbeat", {}))
                        self._socket.recv()  # consume the 'ok' reply
                        self._last_heartbeat_ok_time = time.monotonic()
                    except zmq.Again:
                        # Server did not ack within HEARTBEAT_ACK_TIMEOUT_MS.
                        # REQ_RELAXED lets us retry on the next iteration.
                        self.logger.warning(
                            f"Heartbeat ack timeout ({self._HEARTBEAT_ACK_TIMEOUT_MS} ms) "
                            f"— server may be gone"
                        )
                    finally:
                        # Restore the normal execution timeout.
                        self._socket.setsockopt(zmq.RCVTIMEO, self._EXEC_TIMEOUT_MS)
            except Exception:  # noqa: BLE001  — socket closing during shutdown is normal
                break

    def _send(self, cmd: str, payload: dict, rcvtimeo: int = None) -> dict:
        with self._socket_lock:
            if rcvtimeo is not None:
                self._socket.setsockopt(zmq.RCVTIMEO, rcvtimeo)
            try:
                self._socket.send(protocol.encode(cmd, payload))
                return protocol.decode_response(self._socket.recv())
            finally:
                if rcvtimeo is not None:
                    self._socket.setsockopt(zmq.RCVTIMEO, self._EXEC_TIMEOUT_MS)

    @staticmethod
    def _check_response(resp: dict, cmd: str):
        if resp.get("status") != "ok":
            raise RuntimeError(
                f"Remote command '{cmd}' failed: {resp.get('message', 'unknown error')}"
            )


def _make_zmq_endpoint(endpoint: str) -> str:
    """Convert a user-supplied endpoint string to a ZMQ address.

    Rules:
      /path/...  or  ./path/...  →  ipc:///path/...  (Unix domain socket)
      host                       →  tcp://host:7613
      host:port                  →  tcp://host:port
    """
    if endpoint.startswith('ipc://') or endpoint.startswith('tcp://'):
        return endpoint  # already a full ZMQ endpoint
    if endpoint.startswith('/') or endpoint.startswith('./'):
        return f"ipc://{endpoint}"
    if ':' in endpoint:
        return f"tcp://{endpoint}"
    return f"tcp://{endpoint}:7613"


def create_remote_modifier(child: py_trees.behaviour.Behaviour, args: dict) -> RemoteModifier:
    """
    Entry-point factory called by model_to_py_tree when the 'remote'
    modifier plugin is resolved.

    args keys match the OSC2 modifier declaration:
      endpoint: string               — hostname/IP[:port] or /path/to/unix-socket
      heartbeat_failure_timeout: float — seconds without a heartbeat ack before
                                         update() returns FAILURE (default: 3.0)
      setup_timeout: float          — timeout in seconds for connect + init + setup (default: 10.0)
    """
    return RemoteModifier(
        child=child,
        endpoint=args.get("endpoint", "127.0.0.1"),
        heartbeat_failure_timeout=float(args.get("heartbeat_failure_timeout", 3.0)),
        setup_timeout=float(args.get("setup_timeout", 10.0)),
    )
