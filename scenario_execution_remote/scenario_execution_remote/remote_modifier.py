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
* Extends py_trees.behaviour.Behaviour (NOT Decorator).
  py_trees.decorators.Decorator.tick() always ticks the child locally AND
  then calls self.update() — that would cause dual local+remote execution.
  As a plain Behaviour, we are a leaf node: only our own lifecycle methods
  run during a tick cycle; the child action is NEVER put in self.children.

* The child action is stored as self.decorated purely for metadata:
    - child._external_plugin_key  : which action plugin the server must load
    - child._external_init_args   : init-time args (already resolved)
    - child._model              : OSC2 model for live blackboard resolution
    - child.execute_skip_args   : args consumed by __init__ (skip in execute)

* Each RemoteModifier instance owns its own zmq.Context + REQ socket so
  that parallel remote actions do not share state or contend on a socket.

* The server is fully passive (REP). One REQ/REP cycle per command.
"""

import uuid
import py_trees
import zmq

from scenario_execution_remote import protocol


class RemoteModifier(py_trees.behaviour.Behaviour):
    """
    Replaces a local action with remote execution on a scenario-execution-server.

    The *child* action node is used only for metadata — it is never ticked.
    """

    # connection timeout for setup (fail fast if server not reachable)
    _SETUP_TIMEOUT_MS = 5_000
    # per-command timeout during execution (generous, actions may take time)
    _EXEC_TIMEOUT_MS = 30_000

    def __init__(self, child: py_trees.behaviour.Behaviour, endpoint: str):
        self._zmq_endpoint = _make_zmq_endpoint(endpoint)
        super().__init__(name=f"remote({self._zmq_endpoint})")
        # keep child for metadata only — NOT in self.children
        self.decorated = child
        self._action_id = str(uuid.uuid4())
        self._context: zmq.Context = None
        self._socket: zmq.Socket = None

    # ------------------------------------------------------------------
    # py_trees setup — called once by the tree walker
    # ------------------------------------------------------------------

    def setup(self, **kwargs):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)          # never block on close/term
        # Fail fast if the server is unreachable during setup
        self._socket.setsockopt(zmq.SNDTIMEO, self._SETUP_TIMEOUT_MS)
        self._socket.setsockopt(zmq.RCVTIMEO, self._SETUP_TIMEOUT_MS)

        self.logger.info(
            f"Connecting to scenario-execution-server at "
            f"{self._zmq_endpoint} "
            f"(plugin='{self.decorated._external_plugin_key}', "
            f"action_id={self._action_id[:8]}, "
            f"timeout={self._SETUP_TIMEOUT_MS}ms) ..."
        )
        self._socket.connect(self._zmq_endpoint)

        try:
            # Tell server to instantiate the action plugin
            resp = self._send("init", {
                "action_id": self._action_id,
                "plugin_key": self.decorated._external_plugin_key,
                "init_args": self.decorated._external_init_args,
            })
            self._check_response(resp, "init")

            # Tell server to call action.setup()
            resp = self._send("setup", {
                "action_id": self._action_id,
                "tick_period": kwargs.get("tick_period"),
                "output_dir": kwargs.get("output_dir", ""),
            })
            self._check_response(resp, "setup")
        except zmq.Again:
            self._socket.setsockopt(zmq.LINGER, 0)  # discard pending messages immediately
            self._socket.close()
            self._context.term()
            self._socket = None
            self._context = None
            raise RuntimeError(
                f"Cannot reach scenario-execution-server at "
                f"{self._zmq_endpoint} "
                f"(timeout {self._SETUP_TIMEOUT_MS}ms). Is the server running?"
            )

        # Switch to a longer timeout for the actual execution commands
        self._socket.setsockopt(zmq.SNDTIMEO, self._EXEC_TIMEOUT_MS)
        self._socket.setsockopt(zmq.RCVTIMEO, self._EXEC_TIMEOUT_MS)
        self.logger.info(
            f"Connected to scenario-execution-server at {self._zmq_endpoint} OK"
        )

    # ------------------------------------------------------------------
    # py_trees tick lifecycle
    # ------------------------------------------------------------------

    def initialise(self):
        """
        Called by py_trees on each non-RUNNING → RUNNING transition.
        Resolves exec_args from the current blackboard state and sends
        them to the server. init/setup are already done.
        """
        exec_args = self._resolve_exec_args()
        resp = self._send("execute", {
            "action_id": self._action_id,
            "exec_args": exec_args,
        })
        self._check_response(resp, "execute")

    def update(self) -> py_trees.common.Status:
        """Called every tick while RUNNING. Poll the server for action status."""
        try:
            resp = self._send("update", {"action_id": self._action_id})
        except zmq.Again:
            self.logger.error(f"update timed out (server unreachable?) — returning FAILURE")
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
        if self._socket is not None:
            try:
                self._send("terminate", {
                    "action_id": self._action_id,
                    "new_status": new_status.value,
                })
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"terminate failed: {exc}")

    def shutdown(self):
        """Called on scenario shutdown. Clean up server state and socket."""
        if self._socket is not None:
            try:
                self._send("reset", {"action_id": self._action_id})
                self._send("quit", {})  # ask server to exit; ignored if already gone
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"shutdown failed: {exc}")
            finally:
                self._socket.close()
                self._context.term()
                self._socket = None
                self._context = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_exec_args(self) -> dict:
        """
        Resolve execute() args from the current blackboard state.
        Called at initialise() time when all values are guaranteed ready.
        """
        child = self.decorated
        if child.execute_method is None or child._model is None:
            return {}

        if child.resolve_variable_reference_arguments_in_execute:
            args = child._model.get_resolved_value(
                child.get_blackboard_client(),
                skip_keys=child.execute_skip_args,
            )
        else:
            args = child._model.get_resolved_value_with_variable_references(
                child.get_blackboard_client(),
                skip_keys=child.execute_skip_args,
            )

        if child._model.actor:
            args["associated_actor"] = child._model.actor.get_resolved_value(
                child.get_blackboard_client()
            )
            args["associated_actor"]["name"] = child._model.actor.name

        return args

    def _send(self, cmd: str, payload: dict) -> dict:
        self._socket.send(protocol.encode(cmd, payload))
        return protocol.decode_response(self._socket.recv())

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
      endpoint: string  — hostname/IP[:port] or /path/to/unix-socket
    """
    return RemoteModifier(child=child, endpoint=args.get("endpoint", "127.0.0.1"))
