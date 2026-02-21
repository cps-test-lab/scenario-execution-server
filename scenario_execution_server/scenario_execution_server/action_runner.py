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
ActionRunner — manages the lifecycle of action plugin instances on the server.

The server receives pre-resolved argument dicts from the client (the blackboard
lives on the client), so no OSC2 model access is needed here.
"""

import logging
import py_trees
from importlib.metadata import entry_points

from scenario_execution.actions.base_action import BaseAction
from scenario_execution.actions.base_action_subtree import BaseActionSubtree


class ActionRunner:
    """Manages a set of named action plugin instances by action_id."""

    def __init__(self):
        self._actions: dict = {}  # action_id -> instance

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def init_action(self, action_id: str, plugin_key: str, init_args: dict):
        """
        Instantiate an action plugin.

        Parameters
        ----------
        action_id:  unique ID supplied by the client (uuid)
        plugin_key: entry-point name in group 'scenario_execution.actions'
        init_args:  pre-resolved __init__ arguments (may be empty dict)
        """
        eps = [ep for ep in entry_points(group='scenario_execution.actions')
               if ep.name == plugin_key]
        if not eps:
            raise ValueError(f"No action plugin found for key '{plugin_key}'")

        behavior_cls = eps[0].load()
        if not issubclass(behavior_cls, (BaseAction, BaseActionSubtree)):
            raise TypeError(
                f"Plugin '{plugin_key}' is not a subclass of BaseAction or BaseActionSubtree"
            )

        instance = behavior_cls(**init_args)

        # Set base properties — _model stays None (no OSC2 model on server)
        log = logging.getLogger(f"action.{action_id[:8]}")
        instance._set_base_properities(action_id, None, log)  # pylint: disable=protected-access

        self._actions[action_id] = instance
        logging.getLogger(__name__).info(
            f"[server] Initialized action '{plugin_key}' (id={action_id[:8]})"
        )

    def setup_action(self, action_id: str, tick_period: float, output_dir: str):
        """Call action.setup() on the server side."""
        instance = self._get(action_id)
        log = logging.getLogger(f"action.{action_id[:8]}")
        instance.setup(
            logger=log,
            output_dir=output_dir or "",
            tick_period=tick_period or 0.1,
        )
        logging.getLogger(__name__).info(
            f"[server] Setup action (id={action_id[:8]})"
        )

    def execute_action(self, action_id: str, exec_args: dict):
        """
        Start the action.  exec_args are already resolved by the client.
        Calls action.execute(**exec_args) if the action defines execute(),
        then marks the action as RUNNING.
        """
        instance = self._get(action_id)
        if instance.execute_method is not None:
            instance.execute(**exec_args)
        # Mark as RUNNING so that update() state transitions are correct
        instance.status = py_trees.common.Status.RUNNING
        logging.getLogger(__name__).debug(
            f"[server] Execute action (id={action_id[:8]}, args={list(exec_args.keys())})"
        )

    def update_action(self, action_id: str) -> str:
        """
        Tick the action once and return its status string.

        Returns one of: "running", "success", "failure"
        """
        instance = self._get(action_id)
        status = instance.update()
        instance.status = status
        return status.value.lower()  # protocol uses lowercase

    def terminate_action(self, action_id: str, new_status_str: str):
        """Notify the action that it is no longer being ticked."""
        instance = self._get(action_id)
        try:
            new_status = py_trees.common.Status(new_status_str)
        except ValueError:
            new_status = py_trees.common.Status.INVALID
        instance.terminate(new_status)
        instance.status = new_status

    def reset_action(self, action_id: str):
        """Terminate and shut down a single action, then remove it."""
        if action_id in self._actions:
            instance = self._actions[action_id]
            try:
                instance.terminate(py_trees.common.Status.INVALID)
                instance.shutdown()
            except Exception as exc:  # noqa: BLE001
                logging.getLogger(__name__).warning(
                    f"[server] Error during reset of action {action_id[:8]}: {exc}"
                )
            del self._actions[action_id]

    def reset_all(self):
        """Terminate and shut down all managed actions."""
        for action_id in list(self._actions.keys()):
            self.reset_action(action_id)
        logging.getLogger(__name__).info("[server] All actions reset.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, action_id: str) -> BaseAction:
        try:
            return self._actions[action_id]
        except KeyError:
            raise KeyError(f"Unknown action_id: {action_id}") from None
