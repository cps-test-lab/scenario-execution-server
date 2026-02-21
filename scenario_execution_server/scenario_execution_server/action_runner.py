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

Lifecycle mirrors the local scenario-execution lifecycle:
  init    → instantiate plugin with tree-build-time init_args
  setup   → call instance.setup()
  execute → call instance.execute() with runtime exec_args (from client blackboard)
  update  → tick instance
  terminate → notify instance
  reset   → shutdown instance, clear entry
"""

import logging
import py_trees
from importlib.metadata import entry_points

from scenario_execution.actions.base_action import BaseAction
from scenario_execution.actions.base_action_subtree import BaseActionSubtree

_LOG = logging.getLogger(__name__)


class ActionRunner:
    """Manages a set of named action plugin instances by action_id."""

    def __init__(self):
        self._actions: dict = {}  # action_id -> instance

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def init_action(self, action_id: str, plugin_key: str, init_args: dict):
        """
        Instantiate the action plugin with tree-build-time init_args.
        Mirrors the local client: instance is created exactly once.
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
        log = logging.getLogger(f"action.{action_id[:8]}")
        instance._set_base_properities(action_id, None, log)  # pylint: disable=protected-access

        self._actions[action_id] = instance
        _LOG.info(f"Initialized '{plugin_key}' (id={action_id[:8]})")

    def setup_action(self, action_id: str, tick_period: float, output_dir: str, scenario_file_directory: str = ""):
        """Call instance.setup() — mirrors the local tree walker."""
        if scenario_file_directory:
            from scenario_execution.scenario_execution_base import ScenarioExecutionConfig
            ScenarioExecutionConfig().scenario_file_directory = scenario_file_directory
        instance = self._get(action_id)
        instance.setup(
            logger=instance.logger,
            output_dir=output_dir or "",
            tick_period=tick_period or 0.1,
        )
        _LOG.info(f"Setup (id={action_id[:8]})")

    def execute_action(self, action_id: str, exec_args: dict):
        """
        Call instance.execute() with runtime exec_args resolved from the
        client blackboard.  Instantiation and setup are already done.
        """
        instance = self._get(action_id)
        if instance.execute_method is not None:
            instance.execute(**exec_args)
        instance.status = py_trees.common.Status.RUNNING
        _LOG.debug(f"Execute (id={action_id[:8]}, args={list(exec_args.keys())})") 

    def update_action(self, action_id: str) -> str:
        instance = self._get(action_id)
        status = instance.update()
        instance.status = status
        return status.value.lower()

    def terminate_action(self, action_id: str, new_status_str: str):
        instance = self._get(action_id)
        try:
            new_status = py_trees.common.Status(new_status_str)
        except ValueError:
            new_status = py_trees.common.Status.INVALID
        instance.terminate(new_status)
        instance.status = new_status

    def reset_action(self, action_id: str):
        """Terminate and shut down a single action, then remove it."""
        instance = self._actions.pop(action_id, None)
        if instance is None:
            return
        try:
            instance.terminate(py_trees.common.Status.INVALID)
            instance.shutdown()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(f"Error during reset of action {action_id[:8]}: {exc}")

    def reset_all(self):
        """Terminate and shut down all managed actions."""
        for action_id in list(self._actions.keys()):
            self.reset_action(action_id)
        _LOG.info("All actions reset.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, action_id: str) -> BaseAction:
        try:
            return self._actions[action_id]
        except KeyError:
            raise KeyError(f"Unknown action_id: {action_id}") from None
