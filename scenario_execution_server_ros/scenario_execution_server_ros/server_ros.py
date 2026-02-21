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
scenario-execution-server-ros — ROS2 wrapper around ScenarioExecutionServer.

Differences from the plain server
----------------------------------
* Parameters are declared as ROS2 node parameters (overridable via
  ros args, launch files, or parameter files) in addition to CLI args.
* All logging goes through rclpy, so output is visible in ros2 log,
  rosbag2, and tools such as rqt_console.
* A ROS2 executor spins in a background thread so the node stays
  responsive (parameter services, etc.) while the ZMQ REP loop runs
  in the main thread.
* The server exits cleanly on rclpy shutdown (Ctrl-C or ros2 lifecycle).
"""

import logging
import sys
import threading

import rclpy
import rclpy.executors
import rclpy.logging

from scenario_execution_server.server import ScenarioExecutionServer


# ---------------------------------------------------------------------------
# Bridge: Python stdlib logging → rclpy
# ---------------------------------------------------------------------------

class _RclpyHandler(logging.Handler):
    """Forward every stdlib log record to the rclpy logger of the same name."""

    _LEVEL_MAP = {
        logging.DEBUG: rclpy.logging.LoggingSeverity.DEBUG,
        logging.INFO: rclpy.logging.LoggingSeverity.INFO,
        logging.WARNING: rclpy.logging.LoggingSeverity.WARN,
        logging.ERROR: rclpy.logging.LoggingSeverity.ERROR,
        logging.CRITICAL: rclpy.logging.LoggingSeverity.FATAL,
    }

    def emit(self, record: logging.LogRecord):
        try:
            ros_logger = rclpy.logging.get_logger(record.name)
            severity = self._LEVEL_MAP.get(record.levelno, rclpy.logging.LoggingSeverity.INFO)
            ros_logger.log(self.format(record), severity)
        except Exception:  # noqa: BLE001
            self.handleError(record)


def _install_rclpy_logging(verbose: bool):
    """Replace the root stdlib logging handler with the rclpy bridge."""
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(_RclpyHandler())
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


# ---------------------------------------------------------------------------
# ROS-aware server subclass
# ---------------------------------------------------------------------------

class ScenarioExecutionServerROS(ScenarioExecutionServer):
    """
    ScenarioExecutionServer with ROS2 integration.

    Parameter resolution order (later wins):
      1. compiled defaults
      2. CLI args
      3. ROS2 node parameters (set via launch / ros2 param / parameter file)
    """

    _NODE_NAME = "scenario_execution_server"

    def __init__(self):
        rclpy.init(args=sys.argv)
        self._node = rclpy.create_node(self._NODE_NAME)

        # ---- declare ROS params with defaults ----
        self._node.declare_parameter("port", 7613)
        self._node.declare_parameter("socket_path", "")
        self._node.declare_parameter("watchdog", 30)
        self._node.declare_parameter("verbose", False)

        # ---- read CLI (non-ROS) args ----
        import argparse
        args_without_ros = rclpy.utilities.remove_ros_args(sys.argv[1:])
        parser = argparse.ArgumentParser(
            description="scenario-execution-server (ROS2)"
        )
        parser.add_argument("--port", "-p", type=int, default=None,
                            help="TCP port (default: 7613)")
        parser.add_argument("--socket", "-s", default=None, metavar="PATH",
                            help="Unix domain socket path; takes precedence over --port")
        parser.add_argument("--watchdog", "-w", type=int, default=None, metavar="SECONDS",
                            help="Watchdog timeout in seconds (0=disabled, default: 30)")
        parser.add_argument("--verbose", "-v", action="store_true", default=False,
                            help="Enable debug logging")
        cli, _ = parser.parse_known_args(args_without_ros)

        # ---- merge: ROS params over CLI over built-in defaults ----
        def _ros_param(name, cli_val, default):
            p = self._node.get_parameter(name).value
            # rclpy returns the declared default when not set externally
            # prefer CLI if it was explicitly given, then ROS param
            if cli_val is not None:
                return cli_val
            return p if p != default else default

        port = _ros_param("port", cli.port, 7613)
        socket_path = _ros_param("socket_path", cli.socket, "") or None
        watchdog = _ros_param("watchdog", cli.watchdog, 30)
        verbose = self._node.get_parameter("verbose").value or cli.verbose

        _install_rclpy_logging(verbose)
        self._node.get_logger().info(
            f"scenario-execution-server-ros starting "
            f"(port={port}, socket_path={socket_path!r}, watchdog={watchdog}s)"
        )

        super().__init__(port=port, socket_path=socket_path, watchdog=watchdog)

    def start(self):
        """Start ROS executor in background, then run the ZMQ loop in main thread."""
        executor = rclpy.executors.SingleThreadedExecutor()
        executor.add_node(self._node)

        self._ros_thread = threading.Thread(
            target=self._spin_ros,
            args=(executor,),
            daemon=True,
            name="ros_spin",
        )
        self._ros_thread.start()

        try:
            super().start()
        finally:
            rclpy.shutdown()
            self._ros_thread.join(timeout=2.0)

    def _spin_ros(self, executor: rclpy.executors.Executor):
        """Background thread: keep the ROS node alive until shutdown."""
        try:
            while rclpy.ok():
                executor.spin_once(timeout_sec=0.2)
        except Exception:  # noqa: BLE001
            pass

    def stop(self):
        super().stop()
        rclpy.shutdown()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    server = ScenarioExecutionServerROS()

    import signal
    def _sigterm_handler(_sig, _frame):
        server.stop()

    signal.signal(signal.SIGTERM, _sigterm_handler)

    server.start()


if __name__ == "__main__":
    main()
