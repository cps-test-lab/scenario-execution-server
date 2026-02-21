# Copyright (C) 2026 Frederik Pasch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0

def get_osc_library():
    """Return the package and filename for the remote.osc library.

    model_builder resolves this as:
        importlib.resources.files('scenario_execution_remote')
            .joinpath('lib_osc') / 'remote.osc'
    """
    return 'scenario_execution_remote', 'remote.osc'
