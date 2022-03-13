from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import hooks

# Defining the plugin class
class DutraPlugin(AirflowPlugin):
    name = "dutra_plugin"
    operators = [
        operators.DataQualityOperator,
    ]
    hooks = [hooks.S3fsHook]
