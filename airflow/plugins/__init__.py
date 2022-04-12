from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import hooks
import helpers

# Defining the plugin class
class DutraPlugin(AirflowPlugin):
    name = "dutra_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.ShapefileToParquetOperator,
        operators.SparkOnK8sAppOperator,
        operators.LoadToS3Operator,
        operators.FirmsOperator,
    ]
    hooks = [hooks.S3fsHook, hooks.KubernetesHook]
    helpers = [helpers.EmrTemplates]
