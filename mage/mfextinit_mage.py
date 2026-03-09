"""Metaflow extension registration for mage.

Metaflow discovers this file automatically via the ``metaflow_extensions``
namespace package mechanism (it scans for ``mfextinit_*.py`` files in any
installed package under the ``metaflow_extensions`` namespace).

Place this file at:
    metaflow_extensions/mage/plugins/mfextinit_mage.py

After ``pip install -e .`` the following becomes available:
  - ``python flow.py mage create ...`` (CLI)
  - ``Deployer(flow_file).mage(...)`` (Python API)
  - ``--scheduler-type=mage`` in UX tests

Reference: https://docs.metaflow.org/internals/extensions
"""

# Register the CLI group: ``python flow.py mage create ...``
CLIS_DESC = [
    ("mage", ".mage.mage_cli.cli"),
]

# Register the DeployerImpl subclass so ``Deployer(flow_file).mage()`` works.
# The string is a dotted import path relative to this package's ``plugins/`` directory.
DEPLOYER_IMPL_PROVIDERS_DESC = [
    ("mage", ".mage.mage_deployer.MageDeployerImpl"),
]

# Register the step decorator used for internal runtime tracking (optional).
# Uncomment and implement if your orchestrator needs to inject state into steps.
STEP_DECORATORS_DESC = [
    # ("mage_internal", ".mage.mage_decorator.MageInternalDecorator"),
]
