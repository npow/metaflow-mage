"""Metaflow Deployer plugin for Mage.

Registers ``TYPE = "mage"`` so that ``Deployer(flow_file).mage(...)``
is available and the Metaflow Deployer API can be used with Mage.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.mage.plugins.mage.mage_deployer_objects import (
        MageDeployedFlow,
    )


# ---------------------------------------------------------------------------
# OrchestratorCapabilities contract constants (referenced in mage_compiler.py)
# ---------------------------------------------------------------------------
# REQUIRED (Cap.CONFIG_EXPR): METAFLOW_FLOW_CONFIG_VALUE is injected into every
# step block environment by MageCompiler._build_env_vars().
# Without it, @config/@project decorators use empty/default config at task runtime.

# REQUIRED (Cap.PROJECT_BRANCH): "--branch" is forwarded to every step command
# by MageCompiler._build_step_cmd_parts(). @project reads branch_name from
# "--branch" at step runtime, not just at compile time.

# REQUIRED (Cap.RETRY): retry_count is derived from the Mage block attempt context
# in each generated block. Do NOT hardcode retry_count=0.

# REQUIRED: METAFLOW_DATASTORE_SYSROOT_LOCAL is captured at compile time in
# MageCompiler._build_env_vars() and baked into every step block environment.

# REQUIRED: "--environment" flag is passed to step commands via
# MageCompiler._build_step_cmd_parts(). Required for @conda flows to use
# the correct Python interpreter.


class MageDeployer(DeployerImpl):
    """Deployer implementation for Mage.

    Parameters
    ----------
    name : str, optional
        Override the Mage pipeline UUID (default: derived from flow class name).
    mage_host : str, optional
        Mage server base URL (default: "http://localhost:6789").
    mage_project : str, optional
        Mage project name (default: "metaflow_project").
    max_workers : int, optional
        Maximum concurrent block runs (default: 10).
    """

    TYPE: ClassVar[Optional[str]] = "mage"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> Type["MageDeployedFlow"]:
        from .mage_deployer_objects import MageDeployedFlow
        return MageDeployedFlow

    def create(self, **kwargs) -> "MageDeployedFlow":
        """Deploy this flow to a running Mage instance."""
        from .mage_deployer_objects import MageDeployedFlow
        return self._create(MageDeployedFlow, **kwargs)
