"""DeployedFlow and TriggeredRun objects for the Mage Deployer plugin."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from typing import TYPE_CHECKING, ClassVar, Optional

import metaflow
from metaflow.exception import MetaflowNotFound
from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo
from metaflow.runner.subprocess_manager import SubprocessManager

if TYPE_CHECKING:
    import metaflow.runner.deployer_impl


def _find_flow_for_run_id(run_id: str) -> Optional[str]:
    """Scan the local Metaflow datastore to find which flow class owns ``run_id``."""
    try:
        import metaflow as _mf
        _mf.namespace(None)
        sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL") or os.path.expanduser("~")
        mf_root = os.path.join(sysroot, ".metaflow")
        if not os.path.isdir(mf_root):
            return None
        for entry in os.listdir(mf_root):
            flow_dir = os.path.join(mf_root, entry)
            if os.path.isdir(flow_dir) and os.path.isdir(os.path.join(flow_dir, run_id)):
                return entry
    except Exception:
        pass
    return None


def _make_stub_deployer(name: str):
    """Return a minimal deployer stub for a Mage flow recovered without a flow file."""
    from .mage_deployer import MageDeployer

    stub = object.__new__(MageDeployer)
    stub._deployer_kwargs = {}
    stub.flow_file = ""
    stub.show_output = False
    stub.profile = None
    stub.env = None
    stub.cwd = os.getcwd()
    stub.file_read_timeout = 3600
    stub.env_vars = os.environ.copy()
    stub.spm = SubprocessManager()
    stub.top_level_kwargs = {}
    stub.api = None
    stub.name = name
    stub.flow_name = name
    stub.metadata = "{}"
    stub.additional_info = {}
    return stub


def _make_mage_client(host: str) -> object:
    """Return a requests.Session configured for the given Mage instance."""
    try:
        import requests
    except ImportError:
        raise RuntimeError("The `requests` package is required. Install with: pip install requests")
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session._mage_host = host
    return session


def _get_pipeline_run_status(host: str, pipeline_run_id: int) -> Optional[str]:
    """Fetch the current status of a Mage pipeline run."""
    try:
        import requests
        resp = requests.get(
            "%s/api/pipeline_runs/%d" % (host, pipeline_run_id),
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("pipeline_run", {}).get("status")
    except Exception:
        pass
    return None


class MageTriggeredRun(TriggeredRun):
    """A Mage pipeline run triggered via the Deployer API."""

    @property
    def mage_ui(self) -> Optional[str]:
        """URL to the Mage UI for this pipeline run, if available."""
        try:
            metadata = self._metadata
            url = metadata.get("pipeline_run_url")
            if url:
                return url
        except Exception:
            pass
        return None

    @property
    def run(self):
        """Retrieve the Metaflow Run object, applying deployer env vars."""
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot

            pathspec = self.pathspec
            if pathspec and pathspec.startswith("UNKNOWN/"):
                run_id = pathspec.split("/", 1)[1]
                flow_name = _find_flow_for_run_id(run_id)
                if flow_name:
                    pathspec = "%s/%s" % (flow_name, run_id)
                    self.pathspec = pathspec

            return metaflow.Run(pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        # First check Mage pipeline run status for fast failure detection
        try:
            metadata = self._metadata
            pipeline_run_id = metadata.get("pipeline_run_id")
            mage_host = metadata.get("mage_host", "http://localhost:6789")
            if pipeline_run_id:
                mage_status = _get_pipeline_run_status(mage_host, int(pipeline_run_id))
                if mage_status == "failed":
                    return "FAILED"
                if mage_status in ("cancelled", "canceled"):
                    return "ABORTED"
        except Exception:
            pass

        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class MageDeployedFlow(DeployedFlow):
    """A Metaflow flow deployed as a Mage pipeline."""

    TYPE: ClassVar[Optional[str]] = "mage"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            **additional_info,
        })

    def run(self, **kwargs) -> MageTriggeredRun:
        """Trigger a new execution of this deployed Mage pipeline.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``message="hello"``).

        Returns
        -------
        MageTriggeredRun
        """
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        flow_file = getattr(self.deployer, "flow_file", "") or ""

        # When the deployer was recovered via from_deployment() with a plain name
        # (no flow file available), trigger directly via the Mage REST API.
        if not flow_file:
            return self._trigger_direct(**kwargs)

        # Cap.RUN_PARAMS: run_params MUST be a list, not a tuple.
        run_params = list("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = {"deployer_attribute_file": attribute_file_path}
            if run_params:
                trigger_kwargs["run_params"] = run_params
            for key in ("pipeline_uuid", "mage_host", "mage_project", "schedule_id", "schedule_token"):
                val = additional_info.get(key)
                if val is not None:
                    trigger_kwargs[key] = val
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return MageTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            "Error triggering Mage pipeline %r" % self.deployer.flow_file
        )

    def trigger(
        self,
        run_params=None,
        **kwargs,
    ) -> "MageTriggeredRun":
        """Trigger a new run of this deployed Mage pipeline (Deployer API entry point).

        REQUIRED (Cap.RUN_PARAMS): run_params MUST be a list, not a tuple.
        Click returns tuples for multi-value options; always convert:
            run_params = list(run_params) if run_params else []
        Passing a tuple raises TypeError inside most scheduler client libraries.
        """
        # REQUIRED (Cap.RUN_PARAMS): must be list, not tuple — DO NOT REMOVE
        run_params = list(run_params) if run_params else []

        # Convert list of "key=value" strings into kwargs
        extra_kwargs = {}
        for kv in run_params:
            k, _, v = kv.partition("=")
            extra_kwargs[k.strip()] = v.strip()
        extra_kwargs.update(kwargs)
        return self.run(**extra_kwargs)

    def _trigger_direct(self, **kwargs) -> "MageTriggeredRun":
        """Trigger a Mage pipeline run directly via REST API (no flow file needed)."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        mage_host = additional_info.get("mage_host", "http://localhost:6789")
        mage_project = additional_info.get("mage_project", "metaflow_project")
        pipeline_uuid = additional_info.get("pipeline_uuid") or (
            self.name.lower().replace("-", "_").replace(".", "_")
        )
        schedule_id = additional_info.get("schedule_id")
        schedule_token = additional_info.get("schedule_token")

        try:
            import requests
        except ImportError:
            raise RuntimeError("The `requests` package is required to trigger Mage pipelines.")

        # If schedule_id / schedule_token are missing (e.g. from_deployment with plain name
        # where the initial API lookup failed or the deployment predates schedule creation),
        # do a fresh lookup now at trigger time.
        if not schedule_id or not schedule_token:
            try:
                resp = requests.get(
                    "%s/api/pipelines/%s/pipeline_schedules" % (mage_host, pipeline_uuid),
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    resp_data = resp.json()
                    if "error" not in resp_data:
                        schedules = resp_data.get("pipeline_schedules", [])
                        for sched in schedules:
                            if sched.get("schedule_type") == "api" and sched.get("status") == "active":
                                schedule_id = str(sched["id"])
                                schedule_token = sched.get("token")
                                break
            except Exception:
                pass

        if not schedule_id or not schedule_token:
            raise RuntimeError(
                "Cannot trigger Mage pipeline %r: missing schedule_id or schedule_token. "
                "Deploy the flow first with: python flow.py mage create" % pipeline_uuid
            )

        # Build variables dict from kwargs
        variables = {k: str(v) for k, v in kwargs.items()} if kwargs else {}

        url = "%s/api/pipeline_schedules/%s/pipeline_runs/%s" % (
            mage_host, schedule_id, schedule_token
        )
        payload = {"pipeline_run": {"variables": variables}} if variables else {}
        resp = requests.post(url, json=payload)

        if resp.status_code not in (200, 201):
            raise RuntimeError(
                "Failed to trigger Mage pipeline (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )

        pipeline_run_data = resp.json().get("pipeline_run", {})
        pipeline_run_id = pipeline_run_data.get("id")

        # Compute the Metaflow run_id the same way the init block does:
        # mage-{md5(str(pipeline_run_id))[:16]}
        run_id = "mage-" + hashlib.md5(str(pipeline_run_id).encode()).hexdigest()[:16]

        flow_class_name = additional_info.get("mf_flow_class")
        # Validate that mf_flow_class is a real Python class name (CamelCase, not a UUID).
        # Pipeline UUIDs (e.g. "hello_from_deployment_user_npow_hellofromdeploymentflow")
        # are stored as mf_flow_class when recovered from a plain identifier, but they
        # can't be used as pathspecs. Use "UNKNOWN" to trigger datastore scan in .run.
        if flow_class_name and (
            "_" in flow_class_name  # pipeline UUIDs always have underscores
            or not flow_class_name[0].isupper()  # class names start with uppercase
        ):
            flow_class_name = None
        if not flow_class_name:
            flow_class_name = "UNKNOWN"
        pathspec = "%s/%s" % (flow_class_name, run_id)

        content_dict = {
            "pathspec": pathspec,
            "name": self.name,
            "pipeline_run_id": pipeline_run_id,
            "pipeline_run_url": "%s/pipelines/%s/runs/%d" % (
                mage_host, pipeline_uuid, pipeline_run_id
            ),
            "mage_host": mage_host,
            "mf_flow_class": flow_class_name,
            "metadata": "{}",
        }
        return MageTriggeredRun(deployer=self.deployer, content=json.dumps(content_dict))

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "MageDeployedFlow":
        """Recover a MageDeployedFlow from a deployment identifier.

        ``identifier`` can be either:
        - A JSON string produced by :attr:`id` (preferred).
        - A plain flow name (e.g. ``"HelloFlow"`` or ``"project.branch.HelloFlow"``).
        """
        from .mage_deployer import MageDeployer
        from .mage_compiler import flow_name_to_pipeline_uuid

        info = None
        if identifier.startswith("{"):
            try:
                info = json.loads(identifier)
            except (ValueError, TypeError):
                pass

        if info is not None:
            deployer = MageDeployer(flow_file=info.get("flow_file") or "", deployer_kwargs={})
            deployer.name = info["name"]
            deployer.flow_name = info["flow_name"]
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                k: v for k, v in info.items()
                if k not in ("name", "flow_name", "flow_file")
            }
        else:
            # Cap.FROM_DEPLOYMENT: identifier is either a plain flow name (e.g. "HelloFlow")
            # or a pipeline UUID (e.g. "hello_from_deployment_user_runner_hellofromdeploymentflow").
            # Pipeline UUIDs have no dots (dots are replaced with underscores at compile time),
            # so split(".")[-1] returns the full identifier when it's already a UUID.
            flow_name = identifier.split(".")[-1]
            mage_host = os.environ.get("MAGE_HOST", "http://localhost:6789")
            mage_project = os.environ.get("MAGE_PROJECT", "metaflow_project")
            # If identifier is already a pipeline UUID (underscored lowercase), use as-is.
            # Otherwise derive it from the flow class name.
            pipeline_uuid = flow_name_to_pipeline_uuid(flow_name)

            # Cap.SCHEDULE_METADATA: fetch schedule_id and schedule_token from Mage API
            # so that _trigger_direct can trigger the pipeline without needing a flow file.
            # schedule_id/token are fetched here for efficiency; _trigger_direct will retry
            # the lookup on failure.
            schedule_id = None
            schedule_token = None
            try:
                import requests as _req
                resp = _req.get(
                    "%s/api/pipelines/%s/pipeline_schedules" % (mage_host, pipeline_uuid),
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    resp_data = resp.json()
                    if "error" not in resp_data:
                        schedules = resp_data.get("pipeline_schedules", [])
                        for sched in schedules:
                            if sched.get("schedule_type") == "api" and sched.get("status") == "active":
                                schedule_id = str(sched["id"])
                                schedule_token = sched.get("token")
                                break
            except Exception:
                pass

            deployer = _make_stub_deployer(identifier)
            deployer.name = identifier
            deployer.flow_name = flow_name
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                "pipeline_uuid": pipeline_uuid,
                "mage_host": mage_host,
                "mage_project": mage_project,
                "mf_flow_class": flow_name,
                "schedule_id": schedule_id,
                "schedule_token": schedule_token,
            }

        return cls(deployer=deployer)
