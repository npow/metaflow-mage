"""
Mage pipeline compiler for Metaflow flows.

Converts a Metaflow FlowGraph into a Mage pipeline definition.
Each Metaflow step becomes a Mage custom block that executes the step
as a subprocess.

Pipeline execution model:
  - An "init" block computes a stable Metaflow run_id and creates the run.
  - Each step block calls subprocess.run([python, flow_file, 'step', step_name, ...])
  - Block dependencies mirror Metaflow step dependencies.
  - foreach steps create a "foreach_init" block that enumerates items, then
    each body block processes one item, and the join block collects results.

The pipeline_uuid is derived from the flow name (lowercase, underscores).
"""

import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Tuple


def flow_name_to_pipeline_uuid(name: str) -> str:
    """Convert a Metaflow flow name to a Mage pipeline UUID (lowercase, underscores)."""
    return name.lower().replace("-", "_").replace(".", "_")


class MageCompiler:
    """Compiles a Metaflow flow into a Mage pipeline definition (blocks + dependencies).

    IMPORTANT: Mage stores block code as shared files per project
    (e.g., /home/src/{project}/custom/metaflow_init.py).  This means block names
    must be UNIQUE per pipeline so that deploying one pipeline does not overwrite
    the block code of another pipeline.  All block names include the pipeline UUID
    as a prefix (e.g., "helloflow_init", "helloflow_step_start").
    """

    INIT_BLOCK_SUFFIX = "_mfinit"

    def __init__(
        self,
        name: str,
        graph,
        flow,
        flow_file: str,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        username: Optional[str] = None,
        max_workers: int = 10,
        with_decorators: Optional[List[str]] = None,
        branch: Optional[str] = None,
        production: bool = False,
        mage_host: str = "http://localhost:6789",
        mage_project: str = "metaflow_project",
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.flow_file = flow_file
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags or []
        self.namespace = namespace
        self.username = username or ""
        self.max_workers = max_workers
        self.with_decorators = with_decorators or []
        self.branch = branch
        self.production = production
        self.mage_host = mage_host
        self.mage_project = mage_project

        self._project_info = self._get_project()
        self._flow_name = (
            self._project_info["flow_name"] if self._project_info else name
        )

        # Runtime provider info
        self._metadata_type = metadata.TYPE
        self._datastore_type = flow_datastore.TYPE
        self._datastore_root = getattr(flow_datastore, "datastore_root", None) or ""
        self._environment_type = environment.TYPE
        self._event_logger_type = event_logger.TYPE
        self._monitor_type = monitor.TYPE

        # Capture compile-time config values
        self._flow_config_value = self._extract_flow_config_value(flow)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_flow_config_value(flow) -> Optional[str]:
        """Serialize compile-time config values to a JSON string.

        Config values may be OmegaConf DictConfig objects (not JSON-serializable
        by default). We convert them to plain dicts with OmegaConf.to_container()
        before serializing. Without this, json.dumps raises TypeError which is
        silently caught, causing METAFLOW_FLOW_CONFIG_VALUE to never be set
        and @environment(vars={...: config_expr(...)}) to evaluate to None.
        """
        try:
            from metaflow.flowspec import FlowStateItems
            flow_configs = flow._flow_state[FlowStateItems.CONFIGS]

            def _to_serializable(value):
                """Convert OmegaConf or other non-JSON types to plain Python."""
                try:
                    from omegaconf import OmegaConf
                    if OmegaConf.is_config(value):
                        return OmegaConf.to_container(value, resolve=True)
                except ImportError:
                    pass
                # Try direct serialization; if it fails, convert via repr
                try:
                    json.dumps(value)
                    return value
                except (TypeError, ValueError):
                    return str(value)

            config_env = {
                name: _to_serializable(value)
                for name, (value, _is_plain) in flow_configs.items()
                if value is not None
            }
            if config_env:
                return json.dumps(config_env)
        except Exception:
            pass
        return None

    def _get_project(self) -> Optional[Dict[str, str]]:
        """Return @project info dict if the flow uses @project, else None."""
        try:
            project_deco = next(
                (d for d in self.flow._flow_decorators.get("project", [])), None
            )
            if project_deco is None:
                return None
            project_name = project_deco.attributes.get("name", "")
            if not project_name:
                return None
            if self.production:
                branch = "prod"
            elif self.branch:
                branch = "test.%s" % self.branch
            else:
                branch = "user.%s" % self.username
            flow_name = "%s.%s.%s" % (project_name, branch, self.name)
            return {
                "name": project_name,
                "branch": branch,
                "flow_name": flow_name,
            }
        except Exception:
            return None

    @property
    def pipeline_uuid(self) -> str:
        """Mage pipeline UUID derived from the flow name."""
        return flow_name_to_pipeline_uuid(self._flow_name)

    def _get_parameters(self) -> Dict[str, Any]:
        """Return a dict of Metaflow Parameters (excludes Config objects) from the flow.

        Config objects are handled separately via METAFLOW_FLOW_CONFIG_VALUE and must NOT
        be passed as CLI args to `metaflow init` — they are not writable CLI options in
        the same way as Parameters.
        """
        params = {}
        for var, param in self.flow._get_parameters():
            # Skip Config parameters — they are passed via METAFLOW_FLOW_CONFIG_VALUE,
            # not as individual --<name> CLI args to the init command.
            if getattr(param, "IS_CONFIG_PARAMETER", False):
                continue
            default = None
            if "default" in param.kwargs:
                d = param.kwargs["default"]
                if callable(d):
                    try:
                        default = d()
                    except Exception:
                        default = None
                else:
                    default = d
            params[var] = {"default": default, "param": param}
        return params

    def _build_env_vars(self) -> Dict[str, str]:
        """Build the environment variables dict for step subprocesses."""
        env = {}

        # Metadata and datastore config
        env["METAFLOW_DEFAULT_METADATA"] = self._metadata_type
        env["METAFLOW_DEFAULT_DATASTORE"] = self._datastore_type
        if self._datastore_root:
            # METAFLOW_DATASTORE_SYSROOT_LOCAL should be the PARENT of the .metaflow dir.
            # When the local datastore is initialized, it sets datastore_root to
            # <SYSROOT>/.metaflow, so we need to pass the parent to the subprocess.
            import os as _os
            sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")
            if sysroot:
                env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            else:
                # Derive from datastore_root by going up one level from .metaflow
                env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = _os.path.dirname(self._datastore_root)

        # Forward key Metaflow env vars from compile-time environment
        for key, val in os.environ.items():
            if (
                key.startswith("METAFLOW_SERVICE")
                or key.startswith("METAFLOW_DEFAULT")
                or key.startswith("AWS_")
            ):
                env[key] = val

        # Cap.CONFIG_EXPR: inject compile-time config value
        if self._flow_config_value is not None:
            env["METAFLOW_FLOW_CONFIG_VALUE"] = self._flow_config_value

        # Inject username so Metaflow can identify the user inside the container
        # (Docker containers often have no USER/USERNAME env var set).
        if self.username:
            env.setdefault("USER", self.username)
            env.setdefault("USERNAME", self.username)
            env.setdefault("METAFLOW_USER", self.username)

        return env

    def _build_step_env_vars(self, step_node) -> Dict[str, str]:
        """Build env vars for a specific step, including @environment decorator vars.

        The @environment decorator sets env vars via runtime_step_cli(), which is
        called by Metaflow's local runtime. Since Mage calls step subprocesses
        directly (not through the runtime), we must extract and evaluate @environment
        vars at compile time and inject them into the step's block environment.

        config_expr values are evaluated via str() which uses the current config
        context (populated at compile time via METAFLOW_CLICK_API_PROCESS_CONFIG=1).
        """
        env = {}
        # Find @environment decorator for this step
        step_obj = next(
            (s for s in self.flow if s.name == step_node.name), None
        )
        if step_obj is not None:
            for deco in step_obj.decorators:
                if deco.name == "environment":
                    for key, value in deco.attributes.get("vars", {}).items():
                        try:
                            # str() evaluates config_expr objects and descriptor values
                            env[key] = str(value)
                        except Exception:
                            pass
        return env

    def _build_step_cmd_parts(
        self,
        step_name: str,
        extra_args: Optional[List[str]] = None,
    ) -> List[str]:
        """Build the step command argument list (without python interpreter prefix)."""
        cmd = [
            self.flow_file,
            "--no-pylint",
            "--quiet",
            "--metadata", self._metadata_type,
            "--datastore", self._datastore_type,
        ]

        if self._datastore_root:
            cmd += ["--datastore-root", self._datastore_root]

        if self.namespace:
            cmd += ["--namespace", self.namespace]

        for with_deco in self.with_decorators:
            cmd += ["--with", with_deco]

        # Cap.PROJECT_BRANCH: forward --branch to every step subprocess
        if self.branch:
            cmd += ["--branch", self.branch]

        cmd += ["step", step_name]
        # --tag is a step-level option (not top-level)
        for tag in self.tags:
            cmd += ["--tag", tag]
        cmd += ["--run-id", "{{{{ env_vars.MF_RUN_ID }}}}"]
        cmd += ["--task-id", "{{{{ env_vars.MF_TASK_ID_PREFIX }}}}_{step_name}".format(step_name=step_name)]

        if extra_args:
            cmd += extra_args

        return cmd

    def _render_block_content(
        self,
        step_name: str,
        step_node,
        is_init: bool = False,
    ) -> str:
        """Generate the Python content for a Mage custom block."""
        env_vars = self._build_env_vars()
        env_lines = "\n".join(
            "    env[%r] = %r" % (k, v) for k, v in sorted(env_vars.items())
        )

        if is_init:
            return self._render_init_block_content(env_lines)

        return self._render_step_block_content(step_name, step_node, env_lines)

    def _render_init_block_content(self, env_lines: str) -> str:
        """Render the init block that creates the Metaflow run."""
        # Build --tag args for the init command
        tag_args = ""
        for tag in self.tags:
            tag_args += '        "--tag", %r,\n' % tag

        # Build parameter names list for passing flow Parameters from pipeline variables
        params = self._get_parameters()
        param_names_repr = repr(list(params.keys()))

        return '''import hashlib
import os
import subprocess
import sys

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def metaflow_init(*args, **kwargs):
    """Initialize a Metaflow run: compute run_id and call `metaflow init`."""
    # Compute a stable run_id from the Mage pipeline_run_id
    pipeline_run_id = str(kwargs.get('pipeline_run_id', 'unknown'))
    run_id = "mage-" + hashlib.md5(pipeline_run_id.encode()).hexdigest()[:16]

    env = os.environ.copy()
{env_lines}
    env["MF_RUN_ID"] = run_id
    env["MF_PIPELINE_RUN_ID"] = pipeline_run_id

    # Cap.PARAMETERS: pass flow Parameters from Mage pipeline variables to init command
    # Mage passes pipeline variables (set at trigger time) via kwargs.
    # We forward them as --<param_name> <value> CLI args to `metaflow init`.
    param_names = {param_names_repr}
    param_args = []
    # Mage may expose pipeline-run variables in different ways depending on version:
    #   1. Flat in kwargs directly (most common: global_vars merged in)
    #   2. Nested under kwargs['variables'] (older API path)
    # We check BOTH sources for each param so we cover all Mage versions.
    _MAGE_INTERNAL_KEYS = {{
        'pipeline_run_id', 'context', 'block_uuid', 'event',
        'configuration', 'spark', 'logger', 'run_started_at',
        'trigger_name', 'execution_date', 'execution_partition',
        'ds', 'hr', 'interval_start_datetime', 'interval_end_datetime',
        'interval_seconds', 'retry', 'env',
    }}
    _nested_vars = kwargs.get('variables') or {{}}
    for pname in param_names:
        # Priority: nested variables dict > direct kwarg
        val = _nested_vars.get(pname)
        if val is None:
            val = kwargs.get(pname)
        if val is not None and pname not in _MAGE_INTERNAL_KEYS:
            param_args += ["--" + pname, str(val)]

    # Initialize the Metaflow run (creates _parameters artifact, registers run)
    cmd = [
        sys.executable,
        {flow_file!r},
        "--no-pylint",
        "--quiet",
        "--metadata", {metadata_type!r},
        "--datastore", {datastore_type!r},
{datastore_root_line}
        "init",
        "--run-id", run_id,
        "--task-id", "mage-params",
{tag_args}    ] + param_args
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print("STDOUT:", result.stdout[-2000:])
        print("STDERR:", result.stderr[-2000:])
        raise RuntimeError("metaflow init failed: " + result.stderr[-500:])

    print("Metaflow run initialized: " + run_id)
    return {{"mf_run_id": run_id, "pipeline_run_id": pipeline_run_id}}
'''.format(
            env_lines=env_lines,
            flow_file=self.flow_file,
            metadata_type=self._metadata_type,
            datastore_type=self._datastore_type,
            tag_args=tag_args,
            param_names_repr=param_names_repr,
            datastore_root_line=(
                '        "--datastore-root", %r,\n' % self._datastore_root
                if self._datastore_root else ""
            ),
        )

    def _build_input_paths_code(self, step_name: str, step_node) -> str:
        """Return Python code fragment that computes `input_paths` for a step.

        For the `start` step: use run_id + "/_parameters/" + params_task_id
        For all other steps: run_id + "/" + each_parent_step + "/" + "1"
        For foreach join steps: build comma-separated paths from all foreach body tasks
        """
        if step_name == "start":
            return (
                "    # start step reads from _parameters artifact created by init\n"
                '    input_paths = run_id + "/_parameters/" + params_task_id\n'
            )

        # Collect parent step names
        parents = list(step_node.in_funcs) if step_node.in_funcs else []
        if not parents:
            return "    input_paths = None\n"

        if len(parents) == 1:
            parent = parents[0]
            if step_node.type == "join":
                # Foreach join: input from all foreach body tasks
                # We track foreach_count from the upstream foreach step output
                return (
                    "    # foreach join: gather all body task outputs\n"
                    "    foreach_count = int(upstream_output.get('foreach_count', 1)) if isinstance(upstream_output, dict) else 1\n"
                    "    input_paths = ','.join(\n"
                    "        run_id + '/' + %r + '/' + run_id + '-' + %r + '-' + str(i)\n"
                    "        for i in range(foreach_count)\n"
                    "    )\n"
                ) % (parent, parent)
            return '    input_paths = run_id + "/" + %r + "/" + task_id\n' % parent

        # Multiple parents (join from split/branch): comma-separated
        paths = " + \",\" + ".join(
            'run_id + "/" + %r + "/" + task_id' % p for p in parents
        )
        return "    input_paths = %s\n" % paths

    def _render_step_block_content(
        self,
        step_name: str,
        step_node,
        env_lines: str,
    ) -> str:
        """Render a block that runs one Metaflow step as a subprocess."""
        # Detect if this is a foreach body step:
        # is_inside_foreach=True and at least one parent has type "foreach"
        is_foreach_body = (
            getattr(step_node, "is_inside_foreach", False)
            and step_node.type not in ("join",)
        )

        # Build the top-level command parts (before "step" subcommand)
        top_parts = [
            "sys.executable",
            repr(self.flow_file),
            '"--no-pylint"',
            '"--quiet"',
            '"--metadata"', repr(self._metadata_type),
            '"--datastore"', repr(self._datastore_type),
        ]

        if self._datastore_root:
            top_parts += ['"--datastore-root"', repr(self._datastore_root)]

        if self.namespace:
            top_parts += ['"--namespace"', repr(self.namespace)]

        for with_deco in self.with_decorators:
            top_parts += ['"--with"', repr(with_deco)]

        # Cap.PROJECT_BRANCH: --branch must be in every step command
        if self.branch:
            top_parts += ['"--branch"', repr(self.branch)]

        step_parts = [
            '"step"', repr(step_name),
            '"--run-id"', 'run_id',
            '"--task-id"', 'task_id',
            '"--retry-count"', 'str(retry_count)',
        ]

        # --tag is a step-level option (passed after "step" subcommand), not top-level
        for tag in self.tags:
            step_parts += ['"--tag"', repr(tag)]

        input_paths_code = self._build_input_paths_code(step_name, step_node)

        top_cmd_list = "[\n        %s,\n    ]" % ",\n        ".join(top_parts)
        step_cmd_list = "[\n        %s,\n    ]" % ",\n        ".join(step_parts)

        if is_foreach_body:
            # Cap.FOREACH_SPLIT_INDEX: foreach body steps must run once per item.
            # The foreach step output contains foreach_count; we loop over all items,
            # running the step subprocess with --split-index <i> and a per-item task_id.
            # The join block expects task_ids of the form: run_id + '-' + step_name + '-' + str(i)
            #
            # input_paths_code uses 4-space indentation; inside the loop we need 8 spaces.
            indented_input_paths_code = "    " + input_paths_code.rstrip("\n").replace("\n    ", "\n        ")
            return '''import os
import subprocess
import sys

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def run_{step_name}(*args, **kwargs):
    """Execute Metaflow foreach body step: {step_name} (runs once per foreach item)"""
    # Get run_id and foreach_count from upstream block output
    upstream_output = args[0] if args else {{}}
    if isinstance(upstream_output, dict):
        run_id = upstream_output.get("mf_run_id") or upstream_output.get("run_id")
        foreach_count = int(upstream_output.get("foreach_count", 1))
    else:
        run_id = str(upstream_output) if upstream_output else None
        foreach_count = 1

    if not run_id:
        raise RuntimeError("No run_id found from upstream block output")

    params_task_id = "mage-params"
    # Cap.RETRY: derive retry_count from Mage block attempt number.
    # Mage exposes retry info as kwargs['retry']['attempts'] (via BlockExecutor.retry_metadata).
    # Fall back to kwargs['context']['retry_count'] for compatibility with older Mage versions.
    if kwargs:
        _retry_meta = kwargs.get("retry") or {{}}
        retry_count = int(_retry_meta.get("attempts", 0)) if isinstance(_retry_meta, dict) else 0
        if retry_count == 0:
            retry_count = int((kwargs.get("context") or {{}}).get("retry_count", 0))
    else:
        retry_count = 0

    env = os.environ.copy()
{env_lines}
    env["MF_RUN_ID"] = run_id

    top_cmd = {top_cmd_list}

    # Cap.FOREACH_SPLIT_INDEX: run the step once per foreach item
    for split_index in range(foreach_count):
        # Per-item task_id: join block expects run_id + '-' + step_name + '-' + str(i)
        task_id = run_id + "-" + {step_name_repr} + "-" + str(split_index)
        # input_paths: parent foreach step task (task_id for parent is "mage-1")
        input_paths = run_id + "/" + {parent_step_repr} + "/mage-1"
        step_cmd = {step_cmd_list}
        step_cmd += ["--split-index", str(split_index)]
        step_cmd += ["--input-paths", input_paths]
        cmd = top_cmd + step_cmd

        print("Running {step_name} split_index=%d (task_id=%s)" % (split_index, task_id))
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print("STDOUT:", result.stdout[-4000:])
            print("STDERR:", result.stderr[-4000:])
            raise RuntimeError(
                "Metaflow step {step_name!r} split_index=%d failed (exit %d): %s"
                % (split_index, result.returncode, result.stderr[-500:])
            )
        if result.stdout:
            print("STDOUT:", result.stdout[-2000:])

    print("Step {step_name} completed all %d foreach items" % foreach_count)
    return {{"run_id": run_id, "step": {step_name!r}, "foreach_count": foreach_count, "status": "success"}}
'''.format(
                step_name=step_name,
                step_name_repr=repr(step_name),
                parent_step_repr=repr(list(step_node.in_funcs)[0] if step_node.in_funcs else ""),
                env_lines=env_lines,
                top_cmd_list=top_cmd_list,
                step_cmd_list=step_cmd_list,
            )

        # For foreach steps, we need to read _foreach_num_splits after execution
        is_foreach_step = (step_node.type == "foreach")
        foreach_count_code = ""
        foreach_return_field = ""
        if is_foreach_step:
            # Read _foreach_num_splits directly from the local datastore files.
            # The task datastore stores artifacts as content-addressed blobs:
            #   <sysroot>/.metaflow/<flow>/<run_id>/<step>/<task_id>/0.data.json
            #     -> maps "_foreach_num_splits" -> CA key
            #   <sysroot>/.metaflow/<flow>/data/<key[:2]>/<key>
            #     -> gzip+pickle blob of the actual value
            # Read _foreach_num_splits via a fresh subprocess to avoid any
            # LocalStorage.datastore_root caching in the current Mage Python process.
            # The subprocess uses the same env as the step subprocess, so paths match.
            # Read _foreach_num_splits via fresh subprocess.
            # Use 'find' to locate the file regardless of where sysroot is,
            # because the Mage container (HOME=/root) may write to /root/.metaflow
            # even when env["METAFLOW_DATASTORE_SYSROOT_LOCAL"]=/home/runner.
            # Read _foreach_num_splits via Metaflow Task API in a fresh subprocess.
            # The fresh subprocess avoids LocalStorage.datastore_root caching in the
            # Mage server process. Checks multiple candidate sysroot dirs to handle
            # cases where the container's HOME (/root) differs from env sysroot.
            foreach_count_code = (
                '\n'
                '    # Cap.FOREACH_COUNT: read _foreach_num_splits via fresh subprocess\n'
                '    foreach_count = 1\n'
                '    try:\n'
                '        _sysroot = env.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")\n'
                '        _fc_result = subprocess.run(\n'
                '            [sys.executable, "-c", r"""\n'
                'import json, gzip, pickle, os, sys\n'
                '_r = sys.argv[1]; _f = sys.argv[2]; _t = sys.argv[3]\n'
                '_hint = sys.argv[4] if len(sys.argv) > 4 else ""\n'
                '# Check multiple candidate sysroot dirs (container HOME may differ from env)\n'
                'import os as _os\n'
                '_candidates = [_hint, "/home/runner", "/root", _os.environ.get("HOME",""), "/tmp"]\n'
                '_p = None\n'
                'for _cand in _candidates:\n'
                '    if not _cand: continue\n'
                '    _try = _os.path.join(_cand, ".metaflow", _f, _r, "start", _t, "0.data.json")\n'
                '    if _os.path.isfile(_try):\n'
                '        _p = _try; _mf_root = _os.path.join(_cand, ".metaflow"); break\n'
                'if not _p: sys.exit(0)\n'
                '_obj = json.load(open(_p)).get("objects", {})\n'
                '_key = _obj.get("_foreach_num_splits")\n'
                'if not _key: sys.exit(0)\n'
                '_bp = _os.path.join(_mf_root, _f, "data", _key[:2], _key)\n'
                '_blob = open(_bp, "rb").read()\n'
                'try: print(int(pickle.loads(gzip.decompress(_blob))))\n'
                'except: print(int(pickle.loads(_blob)))\n'
                '""",\n'
                '                run_id, %s, task_id, _sysroot],\n'
                '            env=env, capture_output=True, text=True\n'
                '        )\n'
                '        _out = _fc_result.stdout.strip()\n'
                '        if _out.isdigit():\n'
                '            foreach_count = int(_out)\n'
                '            print("Foreach step %s:", foreach_count, "items")\n'
                '    except Exception as _e:\n'
                '        print("Warning: foreach_count error:", _e)\n'
            ) % (
                repr(self.name),
                step_name,
            )
            foreach_return_field = ', "foreach_count": foreach_count'

        return '''import os
import subprocess
import sys

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def run_{step_name}(*args, **kwargs):
    """Execute Metaflow step: {step_name}"""
    # Get run_id from upstream block output
    upstream_output = args[0] if args else {{}}
    if isinstance(upstream_output, dict):
        run_id = upstream_output.get("mf_run_id") or upstream_output.get("run_id")
    else:
        run_id = str(upstream_output) if upstream_output else None

    if not run_id:
        raise RuntimeError("No run_id found from upstream block output")

    params_task_id = "mage-params"  # task_id used in metaflow init (non-integer so metadata registers it)
    task_id = "mage-1"  # non-integer task_id forces local metadata to create _self.json
    # Cap.RETRY: derive retry_count from Mage block attempt number.
    # Mage exposes retry info as kwargs['retry']['attempts'] (via BlockExecutor.retry_metadata).
    # Fall back to kwargs['context']['retry_count'] for compatibility with older Mage versions.
    if kwargs:
        _retry_meta = kwargs.get("retry") or {{}}
        retry_count = int(_retry_meta.get("attempts", 0)) if isinstance(_retry_meta, dict) else 0
        if retry_count == 0:
            retry_count = int((kwargs.get("context") or {{}}).get("retry_count", 0))
    else:
        retry_count = 0

    env = os.environ.copy()
{env_lines}
    env["MF_RUN_ID"] = run_id

    # Compute input_paths for this step
{input_paths_code}
    top_cmd = {top_cmd_list}
    step_cmd = {step_cmd_list}
    if input_paths:
        step_cmd += ["--input-paths", input_paths]
    cmd = top_cmd + step_cmd

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print("STDOUT:", result.stdout[-4000:])
        print("STDERR:", result.stderr[-4000:])
        raise RuntimeError(
            "Metaflow step {step_name!r} failed (exit %d): %s" % (result.returncode, result.stderr[-500:])
        )

    print("Step {step_name} completed successfully")
    if result.stdout:
        print("STDOUT:", result.stdout[-2000:])
    # DEBUG
    if {step_name!r} == "start":
        print("DBG CMD:", " ".join(str(c) for c in cmd[:8]))
        print("DBG STDOUT:", result.stdout[:500] if result.stdout else "(empty)")
        print("DBG STDERR:", result.stderr[:500] if result.stderr else "(empty)")
        import subprocess as _dbgsp
        _dbg = _dbgsp.run(
            ["find", "/", "-maxdepth", "10", "-name", "0.data.json", "-path", "*/start/*"],
            capture_output=True, text=True, timeout=15
        ).stdout.strip()
        print("DBG_FIND:", _dbg[:500] if _dbg else "(nothing found)")
        _dbg2 = _dbgsp.run(
            ["find", "/", "-maxdepth", "10", "-name", "0.data.json"],
            capture_output=True, text=True, timeout=15
        ).stdout.strip()
        print("DBG_FIND_ALL:", _dbg2[:500] if _dbg2 else "(no 0.data.json anywhere)")
{foreach_count_code}
    return {{"run_id": run_id, "step": {step_name!r}, "status": "success"{foreach_return_field}}}
'''.format(
            step_name=step_name,
            env_lines=env_lines,
            input_paths_code=input_paths_code,
            top_cmd_list=top_cmd_list,
            step_cmd_list=step_cmd_list,
            foreach_count_code=foreach_count_code,
            foreach_return_field=foreach_return_field,
        )

    def _block_prefix(self) -> str:
        """Short prefix derived from pipeline UUID for unique block naming.

        Mage stores all custom block code as shared project-level files:
          /home/src/{project}/custom/{block_name}.py
        Two pipelines sharing a block name (e.g. "metaflow_init") will
        overwrite each other's code.  We prefix every block name with
        a slug of the pipeline UUID to ensure isolation.

        We use the first 12 chars to keep block names readable in the UI.
        """
        # Replace hyphens and dots with underscores; truncate to 12 chars
        slug = self.pipeline_uuid.replace("-", "_").replace(".", "_")[:12]
        return slug

    def compile(self) -> List[Dict[str, Any]]:
        """Return a list of block dicts describing the Mage pipeline.

        Each dict has: name, type, language, content, upstream_blocks
        """
        blocks = []

        # Derive a pipeline-unique prefix to avoid block file collisions
        prefix = self._block_prefix()
        init_block_name = "%s%s" % (prefix, self.INIT_BLOCK_SUFFIX)

        init_content = self._render_init_block_content(
            "\n".join(
                "    env[%r] = %r" % (k, v)
                for k, v in sorted(self._build_env_vars().items())
            )
        )

        blocks.append({
            "name": init_block_name,
            "type": "custom",
            "language": "python",
            "upstream_blocks": [],
            "content": init_content,
        })

        # Build one block per step
        env_vars = self._build_env_vars()
        env_lines = "\n".join(
            "    env[%r] = %r" % (k, v) for k, v in sorted(env_vars.items())
        )

        # First pass: pre-populate step_to_block for ALL nodes so that upstream
        # references are correct regardless of graph iteration order.
        # (Graph may iterate in depth-first or reverse topological order.)
        step_to_block = {init_block_name: init_block_name}
        for node in self.graph:
            step_to_block[node.name] = "%s_s_%s" % (prefix, node.name)

        # Second pass: create blocks with correct upstream references
        for node in self.graph:
            step_name = node.name
            block_name = "%s_s_%s" % (prefix, step_name)

            # Determine upstream blocks
            if step_name == "start":
                upstream = [init_block_name]
            else:
                upstream = [
                    step_to_block.get(p, "%s_s_%s" % (prefix, p))
                    for p in node.in_funcs
                ]

            # Merge global env vars with step-specific @environment decorator vars.
            # @environment sets env vars via runtime_step_cli() which Mage doesn't call,
            # so we evaluate them at compile time and inject directly into the block env.
            step_specific_env = self._build_step_env_vars(node)
            if step_specific_env:
                merged_env = dict(env_vars)
                merged_env.update(step_specific_env)
                step_env_lines = "\n".join(
                    "    env[%r] = %r" % (k, v) for k, v in sorted(merged_env.items())
                )
            else:
                step_env_lines = env_lines

            content = self._render_step_block_content(step_name, node, step_env_lines)

            blocks.append({
                "name": block_name,
                "type": "custom",
                "language": "python",
                "upstream_blocks": upstream,
                "content": content,
            })

        return blocks
