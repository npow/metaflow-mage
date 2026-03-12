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

import json
import os
from typing import Any, Dict, List, Optional, Tuple


def flow_name_to_pipeline_uuid(name: str) -> str:
    """Convert a Metaflow flow name to a Mage pipeline UUID (lowercase, underscores)."""
    return name.lower().replace("-", "_").replace(".", "_")


# ---------------------------------------------------------------------------
# Shared constants for generated block code
# ---------------------------------------------------------------------------

# Standard header for every generated Mage custom block.
_BLOCK_HEADER = """\
import os
import subprocess
import sys

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

"""

# Inline Python snippet that reads _foreach_num_splits via FlowDataStore API.
# Embedded directly in generated block code — no subprocess needed.
_FOREACH_NUM_SPLITS_READER_SNIPPET = '''\
        from metaflow.datastore import FlowDataStore as _FDS
        from metaflow.plugins import DATASTORES as _DATASTORES
        _ds_impl = next(_d for _d in _DATASTORES if _d.TYPE == env.get("METAFLOW_DEFAULT_DATASTORE", "local"))
        _ds_root = _ds_impl.get_datastore_root_from_config(lambda *a: None)
        _fds = _FDS({flow_name!r}, None, storage_impl=_ds_impl, ds_root=_ds_root)
'''


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
        self.tags = tags or []
        self.namespace = namespace
        self.username = username or ""
        self.with_decorators = with_decorators or []
        self.branch = branch
        self.production = production

        self._project_info = self._get_project()
        self._flow_name = (
            self._project_info["flow_name"] if self._project_info else name
        )

        self._metadata_type = metadata.TYPE
        self._datastore_type = flow_datastore.TYPE
        self._datastore_root = getattr(flow_datastore, "datastore_root", None) or ""
        self._flow_config_value = self._extract_flow_config_value(flow)

    # ------------------------------------------------------------------
    # Helpers: flow metadata and environment
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
        """Return a dict of Metaflow Parameters (excludes Config objects) from the flow."""
        params = {}
        for var, param in self.flow._get_parameters():
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

        env["METAFLOW_DEFAULT_METADATA"] = self._metadata_type
        env["METAFLOW_DEFAULT_DATASTORE"] = self._datastore_type
        if self._datastore_root:
            env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = (
                os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
                or os.path.dirname(self._datastore_root)
            )

        # Forward key Metaflow env vars from compile-time environment
        for key, val in os.environ.items():
            if (
                key.startswith("METAFLOW_SERVICE")
                or key.startswith("METAFLOW_DEFAULT")
                or key.startswith("AWS_")
            ):
                env[key] = val

        if self._flow_config_value is not None:
            env["METAFLOW_FLOW_CONFIG_VALUE"] = self._flow_config_value

        if self.username:
            env.setdefault("USER", self.username)
            env.setdefault("USERNAME", self.username)
            env.setdefault("METAFLOW_USER", self.username)

        return env

    def _find_step_obj(self, step_node):
        """Look up the Step object for a graph node (or None)."""
        return next(
            (s for s in self.flow if s.name == step_node.name), None
        )

    def _build_step_env_vars(self, step_node) -> Dict[str, str]:
        """Build env vars for a specific step, including @environment decorator vars.

        The @environment decorator sets env vars via runtime_step_cli(), which is
        called by Metaflow's local runtime. Since Mage calls step subprocesses
        directly (not through the runtime), we must extract and evaluate @environment
        vars at compile time and inject them into the step's block environment.
        """
        env = {}
        step_obj = self._find_step_obj(step_node)
        if step_obj is not None:
            for deco in step_obj.decorators:
                if deco.name == "environment":
                    for key, value in deco.attributes.get("vars", {}).items():
                        try:
                            env[key] = str(value)
                        except Exception:
                            pass
        return env

    def _get_max_user_code_retries(self, step_node) -> int:
        """Extract max retry count from step's @retry decorator (0 if absent)."""
        step_obj = self._find_step_obj(step_node)
        if step_obj is not None:
            for deco in step_obj.decorators:
                if deco.name == "retry":
                    return int(deco.attributes.get("times", 3))
        return 0

    def _get_timeout_seconds(self, step_node) -> Optional[int]:
        """Extract timeout in seconds from step's @timeout decorator (None if absent)."""
        step_obj = self._find_step_obj(step_node)
        if step_obj is not None:
            for deco in step_obj.decorators:
                if deco.name == "timeout":
                    seconds = deco.attributes.get("seconds")
                    if seconds is not None:
                        return int(seconds)
        return None

    def _get_parent_step(self, step_node) -> str:
        """Return the first parent step name, or empty string if none."""
        return list(step_node.in_funcs)[0] if step_node.in_funcs else ""

    def _get_nested_foreach_extras(self, step_name: str, step_node) -> Tuple[str, str]:
        """Return (read_code, return_extra) for a foreach body that is also a foreach step."""
        if step_node.type == "foreach":
            return (
                self._build_foreach_count_reader_loop(step_name),
                ', "nested_foreach_counts": nested_foreach_counts',
            )
        return ("", "")

    @staticmethod
    def _format_env_lines(env_dict: Dict[str, str]) -> str:
        """Format an env dict as indented assignment lines for generated block code."""
        return "\n".join(
            "    env[%r] = %r" % (k, v) for k, v in sorted(env_dict.items())
        )

    @staticmethod
    def _make_block_dict(
        name: str,
        upstream: List[str],
        content: str,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build a Mage block definition dict."""
        block = {
            "name": name,
            "type": "custom",
            "language": "python",
            "upstream_blocks": upstream,
            "content": content,
        }
        if timeout:
            block["timeout"] = timeout
        return block

    # ------------------------------------------------------------------
    # Code generation: shared fragments
    # ------------------------------------------------------------------

    @staticmethod
    def _render_upstream_parsing(mode="simple"):
        """Return code that parses run_id (and optionally foreach counts) from upstream output.

        Modes: "simple" (run_id only), "foreach" (+foreach_count), "nested" (+nested_foreach_counts).
        """
        lines = [
            '    upstream_output = args[0] if args else {}',
            '    if isinstance(upstream_output, dict):',
            '        run_id = upstream_output.get("mf_run_id") or upstream_output.get("run_id")',
        ]
        if mode == "nested":
            lines += [
                '        foreach_count = int(upstream_output.get("foreach_count", 1))',
                '        nested_foreach_counts = upstream_output.get("nested_foreach_counts", [1] * foreach_count)',
            ]
        elif mode == "foreach":
            lines += [
                '        foreach_count = int(upstream_output.get("foreach_count", 1))',
            ]
        lines += [
            '    else:',
            '        run_id = str(upstream_output) if upstream_output else None',
        ]
        if mode == "nested":
            lines += [
                '        foreach_count = 1',
                '        nested_foreach_counts = [1]',
            ]
        elif mode == "foreach":
            lines += [
                '        foreach_count = 1',
            ]
        lines += [
            '',
            '    if not run_id:',
            '        raise RuntimeError("No run_id found from upstream block output")',
        ]
        return "\n".join(lines) + "\n"

    def _render_block_preamble(
        self,
        step_name: str,
        docstring: str,
        env_lines: str,
        top_cmd_list: str,
        max_retries: int,
        upstream_parsing: str,
        timeout_seconds: Optional[int] = None,
    ) -> str:
        """Return the shared preamble used by all step block templates.

        Includes: header, @custom decorator, function def, upstream parsing,
        env setup, max_retries, and top_cmd assignment.
        """
        timeout_line = ""
        if timeout_seconds:
            timeout_line = "\n    _timeout = %d  # @timeout seconds" % timeout_seconds
        return '''{header}
@custom
def run_{step_name}(*args, **kwargs):
    """{docstring}"""
{upstream_parsing}
    max_retries = {max_retries}{timeout_line}

    env = os.environ.copy()
{env_lines}
    env["MF_RUN_ID"] = run_id

    top_cmd = {top_cmd_list}
'''.format(
            header=_BLOCK_HEADER,
            step_name=step_name,
            docstring=docstring,
            upstream_parsing=upstream_parsing,
            max_retries=max_retries,
            env_lines=env_lines,
            top_cmd_list=top_cmd_list,
            timeout_line=timeout_line,
        )

    def _build_cmd_lists(self, step_name: str) -> Tuple[str, str]:
        """Build the top-level and step-level command list strings for code generation.

        Returns (top_cmd_list, step_cmd_list) as Python source code strings.
        """
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

        if self.branch:
            top_parts += ['"--branch"', repr(self.branch)]

        step_parts = [
            '"step"', repr(step_name),
            '"--run-id"', 'run_id',
            '"--task-id"', 'task_id',
            '"--retry-count"', 'str(retry_count)',
        ]

        for tag in self.tags:
            step_parts += ['"--tag"', repr(tag)]

        top_cmd_list = "[\n        %s,\n    ]" % ",\n        ".join(top_parts)
        step_cmd_list = "[\n        %s,\n    ]" % ",\n        ".join(step_parts)
        return top_cmd_list, step_cmd_list

    def _build_foreach_count_reader_single(self, step_name: str) -> str:
        """Return code that reads _foreach_num_splits for a single task.

        Uses the FlowDataStore API instead of subprocess+pickle.
        """
        return (
            '\n'
            '    # Cap.FOREACH_COUNT: read _foreach_num_splits via FlowDataStore API\n'
            '    foreach_count = 1\n'
            '    try:\n'
            + _FOREACH_NUM_SPLITS_READER_SNIPPET.format(flow_name=self.name)
            + '        _tds = _fds.get_task_datastore(run_id, %r, task_id, attempt=0, mode="r")\n'
            '        foreach_count = int(_tds["_foreach_num_splits"])\n'
            '        print("Foreach step %s:", foreach_count, "items")\n'
            '    except Exception as _e:\n'
            '        print("Warning: foreach_count error:", _e)\n'
        ) % (step_name, step_name)

    def _build_foreach_count_reader_loop(self, step_name: str) -> str:
        """Return code that reads _foreach_num_splits per foreach body task.

        Used when a foreach body step is ALSO a foreach step (nested foreach).
        Uses the FlowDataStore API instead of subprocess+pickle.
        """
        return (
            '\n'
            '    # Read _foreach_num_splits from each body task for nested foreach\n'
            '    nested_foreach_counts = []\n'
            '    try:\n'
            + _FOREACH_NUM_SPLITS_READER_SNIPPET.format(flow_name=self.name)
            + '    except Exception as _e:\n'
            '        print("Warning: could not init FlowDataStore for nested foreach:", _e)\n'
            '        _fds = None\n'
            '    for _nfc_i in range(foreach_count):\n'
            '        _nfc_task_id = run_id + "-" + %r + "-" + str(_nfc_i)\n'
            '        _nfc_count = 1\n'
            '        if _fds is not None:\n'
            '            try:\n'
            '                _nfc_tds = _fds.get_task_datastore(run_id, %r, _nfc_task_id, attempt=0, mode="r")\n'
            '                _nfc_count = int(_nfc_tds["_foreach_num_splits"])\n'
            '            except Exception as _e:\n'
            '                print("Warning: nested foreach_count error for task %%s: %%s" %% (_nfc_task_id, _e))\n'
            '        nested_foreach_counts.append(_nfc_count)\n'
            '        print("Nested foreach: %%s task %%d has %%d items" %% (%r, _nfc_i, _nfc_count))\n'
        ) % (step_name, step_name, step_name)

    def _build_input_paths_code(self, step_name: str, step_node) -> str:
        """Return Python code fragment that computes `input_paths` for a step."""
        if step_name == "start":
            return (
                "    # start step reads from _parameters artifact created by init\n"
                '    input_paths = run_id + "/_parameters/" + params_task_id\n'
            )

        parents = list(step_node.in_funcs) if step_node.in_funcs else []
        if not parents:
            return "    input_paths = None\n"

        if len(parents) == 1:
            parent = parents[0]
            if step_node.type == "join":
                return (
                    "    # foreach join: gather all body task outputs\n"
                    "    foreach_count = int(upstream_output.get('foreach_count', 1)) if isinstance(upstream_output, dict) else 1\n"
                    "    input_paths = ','.join(\n"
                    "        run_id + '/' + %r + '/' + run_id + '-' + %r + '-' + str(i)\n"
                    "        for i in range(foreach_count)\n"
                    "    )\n"
                ) % (parent, parent)
            return '    input_paths = run_id + "/" + %r + "/" + task_id\n' % parent

        paths = " + \",\" + ".join(
            'run_id + "/" + %r + "/" + task_id' % p for p in parents
        )
        return "    input_paths = %s\n" % paths

    # ------------------------------------------------------------------
    # Code generation: block renderers
    # ------------------------------------------------------------------

    def _render_init_block_content(self, env_lines: str) -> str:
        """Render the init block that creates the Metaflow run."""
        tag_args = "".join('        "--tag", %r,\n' % tag for tag in self.tags)

        params = self._get_parameters()
        param_names_repr = repr(list(params.keys()))

        return '''import hashlib
{header}
@custom
def metaflow_init(*args, **kwargs):
    """Initialize a Metaflow run: compute run_id and call `metaflow init`."""
    pipeline_run_id = str(kwargs.get('pipeline_run_id', 'unknown'))
    run_id = "mage-" + hashlib.md5(pipeline_run_id.encode()).hexdigest()[:16]

    env = os.environ.copy()
{env_lines}
    env["MF_RUN_ID"] = run_id
    env["MF_PIPELINE_RUN_ID"] = pipeline_run_id

    # Pass flow Parameters from Mage pipeline variables to init command
    param_names = {param_names_repr}
    param_args = []
    _MAGE_INTERNAL_KEYS = {{
        'pipeline_run_id', 'context', 'block_uuid', 'event',
        'configuration', 'spark', 'logger', 'run_started_at',
        'trigger_name', 'execution_date', 'execution_partition',
        'ds', 'hr', 'interval_start_datetime', 'interval_end_datetime',
        'interval_seconds', 'retry', 'env',
    }}
    _nested_vars = kwargs.get('variables') or {{}}
    for pname in param_names:
        val = _nested_vars.get(pname)
        if val is None:
            val = kwargs.get(pname)
        if val is not None and pname not in _MAGE_INTERNAL_KEYS:
            param_args += ["--" + pname, str(val)]

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
            header=_BLOCK_HEADER,
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

    def _render_foreach_body_block(
        self,
        step_name: str,
        step_node,
        env_lines: str,
        top_cmd_list: str,
        step_cmd_list: str,
    ) -> str:
        """Render a foreach body block that runs the step once per foreach item."""
        parent_step = self._get_parent_step(step_node)
        max_retries = self._get_max_user_code_retries(step_node)
        timeout_seconds = self._get_timeout_seconds(step_node)
        foreach_read_code, foreach_return_extra = self._get_nested_foreach_extras(step_name, step_node)

        preamble = self._render_block_preamble(
            step_name,
            "Execute Metaflow foreach body step: %s (runs once per foreach item)" % step_name,
            env_lines, top_cmd_list, max_retries,
            self._render_upstream_parsing("foreach"),
            timeout_seconds=timeout_seconds,
        )

        timeout_kwarg = ", timeout=_timeout" if timeout_seconds else ""

        return '''{preamble}
    # Run the step once per foreach item
    for split_index in range(foreach_count):
        task_id = run_id + "-" + {step_name_repr} + "-" + str(split_index)
        input_paths = run_id + "/" + {parent_step_repr} + "/mage-1"
        for retry_count in range(max_retries + 1):
            step_cmd = {step_cmd_list}
            step_cmd += ["--split-index", str(split_index)]
            step_cmd += ["--input-paths", input_paths]
            cmd = top_cmd + step_cmd

            print("Running {step_name} split_index=%d retry=%d (task_id=%s)" % (split_index, retry_count, task_id))
            result = subprocess.run(cmd, env=env, capture_output=True, text=True{timeout_kwarg})
            if result.returncode == 0:
                break
            if retry_count < max_retries:
                print("Step {step_name} split_index=%d failed on attempt %d, retrying..." % (split_index, retry_count))
                if result.stderr:
                    print("STDERR:", result.stderr[-2000:])
                continue
            print("STDOUT:", result.stdout[-4000:])
            print("STDERR:", result.stderr[-4000:])
            raise RuntimeError(
                "Metaflow step {step_name!r} split_index=%d failed (exit %d): %s"
                % (split_index, result.returncode, result.stderr[-500:])
            )
        if result.stdout:
            print("STDOUT:", result.stdout[-2000:])
{foreach_read_code}
    print("Step {step_name} completed all %d foreach items" % foreach_count)
    return {{"run_id": run_id, "step": {step_name!r}, "foreach_count": foreach_count, "status": "success"{foreach_return_extra}}}
'''.format(
            preamble=preamble,
            step_name=step_name,
            step_name_repr=repr(step_name),
            parent_step_repr=repr(parent_step),
            step_cmd_list=step_cmd_list,
            foreach_read_code=foreach_read_code,
            foreach_return_extra=foreach_return_extra,
            timeout_kwarg=timeout_kwarg,
        )

    def _render_nested_foreach_body(
        self,
        step_name: str,
        step_node,
        env_lines: str,
        top_cmd_list: str,
        step_cmd_list: str,
    ) -> str:
        """Render a nested foreach body block (e.g., 'inner' step).

        The parent step is itself a foreach body, so this block iterates
        over all (outer_i, inner_j) combinations.
        """
        parent_step = self._get_parent_step(step_node)
        max_retries = self._get_max_user_code_retries(step_node)
        timeout_seconds = self._get_timeout_seconds(step_node)
        nested_read_code, nested_return_extra = self._get_nested_foreach_extras(step_name, step_node)

        preamble = self._render_block_preamble(
            step_name,
            "Execute nested foreach body step: %s" % step_name,
            env_lines, top_cmd_list, max_retries,
            self._render_upstream_parsing("nested"),
            timeout_seconds=timeout_seconds,
        )

        timeout_kwarg = ", timeout=_timeout" if timeout_seconds else ""

        return '''{preamble}
    # Nested foreach: iterate over all (outer_i, inner_j) combinations
    for outer_i in range(foreach_count):
        inner_count = nested_foreach_counts[outer_i] if outer_i < len(nested_foreach_counts) else 1
        parent_task_id = run_id + "-" + {parent_step_repr} + "-" + str(outer_i)
        for inner_j in range(inner_count):
            task_id = run_id + "-" + {step_name_repr} + "-" + str(outer_i) + "-" + str(inner_j)
            input_paths = run_id + "/" + {parent_step_repr} + "/" + parent_task_id
            for retry_count in range(max_retries + 1):
                step_cmd = {step_cmd_list}
                step_cmd += ["--split-index", str(inner_j)]
                step_cmd += ["--input-paths", input_paths]
                cmd = top_cmd + step_cmd

                print("Running {step_name} outer=%d inner=%d retry=%d (task_id=%s)" % (outer_i, inner_j, retry_count, task_id))
                result = subprocess.run(cmd, env=env, capture_output=True, text=True{timeout_kwarg})
                if result.returncode == 0:
                    break
                if retry_count < max_retries:
                    print("Step {step_name} outer=%d inner=%d failed on attempt %d, retrying..." % (outer_i, inner_j, retry_count))
                    if result.stderr:
                        print("STDERR:", result.stderr[-2000:])
                    continue
                print("STDOUT:", result.stdout[-4000:])
                print("STDERR:", result.stderr[-4000:])
                raise RuntimeError(
                    "Metaflow step {step_name!r} outer=%d inner=%d failed (exit %d): %s"
                    % (outer_i, inner_j, result.returncode, result.stderr[-500:])
                )
            if result.stdout:
                print("STDOUT:", result.stdout[-2000:])
{nested_read_code}
    print("Step {step_name} completed all nested foreach items")
    return {{"run_id": run_id, "step": {step_name!r}, "foreach_count": foreach_count, "nested_foreach_counts": nested_foreach_counts, "status": "success"{nested_return_extra}}}
'''.format(
            preamble=preamble,
            step_name=step_name,
            step_name_repr=repr(step_name),
            parent_step_repr=repr(parent_step),
            step_cmd_list=step_cmd_list,
            nested_read_code=nested_read_code,
            nested_return_extra=nested_return_extra,
            timeout_kwarg=timeout_kwarg,
        )

    def _render_nested_foreach_join(
        self,
        step_name: str,
        step_node,
        env_lines: str,
        top_cmd_list: str,
        step_cmd_list: str,
    ) -> str:
        """Render a join block inside a nested foreach.

        Runs once per outer foreach iteration, gathering all inner body tasks.
        """
        parent_step = self._get_parent_step(step_node)
        max_retries = self._get_max_user_code_retries(step_node)
        timeout_seconds = self._get_timeout_seconds(step_node)

        preamble = self._render_block_preamble(
            step_name,
            "Execute nested foreach join step: %s" % step_name,
            env_lines, top_cmd_list, max_retries,
            self._render_upstream_parsing("nested"),
            timeout_seconds=timeout_seconds,
        )

        timeout_kwarg = ", timeout=_timeout" if timeout_seconds else ""

        return '''{preamble}
    # Nested foreach join: run join once per outer iteration
    for outer_i in range(foreach_count):
        inner_count = nested_foreach_counts[outer_i] if outer_i < len(nested_foreach_counts) else 1
        task_id = run_id + "-" + {step_name_repr} + "-" + str(outer_i)
        input_paths = ",".join(
            run_id + "/" + {parent_step_repr} + "/" + run_id + "-" + {parent_step_repr} + "-" + str(outer_i) + "-" + str(j)
            for j in range(inner_count)
        )
        for retry_count in range(max_retries + 1):
            step_cmd = {step_cmd_list}
            step_cmd += ["--input-paths", input_paths]
            cmd = top_cmd + step_cmd

            print("Running {step_name} outer=%d retry=%d (task_id=%s)" % (outer_i, retry_count, task_id))
            result = subprocess.run(cmd, env=env, capture_output=True, text=True{timeout_kwarg})
            if result.returncode == 0:
                break
            if retry_count < max_retries:
                print("Step {step_name} outer=%d failed on attempt %d, retrying..." % (outer_i, retry_count))
                if result.stderr:
                    print("STDERR:", result.stderr[-2000:])
                continue
            print("STDOUT:", result.stdout[-4000:])
            print("STDERR:", result.stderr[-4000:])
            raise RuntimeError(
                "Metaflow step {step_name!r} outer=%d failed (exit %d): %s"
                % (outer_i, result.returncode, result.stderr[-500:])
            )
        if result.stdout:
            print("STDOUT:", result.stdout[-2000:])

    print("Step {step_name} completed all %d join iterations" % foreach_count)
    return {{"run_id": run_id, "step": {step_name!r}, "foreach_count": foreach_count, "status": "success"}}
'''.format(
            preamble=preamble,
            step_name=step_name,
            step_name_repr=repr(step_name),
            parent_step_repr=repr(parent_step),
            step_cmd_list=step_cmd_list,
            timeout_kwarg=timeout_kwarg,
        )

    def _render_regular_step_block(
        self,
        step_name: str,
        step_node,
        env_lines: str,
        top_cmd_list: str,
        step_cmd_list: str,
    ) -> str:
        """Render a regular (non-foreach-body) step block."""
        input_paths_code = self._build_input_paths_code(step_name, step_node)
        max_retries = self._get_max_user_code_retries(step_node)
        timeout_seconds = self._get_timeout_seconds(step_node)

        is_foreach_step = (step_node.type == "foreach")
        foreach_count_code = ""
        foreach_return_field = ""
        if is_foreach_step:
            foreach_count_code = self._build_foreach_count_reader_single(step_name)
            foreach_return_field = ', "foreach_count": foreach_count'

        preamble = self._render_block_preamble(
            step_name,
            "Execute Metaflow step: %s" % step_name,
            env_lines, top_cmd_list, max_retries,
            self._render_upstream_parsing(),
            timeout_seconds=timeout_seconds,
        )

        timeout_kwarg = ", timeout=_timeout" if timeout_seconds else ""

        return '''{preamble}
    params_task_id = "mage-params"
    task_id = "mage-1"

    # Compute input_paths for this step
{input_paths_code}
    for retry_count in range(max_retries + 1):
        step_cmd = {step_cmd_list}
        if input_paths:
            step_cmd += ["--input-paths", input_paths]
        cmd = top_cmd + step_cmd

        result = subprocess.run(cmd, env=env, capture_output=True, text=True{timeout_kwarg})
        if result.returncode == 0:
            break
        if retry_count < max_retries:
            print("Step {step_name} failed on attempt %d, retrying (%d/%d)..." % (retry_count, retry_count + 1, max_retries))
            if result.stderr:
                print("STDERR:", result.stderr[-2000:])
            continue
        print("STDOUT:", result.stdout[-4000:])
        print("STDERR:", result.stderr[-4000:])
        raise RuntimeError(
            "Metaflow step {step_name!r} failed (exit %d): %s" % (result.returncode, result.stderr[-500:])
        )

    print("Step {step_name} completed successfully")
    if result.stdout:
        print("STDOUT:", result.stdout[-2000:])
{foreach_count_code}
    return {{"run_id": run_id, "step": {step_name!r}, "status": "success"{foreach_return_field}}}
'''.format(
            preamble=preamble,
            step_name=step_name,
            input_paths_code=input_paths_code,
            step_cmd_list=step_cmd_list,
            foreach_count_code=foreach_count_code,
            foreach_return_field=foreach_return_field,
            timeout_kwarg=timeout_kwarg,
        )

    def _render_step_block_content(
        self,
        step_name: str,
        step_node,
        env_lines: str,
    ) -> str:
        """Dispatch to the appropriate block renderer based on step type.

        Step classification:
        1. Nested foreach body: is_inside_foreach, not join, split_parents > 1
        2. Foreach body: is_inside_foreach, not join
        3. Nested foreach join: join with split_parents > 1
        4. Regular step (including foreach initiators and simple joins)
        """
        top_cmd_list, step_cmd_list = self._build_cmd_lists(step_name)
        split_parents = getattr(step_node, "split_parents", [])

        is_foreach_body = (
            getattr(step_node, "is_inside_foreach", False)
            and step_node.type != "join"
        )

        if is_foreach_body:
            if len(split_parents) > 1:
                return self._render_nested_foreach_body(
                    step_name, step_node, env_lines,
                    top_cmd_list, step_cmd_list,
                )
            return self._render_foreach_body_block(
                step_name, step_node, env_lines,
                top_cmd_list, step_cmd_list,
            )

        if step_node.type == "join" and len(split_parents) > 1:
            return self._render_nested_foreach_join(
                step_name, step_node, env_lines,
                top_cmd_list, step_cmd_list,
            )

        return self._render_regular_step_block(
            step_name, step_node, env_lines,
            top_cmd_list, step_cmd_list,
        )

    # ------------------------------------------------------------------
    # Pipeline compilation
    # ------------------------------------------------------------------

    def _block_prefix(self) -> str:
        """Short prefix derived from pipeline UUID for unique block naming."""
        return self.pipeline_uuid[:12]

    def compile(self) -> List[Dict[str, Any]]:
        """Return a list of block dicts describing the Mage pipeline.

        Each dict has: name, type, language, content, upstream_blocks
        """
        blocks = []

        prefix = self._block_prefix()
        init_block_name = "%s%s" % (prefix, self.INIT_BLOCK_SUFFIX)

        env_vars = self._build_env_vars()
        env_lines = self._format_env_lines(env_vars)

        init_content = self._render_init_block_content(env_lines)
        blocks.append(self._make_block_dict(init_block_name, [], init_content))

        for node in self.graph:
            step_name = node.name
            block_name = "%s_s_%s" % (prefix, step_name)

            if step_name == "start":
                upstream = [init_block_name]
            else:
                upstream = ["%s_s_%s" % (prefix, p) for p in node.in_funcs]

            step_specific_env = self._build_step_env_vars(node)
            if step_specific_env:
                step_env_lines = self._format_env_lines({**env_vars, **step_specific_env})
            else:
                step_env_lines = env_lines

            content = self._render_step_block_content(step_name, node, step_env_lines)

            timeout_sec = self._get_timeout_seconds(node)

            blocks.append(self._make_block_dict(
                block_name, upstream, content,
                timeout=timeout_sec,
            ))

        return blocks
