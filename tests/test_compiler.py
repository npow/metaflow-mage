"""Unit tests for MageCompiler — no Mage instance required."""

import ast
import json
import os
import sys
import types
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metaflow_extensions.mage.plugins.mage.mage_compiler import (
    MageCompiler,
    flow_name_to_pipeline_uuid,
)


# ---------------------------------------------------------------------------
# Minimal mock objects
# ---------------------------------------------------------------------------

def _make_step_node(name, step_type="linear", is_inside_foreach=False, in_funcs=None, split_parents=None, decorators=None):
    """Return a mock Metaflow graph node (used for the graph list)."""
    node = types.SimpleNamespace(
        name=name,
        type=step_type,
        is_inside_foreach=is_inside_foreach,
        in_funcs=set(in_funcs or []),
        split_parents=split_parents or [],
        parallel_foreach=False,
        decorators=decorators or [],
    )
    return node


def _make_deco(name, attributes):
    return types.SimpleNamespace(name=name, attributes=attributes)


def _make_flow(parameters=None, decorators=None, flow_state=None, step_objs=None):
    """Return a minimal mock flow object.

    step_objs: list of objects with a .name attribute representing flow steps
    (used by _find_step_obj to resolve decorators like @timeout and @retry).
    """
    _steps = step_objs or []
    _parameters = parameters or {}

    class MockFlow:
        name = "TestFlow"
        _flow_decorators = decorators or {}
        _flow_state = flow_state or {}

        def __iter__(self):
            return iter(_steps)

        def _get_parameters(self):
            for var, param in _parameters.items():
                yield var, param

    return MockFlow()


def _make_step_obj(name, decorators=None):
    """Return a mock Step object (for flow iteration / _find_step_obj lookup)."""
    return types.SimpleNamespace(name=name, decorators=decorators or [])


def _make_compiler(
    name="TestFlow",
    steps=None,
    parameters=None,
    mage_host="http://localhost:6789",
    mage_project="metaflow_project",
    environment_type="local",
    step_objs=None,
    max_workers=10,
):
    """Return a MageCompiler with mock dependencies.

    steps: list of graph nodes (SimpleNamespace with graph node fields)
    step_objs: list of step objects returned when iterating the flow
               (used for decorator lookup via _find_step_obj)
    """
    graph = steps or []
    flow = _make_flow(parameters=parameters, step_objs=step_objs)
    metadata = types.SimpleNamespace(TYPE="local")
    flow_datastore = types.SimpleNamespace(TYPE="local", datastore_root="/tmp/.metaflow")
    environment = types.SimpleNamespace(TYPE=environment_type)
    event_logger = types.SimpleNamespace()
    monitor = types.SimpleNamespace()

    return MageCompiler(
        name=name,
        graph=graph,
        flow=flow,
        flow_file="/tmp/test_flow.py",
        metadata=metadata,
        flow_datastore=flow_datastore,
        environment=environment,
        event_logger=event_logger,
        monitor=monitor,
        mage_host=mage_host,
        mage_project=mage_project,
        max_workers=max_workers,
    )


# ---------------------------------------------------------------------------
# Tests: flow_name_to_pipeline_uuid
# ---------------------------------------------------------------------------

class TestFlowNameToPipelineUuid:
    def test_lowercase(self):
        assert flow_name_to_pipeline_uuid("HelloFlow") == "helloflow"

    def test_dash_to_underscore(self):
        assert flow_name_to_pipeline_uuid("My-Flow") == "my_flow"

    def test_dot_to_underscore(self):
        assert flow_name_to_pipeline_uuid("My.Flow") == "my_flow"

    def test_collision(self):
        """My-Flow, My.Flow, My_Flow all map to my_flow — document the collision."""
        assert (
            flow_name_to_pipeline_uuid("My-Flow")
            == flow_name_to_pipeline_uuid("My.Flow")
            == flow_name_to_pipeline_uuid("My_Flow")
        )


# ---------------------------------------------------------------------------
# Tests: _get_timeout_seconds
#
# _get_timeout_seconds(node) looks up the step via _find_step_obj which
# iterates self.flow to find a step whose .name matches node.name.
# So we must supply matching step_objs in the flow, not just decorators
# on the graph node.
# ---------------------------------------------------------------------------

class TestGetTimeoutSeconds:
    def _compiler_with_timeout(self, **timeout_attrs):
        """Build a compiler whose 'start' step has @timeout(**timeout_attrs)."""
        attrs = dict(seconds=0, minutes=0, hours=0)
        attrs.update(timeout_attrs)
        deco = _make_deco("timeout", attrs)
        step_obj = _make_step_obj("start", decorators=[deco])
        node = _make_step_node("start")
        return _make_compiler(steps=[node], step_objs=[step_obj]), node

    def test_seconds_only(self):
        c, node = self._compiler_with_timeout(seconds=30)
        assert c._get_timeout_seconds(node) == 30

    def test_minutes_only(self):
        """D-TIMEOUT-1: @timeout(minutes=5) should return 300."""
        c, node = self._compiler_with_timeout(minutes=5)
        assert c._get_timeout_seconds(node) == 300

    def test_hours_only(self):
        c, node = self._compiler_with_timeout(hours=1)
        assert c._get_timeout_seconds(node) == 3600

    def test_mixed(self):
        c, node = self._compiler_with_timeout(hours=1, minutes=30, seconds=45)
        assert c._get_timeout_seconds(node) == 5445

    def test_minutes_and_seconds(self):
        c, node = self._compiler_with_timeout(minutes=5, seconds=30)
        assert c._get_timeout_seconds(node) == 330

    def test_all_zero_returns_none(self):
        c, node = self._compiler_with_timeout()
        assert c._get_timeout_seconds(node) is None

    def test_no_timeout_deco_returns_none(self):
        step_obj = _make_step_obj("start", decorators=[])
        node = _make_step_node("start")
        c = _make_compiler(steps=[node], step_objs=[step_obj])
        assert c._get_timeout_seconds(node) is None

    def test_step_not_in_flow_returns_none(self):
        """If _find_step_obj returns None, timeout must be None."""
        node = _make_step_node("orphan")
        c = _make_compiler(steps=[node], step_objs=[])
        assert c._get_timeout_seconds(node) is None


# ---------------------------------------------------------------------------
# Tests: _build_cmd_lists — --environment flag
# ---------------------------------------------------------------------------

class TestBuildCmdLists:
    def test_environment_flag_present(self):
        """D-ENV-1: --environment must appear in the top-level command."""
        c = _make_compiler(environment_type="local")
        top_cmd, step_cmd = c._build_cmd_lists("start")
        assert '"--environment"' in top_cmd, "--environment flag missing from top command"

    def test_environment_type_in_cmd(self):
        """D-ENV-1: the environment type value must be in the top command."""
        c = _make_compiler(environment_type="conda")
        top_cmd, step_cmd = c._build_cmd_lists("start")
        assert "'conda'" in top_cmd or '"conda"' in top_cmd, "environment type 'conda' not in top command"

    def test_step_cmd_has_run_id(self):
        top_cmd, step_cmd = _make_compiler()._build_cmd_lists("start")
        assert '"--run-id"' in step_cmd

    def test_step_cmd_has_retry_count(self):
        top_cmd, step_cmd = _make_compiler()._build_cmd_lists("start")
        assert '"--retry-count"' in step_cmd


# ---------------------------------------------------------------------------
# Tests: _block_prefix — no truncation
# ---------------------------------------------------------------------------

class TestBlockPrefix:
    def test_full_uuid_used(self):
        """D-BLOCK-1: prefix must be the full pipeline_uuid, not truncated.

        NOTE: This test currently FAILS because _block_prefix() returns
        pipeline_uuid[:12]. It documents the expected behavior after the
        D-BLOCK-1 fix is applied.
        """
        c = _make_compiler(name="LongFlowNameThatExceeds12Chars")
        uuid = flow_name_to_pipeline_uuid("LongFlowNameThatExceeds12Chars")
        assert c._block_prefix() == uuid

    def test_project_flows_differ(self):
        """D-BLOCK-1: two different flows must not share a prefix."""
        c1 = _make_compiler(name="TrainFlow")
        c2 = _make_compiler(name="EvalFlow")
        assert c1._block_prefix() != c2._block_prefix()


# ---------------------------------------------------------------------------
# Tests: _get_parameters — _MAGE_INTERNAL_KEYS collision detection
# ---------------------------------------------------------------------------

class TestParameterCollisionDetection:
    def test_reserved_name_raises(self):
        """D-PARAM-1: Parameter named 'retry' should raise at compile time."""
        flow = _make_flow(parameters={"retry": types.SimpleNamespace(IS_CONFIG_PARAMETER=False, kwargs={})})
        compiler = _make_compiler()
        compiler.flow = flow
        with pytest.raises((ValueError, Exception), match="(?i)reserv|conflict|internal"):
            compiler._get_parameters()

    def test_env_reserved_name_raises(self):
        flow = _make_flow(parameters={"env": types.SimpleNamespace(IS_CONFIG_PARAMETER=False, kwargs={})})
        compiler = _make_compiler()
        compiler.flow = flow
        with pytest.raises((ValueError, Exception)):
            compiler._get_parameters()

    def test_normal_name_ok(self):
        flow = _make_flow(parameters={"message": types.SimpleNamespace(IS_CONFIG_PARAMETER=False, kwargs={"default": "hi"})})
        compiler = _make_compiler()
        compiler.flow = flow
        params = compiler._get_parameters()
        assert "message" in params


# ---------------------------------------------------------------------------
# Tests: generated block code does NOT contain credentials
# ---------------------------------------------------------------------------

class TestNoCredentialsInGeneratedCode:
    def test_aws_secret_not_in_block_content(self, monkeypatch):
        """D-CRED-1: AWS_SECRET_ACCESS_KEY must not appear as a literal in generated code."""
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "TESTSECRET12345")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")

        start_node = _make_step_node("start", in_funcs=[])
        end_node = _make_step_node("end", in_funcs=["start"])
        c = _make_compiler(steps=[start_node, end_node])
        blocks = c.compile()

        for block in blocks:
            content = block["content"]
            assert "TESTSECRET12345" not in content, (
                "AWS_SECRET_ACCESS_KEY value found in block %r content" % block["name"]
            )
            assert "AKIAIOSFODNN7EXAMPLE" not in content, (
                "AWS_ACCESS_KEY_ID value found in block %r content" % block["name"]
            )

    def test_metaflow_service_auth_not_in_block(self, monkeypatch):
        """D-CRED-1: METAFLOW_SERVICE_AUTH_KEY must not appear as a literal."""
        monkeypatch.setenv("METAFLOW_SERVICE_AUTH_KEY", "supersecrettoken")

        start_node = _make_step_node("start", in_funcs=[])
        c = _make_compiler(steps=[start_node])
        blocks = c.compile()

        for block in blocks:
            assert "supersecrettoken" not in block["content"], (
                "METAFLOW_SERVICE_AUTH_KEY value found in block %r" % block["name"]
            )


# ---------------------------------------------------------------------------
# Tests: generated block code is syntactically valid Python
# ---------------------------------------------------------------------------

class TestGeneratedCodeSyntax:
    def test_init_block_syntax(self):
        c = _make_compiler()
        env_lines = c._format_env_lines({"METAFLOW_DEFAULT_METADATA": "local"})
        code = c._render_init_block_content(env_lines)
        ast.parse(code)  # raises SyntaxError if invalid

    def test_regular_step_block_syntax(self):
        node = _make_step_node("start", in_funcs=[])
        c = _make_compiler(steps=[node])
        env_lines = c._format_env_lines({})
        code = c._render_step_block_content("start", node, env_lines)
        ast.parse(code)

    def test_foreach_body_block_syntax(self):
        node = _make_step_node("process", is_inside_foreach=True, in_funcs=["start"], split_parents=["start"])
        c = _make_compiler(steps=[node])
        env_lines = c._format_env_lines({})
        code = c._render_step_block_content("process", node, env_lines)
        ast.parse(code)

    def test_step_with_timeout_syntax(self):
        """Block code generated for a step with @timeout must be syntactically valid."""
        deco = _make_deco("timeout", {"seconds": 0, "minutes": 5, "hours": 0})
        step_obj = _make_step_obj("work", decorators=[deco])
        node = _make_step_node("work", in_funcs=["start"])
        c = _make_compiler(steps=[node], step_objs=[step_obj])
        env_lines = c._format_env_lines({})
        code = c._render_step_block_content("work", node, env_lines)
        ast.parse(code)

    def test_step_with_retry_syntax(self):
        """Block code generated for a step with @retry must be syntactically valid."""
        deco = _make_deco("retry", {"times": 3})
        step_obj = _make_step_obj("flaky", decorators=[deco])
        node = _make_step_node("flaky", in_funcs=["start"])
        c = _make_compiler(steps=[node], step_objs=[step_obj])
        env_lines = c._format_env_lines({})
        code = c._render_step_block_content("flaky", node, env_lines)
        ast.parse(code)


# ---------------------------------------------------------------------------
# Tests: generated parallel foreach code structure
# ---------------------------------------------------------------------------

class TestParallelForeachCodeStructure:
    """Structural assertions on the generated ThreadPoolExecutor foreach code."""

    def _foreach_body_code(self, timeout_seconds=None, max_workers=10):
        """Generate a foreach body block and return its code string."""
        decos = []
        if timeout_seconds:
            decos.append(_make_deco("timeout", {"seconds": timeout_seconds, "minutes": 0, "hours": 0}))
        node = _make_step_node("process", is_inside_foreach=True, in_funcs=["start"], split_parents=["start"], decorators=decos)
        step_obj = _make_step_obj("process", decorators=decos)
        c = _make_compiler(steps=[node], step_objs=[step_obj], max_workers=max_workers)
        env_lines = c._format_env_lines({})
        return c._render_step_block_content("process", node, env_lines)

    def test_threadpoolexecutor_present(self):
        """Generated foreach body must use ThreadPoolExecutor."""
        code = self._foreach_body_code()
        assert "ThreadPoolExecutor" in code, "ThreadPoolExecutor missing from foreach body block"

    def test_as_completed_present(self):
        """Generated foreach body must use as_completed to collect all exceptions."""
        code = self._foreach_body_code()
        assert "as_completed" in code, "as_completed missing — only first exception would propagate"

    def test_max_workers_embedded(self):
        """max_workers value must be embedded in generated code."""
        code = self._foreach_body_code(max_workers=7)
        assert "max_workers=7" in code, "max_workers=7 not found in generated foreach body"

    def test_cancel_futures_present(self):
        """shutdown(cancel_futures=True) must be in generated code to avoid draining pending futures."""
        code = self._foreach_body_code()
        assert "cancel_futures=True" in code, "cancel_futures=True missing — pool drains on failure"

    def test_errors_collected_before_raise(self):
        """All split exceptions must be collected before raising (not raise-on-first)."""
        code = self._foreach_body_code()
        assert "_errors" in code, "_errors list missing — only first exception propagates"
        assert "_errors.append" in code, "_errors.append missing"

    def test_timeout_uses_deadline_not_static(self):
        """When @timeout is set, subprocess.run must use _deadline remainder, not static _timeout."""
        code = self._foreach_body_code(timeout_seconds=60)
        assert "_deadline" in code, "_deadline missing from preamble"
        assert "timeout=max(0.1, _deadline - time.monotonic())" in code, (
            "subprocess.run uses static timeout=_timeout instead of deadline-aware remaining time"
        )

    def test_timeout_expired_handler_present(self):
        """subprocess.TimeoutExpired must be caught to kill the child process."""
        code = self._foreach_body_code(timeout_seconds=30)
        assert "subprocess.TimeoutExpired" in code, "TimeoutExpired handler missing"
        assert "_te.process.kill()" in code, "child process kill missing on timeout"

    def test_foreach_count_zero_guard(self):
        """foreach_count=0 must return early, not submit zero futures silently."""
        code = self._foreach_body_code()
        assert "foreach_count == 0" in code or "not _task_pairs" in code, (
            "No guard for foreach_count=0 — block returns false success on empty foreach"
        )

    def test_syntax_with_max_workers(self):
        """Generated code must be syntactically valid with custom max_workers."""
        import ast
        code = self._foreach_body_code(max_workers=5)
        ast.parse(code)

    def test_syntax_with_timeout_and_retry(self):
        """Generated code must be syntactically valid with both @timeout and @retry."""
        import ast
        timeout_deco = _make_deco("timeout", {"seconds": 30, "minutes": 0, "hours": 0})
        retry_deco = _make_deco("retry", {"times": 2})
        node = _make_step_node("flaky", is_inside_foreach=True, in_funcs=["start"],
                               split_parents=["start"], decorators=[timeout_deco, retry_deco])
        step_obj = _make_step_obj("flaky", decorators=[timeout_deco, retry_deco])
        c = _make_compiler(steps=[node], step_objs=[step_obj])
        env_lines = c._format_env_lines({})
        code = c._render_step_block_content("flaky", node, env_lines)
        ast.parse(code)


# ---------------------------------------------------------------------------
# Tests: generated init block correctness
# ---------------------------------------------------------------------------

class TestGeneratedInitBlock:
    def _init_block(self):
        c = _make_compiler()
        env_lines = c._format_env_lines({"METAFLOW_DEFAULT_METADATA": "local"})
        return c._render_init_block_content(env_lines)

    def test_pipeline_run_id_uses_is_none_check(self):
        """D-INIT-FALSY: must use `is None` not falsy check to allow integer 0."""
        code = self._init_block()
        assert "pipeline_run_id is None" in code, (
            "Init block uses `if not pipeline_run_id` which incorrectly raises for "
            "integer 0. Should use `if pipeline_run_id is None`."
        )
        # Negative: ensure the falsy pattern is NOT present
        assert "if not pipeline_run_id" not in code, (
            "`if not pipeline_run_id` found — this fires for integer 0, a valid Mage run ID"
        )

    def test_init_block_syntax(self):
        import ast
        ast.parse(self._init_block())
