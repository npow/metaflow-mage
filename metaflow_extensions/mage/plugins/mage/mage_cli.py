"""
Mage CLI commands for Metaflow.

Registered via mfextinit_mage.py as the `mage` CLI group.

Commands
--------
  create   Compile and upload the flow to a running Mage instance.
  trigger  Trigger a run for a previously deployed Mage pipeline.
  run      Compile, deploy, trigger and stream logs from Mage.
  delete   Remove a Mage pipeline deployment.
"""

import hashlib
import json
import os
import sys
import time

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.util import get_username

from .exception import MageException, NotSupportedException
from .mage_compiler import MageCompiler, flow_name_to_pipeline_uuid


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_workflow(flow, graph):
    """Raise MetaflowException / NotSupportedException for unsupported features."""
    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                "Step *%s* uses @parallel, which requires MPI-style collective communication "
                "across N workers that share a barrier at each step. "
                "Mage blocks are independent processes with no shared-memory or inter-block "
                "communication channel during execution, so the collective semantics "
                "(@parallel workers calling current.parallel.* to exchange data) cannot be "
                "reproduced. This is a fundamental architectural mismatch, not a missing "
                "implementation." % node.name
            )


# ---------------------------------------------------------------------------
# Click command group
# ---------------------------------------------------------------------------

@click.group()
def cli():
    pass


@cli.group(help="Commands related to Mage orchestration.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Override the Mage pipeline UUID (default: derived from flow class name).",
)
@click.pass_obj
def mage(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.mage_pipeline_name = name or obj.graph.name


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

@mage.command(help="Compile and deploy this flow to a running Mage instance.")
@click.option(
    "--mage-host",
    default="http://localhost:6789",
    show_default=True,
    envvar="MAGE_HOST",
    help="Mage server base URL.",
)
@click.option(
    "--mage-project",
    default="metaflow_project",
    show_default=True,
    envvar="MAGE_PROJECT",
    help="Mage project name.",
)
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option("--max-workers", default=10, show_default=True, type=int)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def create(
    obj,
    mage_host,
    mage_project,
    tags,
    namespace,
    max_workers,
    with_decorators,
    branch,
    production,
    deployer_attribute_file,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo("Compiling *%s* to Mage pipeline..." % obj.mage_pipeline_name, bold=True)

    compiler = _build_compiler(
        obj, mage_host, mage_project, max_workers, with_decorators,
        branch, production, namespace=namespace, tags=tags,
    )
    blocks = compiler.compile()
    pipeline_uuid = compiler.pipeline_uuid

    client = _make_client(mage_host)
    _deploy_pipeline(client, mage_host, mage_project, pipeline_uuid, blocks, obj)
    schedule_id, schedule_token = _ensure_api_trigger(client, mage_host, pipeline_uuid, obj)

    obj.echo(
        "Pipeline *%s* deployed to Mage at %s" % (pipeline_uuid, mage_host),
        bold=True,
    )

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": pipeline_uuid,
                    "flow_name": obj.flow.name,
                    "metadata": "{}",
                    "additional_info": {
                        "pipeline_uuid": pipeline_uuid,
                        "mage_host": mage_host,
                        "mage_project": mage_project,
                        "schedule_id": schedule_id,
                        "schedule_token": schedule_token,
                        "mf_flow_class": obj.flow.name,
                    },
                },
                f,
            )


# ---------------------------------------------------------------------------
# trigger
# ---------------------------------------------------------------------------

@mage.command(help="Trigger a run for a previously deployed Mage pipeline.")
@click.option("--mage-host", default="http://localhost:6789", show_default=True, envvar="MAGE_HOST")
@click.option("--mage-project", default="metaflow_project", show_default=True, envvar="MAGE_PROJECT")
@click.option(
    "--pipeline-uuid",
    default=None,
    hidden=True,
    help="Mage pipeline UUID to trigger (overrides computed default).",
)
@click.option("--schedule-id", default=None, hidden=True)
@click.option("--schedule-token", default=None, hidden=True)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def trigger(
    obj,
    mage_host,
    mage_project,
    pipeline_uuid,
    schedule_id,
    schedule_token,
    deployer_attribute_file,
    run_params,
):
    if pipeline_uuid is None:
        pipeline_uuid = flow_name_to_pipeline_uuid(obj.mage_pipeline_name)

    # Parse run params
    variables = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        variables[k.strip()] = v.strip()

    # Look up schedule if not provided
    if not schedule_id or not schedule_token:
        client = _make_client(mage_host)
        schedule_id, schedule_token = _ensure_api_trigger(
            client, mage_host, pipeline_uuid, obj
        )

    obj.echo(
        "Triggering Mage pipeline *%s*..." % pipeline_uuid,
        bold=True,
    )

    pipeline_run_id = _trigger_pipeline_run(
        mage_host, schedule_id, schedule_token, variables
    )

    # Compute Metaflow run_id
    run_id = "mage-" + hashlib.md5(str(pipeline_run_id).encode()).hexdigest()[:16]
    pathspec = "%s/%s" % (obj.flow.name, run_id)
    pipeline_run_url = "%s/pipelines/%s/runs/%d" % (mage_host, pipeline_uuid, pipeline_run_id)

    obj.echo("Pipeline run started: *%s*" % pipeline_run_url)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": pipeline_uuid,
                    "pipeline_run_id": pipeline_run_id,
                    "pipeline_run_url": pipeline_run_url,
                    "mage_host": mage_host,
                    "mf_flow_class": obj.flow.name,
                    "metadata": "{}",
                },
                f,
            )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@mage.command(help="Compile, deploy, trigger and wait for a Mage pipeline run.")
@click.option("--mage-host", default="http://localhost:6789", show_default=True, envvar="MAGE_HOST")
@click.option("--mage-project", default="metaflow_project", show_default=True, envvar="MAGE_PROJECT")
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option("--max-workers", default=10, show_default=True, type=int)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for the pipeline run to complete.",
)
@click.pass_obj
def run(
    obj,
    mage_host,
    mage_project,
    tags,
    namespace,
    max_workers,
    with_decorators,
    branch,
    production,
    wait,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo("Compiling *%s* to Mage pipeline..." % obj.mage_pipeline_name, bold=True)

    compiler = _build_compiler(
        obj, mage_host, mage_project, max_workers, with_decorators,
        branch, production, namespace=namespace, tags=tags,
    )
    blocks = compiler.compile()
    pipeline_uuid = compiler.pipeline_uuid

    client = _make_client(mage_host)
    _deploy_pipeline(client, mage_host, mage_project, pipeline_uuid, blocks, obj)
    schedule_id, schedule_token = _ensure_api_trigger(client, mage_host, pipeline_uuid, obj)

    obj.echo("Triggering pipeline run...", bold=True)
    pipeline_run_id = _trigger_pipeline_run(mage_host, schedule_id, schedule_token)

    pipeline_run_url = "%s/pipelines/%s/runs/%d" % (mage_host, pipeline_uuid, pipeline_run_id)
    obj.echo("Pipeline run started: *%s*" % pipeline_run_url)

    if wait:
        obj.echo("Waiting for pipeline run to complete...")
        final_status = _wait_for_pipeline_run(mage_host, pipeline_run_id, obj)
        if final_status == "completed":
            obj.echo("Pipeline run *%d* completed successfully." % pipeline_run_id, bold=True)
        else:
            raise MageException(
                "Pipeline run %d finished with status: %s\nURL: %s"
                % (pipeline_run_id, final_status, pipeline_run_url)
            )
    else:
        obj.echo("Pipeline run ID: %d" % pipeline_run_id)
        obj.echo("Track it at: %s" % pipeline_run_url)


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

@mage.command(help="Delete a deployed Mage pipeline.")
@click.option("--mage-host", default="http://localhost:6789", show_default=True, envvar="MAGE_HOST")
@click.option("--mage-project", default="metaflow_project", show_default=True, envvar="MAGE_PROJECT")
@click.option("--pipeline-uuid", default=None, help="Mage pipeline UUID to delete.")
@click.pass_obj
def delete(obj, mage_host, mage_project, pipeline_uuid):
    if pipeline_uuid is None:
        pipeline_uuid = flow_name_to_pipeline_uuid(obj.mage_pipeline_name)

    try:
        import requests
        resp = requests.delete(
            "%s/api/pipelines/%s" % (mage_host, pipeline_uuid),
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code in (200, 204):
            obj.echo("Pipeline *%s* deleted from Mage." % pipeline_uuid, bold=True)
        else:
            obj.echo("Warning: DELETE returned HTTP %d: %s" % (resp.status_code, resp.text[:200]))
    except Exception as exc:
        raise MageException("Failed to delete pipeline: %s" % exc) from exc


# ---------------------------------------------------------------------------
# Mage API helpers
# ---------------------------------------------------------------------------

def _build_compiler(
    obj,
    mage_host,
    mage_project,
    max_workers,
    with_decorators,
    branch,
    production,
    namespace=None,
    tags=(),
) -> MageCompiler:
    """Construct a MageCompiler from a Metaflow CLI obj and shared options."""
    effective_branch = branch
    if effective_branch is None and not production:
        try:
            from metaflow import current as _current
            bn = getattr(_current, "branch_name", None)
            if bn and not bn.startswith("user.") and not bn.startswith("prod"):
                if bn.startswith("test."):
                    effective_branch = bn[len("test."):]
                else:
                    effective_branch = bn
        except Exception:
            pass

    return MageCompiler(
        name=obj.mage_pipeline_name,
        graph=obj.graph,
        flow=obj.flow,
        flow_file=os.path.abspath(sys.argv[0]),
        metadata=obj.metadata,
        flow_datastore=obj.flow_datastore,
        environment=obj.environment,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        tags=list(tags),
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        with_decorators=list(with_decorators),
        branch=effective_branch,
        production=production,
        mage_host=mage_host,
        mage_project=mage_project,
    )


def _make_client(host: str):
    """Return a requests.Session configured for the given Mage instance."""
    try:
        import requests
    except ImportError:
        raise MageException(
            "The `requests` package is required for deploy/run commands. "
            "Install it with: pip install requests"
        )
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session._mage_host = host
    return session


def _pipeline_exists(client, host, pipeline_uuid) -> bool:
    """Return True if the pipeline already exists in Mage."""
    resp = client.get("%s/api/pipelines/%s" % (host, pipeline_uuid))
    if resp.status_code != 200:
        return False
    data = resp.json()
    # Mage returns HTTP 200 with {"error": {...}} when pipeline doesn't exist
    if "error" in data:
        return False
    return "pipeline" in data


def _deploy_pipeline(client, mage_host, mage_project, pipeline_uuid, blocks, obj):
    """Create or update a Mage pipeline with the given blocks."""
    host = client._mage_host

    # Check if pipeline already exists (Mage always returns HTTP 200, even for errors)
    if _pipeline_exists(client, host, pipeline_uuid):
        # Update existing pipeline - delete old blocks first, then recreate
        obj.echo("Updating existing pipeline *%s*..." % pipeline_uuid)
        resp = client.get("%s/api/pipelines/%s" % (host, pipeline_uuid))
        existing_data = resp.json().get("pipeline", {})
        existing_blocks = existing_data.get("blocks", [])
        for blk in existing_blocks:
            client.delete(
                "%s/api/pipelines/%s/blocks/%s" % (host, pipeline_uuid, blk["uuid"])
            )
    else:
        # Create new pipeline
        obj.echo("Creating new pipeline *%s*..." % pipeline_uuid)
        create_resp = client.post(
            "%s/api/pipelines" % host,
            json={"pipeline": {"name": pipeline_uuid, "type": "python"}},
        )
        if create_resp.status_code not in (200, 201) or "error" in create_resp.json():
            err_text = create_resp.json().get("error", {}).get("message", create_resp.text[:300])
            raise MageException(
                "Failed to create Mage pipeline (HTTP %d): %s"
                % (create_resp.status_code, err_text)
            )

    # Phase 1: Create all blocks WITHOUT upstream_blocks.
    # Mage silently drops upstream_blocks if the referenced block doesn't exist yet,
    # so we must set dependencies in a second phase after all blocks are created.
    for block in blocks:
        block_resp = client.post(
            "%s/api/pipelines/%s/blocks" % (host, pipeline_uuid),
            json={
                "block": {
                    "name": block["name"],
                    "type": block["type"],
                    "language": block["language"],
                    "content": block["content"],
                }
            },
        )
        if block_resp.status_code not in (200, 201):
            raise MageException(
                "Failed to create block %r (HTTP %d): %s"
                % (block["name"], block_resp.status_code, block_resp.text[:300])
            )
        obj.echo("  Created block *%s*" % block["name"])

    # Phase 2: Update content and set upstream_blocks for each block.
    # Block files are SHARED in Mage (stored in custom/ by name), so even when we
    # delete+recreate a block, the file on disk may not be updated unless we
    # explicitly PUT the new content.
    for block in blocks:
        update_payload = {}
        if block["upstream_blocks"]:
            update_payload["upstream_blocks"] = block["upstream_blocks"]
        # Always update content to ensure re-deployments pick up changes
        update_payload["content"] = block["content"]
        if block.get("timeout"):
            update_payload["timeout"] = block["timeout"]
        update_resp = client.put(
            "%s/api/pipelines/%s/blocks/%s" % (host, pipeline_uuid, block["name"]),
            json={"block": update_payload},
        )
        if update_resp.status_code not in (200, 201):
            raise MageException(
                "Failed to update block %r (HTTP %d): %s"
                % (block["name"], update_resp.status_code, update_resp.text[:300])
            )
        if block["upstream_blocks"]:
            obj.echo("  Set upstream for *%s* -> %s" % (block["name"], block["upstream_blocks"]))
        else:
            obj.echo("  Updated block *%s*" % block["name"])

    obj.echo("Pipeline *%s* deployed successfully." % pipeline_uuid)


def _ensure_api_trigger(client, mage_host, pipeline_uuid, obj):
    """Ensure there's an active API trigger (pipeline_schedule) for the pipeline.

    Returns (schedule_id, schedule_token).
    """
    host = client._mage_host

    # List existing schedules
    resp = client.get(
        "%s/api/pipelines/%s/pipeline_schedules" % (host, pipeline_uuid)
    )

    if resp.status_code == 200 and "error" not in resp.json():
        schedules = resp.json().get("pipeline_schedules", [])
        # Look for existing API trigger
        for sched in schedules:
            if sched.get("schedule_type") == "api" and sched.get("status") == "active":
                # Return schedule_id as str (Mage API returns int, but CLI expects str)
                return str(sched["id"]), sched["token"]

    # Create a new API trigger
    create_resp = client.post(
        "%s/api/pipelines/%s/pipeline_schedules" % (host, pipeline_uuid),
        json={
            "pipeline_schedule": {
                "name": "metaflow_api_trigger",
                "schedule_type": "api",
                "start_time": "2024-01-01T00:00:00.000000",
                "status": "active",
            }
        },
    )
    resp_data = create_resp.json()
    if create_resp.status_code not in (200, 201) or "error" in resp_data:
        err_text = resp_data.get("error", {}).get("message", create_resp.text[:300])
        raise MageException(
            "Failed to create API trigger for pipeline %r (HTTP %d): %s"
            % (pipeline_uuid, create_resp.status_code, err_text)
        )

    sched_data = resp_data.get("pipeline_schedule", {})
    # Return schedule_id as str (Mage API returns int, but CLI expects str)
    return str(sched_data["id"]), sched_data["token"]


def _trigger_pipeline_run(mage_host, schedule_id, schedule_token, variables=None):
    """Trigger a Mage pipeline run and return the pipeline_run_id."""
    try:
        import requests
    except ImportError:
        raise MageException("The `requests` package is required.")

    url = "%s/api/pipeline_schedules/%s/pipeline_runs/%s" % (
        mage_host, schedule_id, schedule_token
    )
    payload = {}
    if variables:
        payload = {"pipeline_run": {"variables": variables}}

    resp = requests.post(url, json=payload)
    if resp.status_code not in (200, 201):
        raise MageException(
            "Failed to trigger pipeline run (HTTP %d): %s"
            % (resp.status_code, resp.text[:500])
        )

    return resp.json()["pipeline_run"]["id"]


def _wait_for_pipeline_run(mage_host, pipeline_run_id, obj, poll_interval=5):
    """Poll until the pipeline run reaches a terminal state and return the status."""
    terminal_statuses = {"completed", "failed", "cancelled", "canceled"}
    url = "%s/api/pipeline_runs/%d" % (mage_host, pipeline_run_id)

    try:
        import requests
    except ImportError:
        raise MageException("The `requests` package is required.")

    seen_running = False
    while True:
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("pipeline_run", {}).get("status", "unknown")
                if not seen_running and status == "running":
                    obj.echo("Pipeline run is running...")
                    seen_running = True
                if status in terminal_statuses:
                    return status
            else:
                obj.echo("Warning: could not poll run status (HTTP %d)" % resp.status_code)
        except Exception as exc:
            obj.echo("Warning: error polling run: %s" % exc)

        time.sleep(poll_interval)
