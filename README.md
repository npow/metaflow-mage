# metaflow-mage

[![UX Tests](https://github.com/npow/metaflow-mage/actions/workflows/ux-tests.yml/badge.svg)](https://github.com/npow/metaflow-mage/actions/workflows/ux-tests.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-mage)](https://pypi.org/project/metaflow-mage/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Deploy and run Metaflow flows as [Mage](https://docs.mage.ai) pipelines.

`metaflow-mage` compiles any Metaflow flow into a Mage pipeline, letting you schedule, deploy,
and monitor your pipelines through Mage while keeping all your existing Metaflow code unchanged.

## How it works

Each Metaflow step becomes a Mage [custom block](https://docs.mage.ai/custom-blocks/overview).
An `init` block runs first to create the Metaflow run and compute a stable `run_id` from the
Mage pipeline run ID. Then each step block runs the step as a subprocess
(`python flow.py step <step_name> --run-id ...`), passing artifacts through the Metaflow
datastore rather than Mage block outputs. Block dependencies mirror Metaflow step dependencies.

## Prerequisites

A running Mage instance. The quickest way is Docker:

```bash
docker run -d -p 6789:6789 \
  -v "$HOME:$HOME" \
  -e REQUIRE_USER_AUTHENTICATION=0 \
  mageai/mageai:latest mage start metaflow_project
```

The volume mount (`-v "$HOME:$HOME"`) is required so that Mage can execute your flow file at
its absolute path and share the local Metaflow datastore with the test runner.

## Install

```bash
pip install metaflow-mage
```

Or from source:

```bash
git clone https://github.com/npow/metaflow-mage.git
cd metaflow-mage
pip install -e ".[dev]"
```

## Quick start

```bash
# Compile and deploy to Mage, trigger a run, and wait for completion
python my_flow.py mage run --mage-host http://localhost:6789

# Just deploy (no run)
python my_flow.py mage create --mage-host http://localhost:6789

# Trigger a previously deployed pipeline
python my_flow.py mage trigger --mage-host http://localhost:6789

# Remove a pipeline from Mage
python my_flow.py mage delete --mage-host http://localhost:6789
```

## Configuration reference

All options can be set via CLI flags or environment variables.

| Option | Env var | Default | Description |
|---|---|---|---|
| `--mage-host` | `MAGE_HOST` | `http://localhost:6789` | Mage server base URL |
| `--mage-project` | `MAGE_PROJECT` | `metaflow_project` | Mage project name |
| `--max-workers` | | `10` | Max concurrent block runs |
| `--tag` | | | Tag the Metaflow run (repeatable) |
| `--namespace` | | | Metaflow namespace |
| `--with` | | | Apply step decorators (e.g. `--with retry`) |
| `--branch` | | | `@project` branch name |
| `--production` | | | Deploy as production branch |

## Deployer API

`metaflow-mage` implements the standard [Metaflow Deployer API](https://docs.metaflow.org/production/scheduling-metaflow-flows/introduction), so you can manage deployments programmatically:

```python
from metaflow import Deployer

with Deployer("my_flow.py").mage(mage_host="http://localhost:6789") as d:
    deployed = d.create()
    run = deployed.run(greeting="hello")
    print(run.status)        # RUNNING / SUCCEEDED / FAILED
    print(run.mage_ui)       # link to the Mage pipeline run UI
    print(run.run)           # metaflow.Run object with full artifact access
```

Recovering an existing deployment:

```python
from metaflow.plugins.mage import MageDeployedFlow

deployed = MageDeployedFlow.from_deployment("MyFlow")
run = deployed.run()
```

## Supported capabilities

| Feature | Supported |
|---|---|
| Linear flows | Yes |
| Static branching (split/join) | Yes |
| foreach fan-out (single level) | Yes |
| Nested foreach | Yes |
| `@retry` | Yes |
| `@timeout` | Yes |
| `@catch` | Yes |
| `@resources` | Yes |
| `@project` (branch/production) | Yes |
| `@config` / `config_expr` | Yes |
| `Parameter` | Yes |
| `@schedule` | Via Mage pipeline schedule (API trigger created automatically) |
| `@parallel` (MPI-style) | Not supported (see below) |

## Known limitations

### `@parallel` is not supported

`@parallel` in Metaflow implements MPI-style collective communication: N workers all run the
same step simultaneously and can exchange data through `current.parallel.*` (rank, num_workers,
barrier). This requires all N workers to share a communication channel during step execution.

Mage blocks are independent processes. There is no mechanism for blocks executing in the same
pipeline run to communicate during execution. The collective semantics cannot be reproduced.
This is a fundamental architectural mismatch, not a missing implementation.

If you need MPI-style parallelism, consider [metaflow-kestra](https://github.com/npow/metaflow-kestra)
(which also does not support `@parallel`) or a Metaflow-native scheduler such as
[AWS Batch](https://docs.metaflow.org/scaling/remote-tasks/aws-batch) or
[Kubernetes](https://docs.metaflow.org/scaling/remote-tasks/kubernetes) with `@parallel`.

## GHA setup

The CI workflow (`.github/workflows/ux-tests.yml`) starts a fresh Mage Docker container for
each deployer test, installs the extension inside the container, and runs the Metaflow UX test
suite against it. No secrets are required for the UX tests. A `CODECOV_TOKEN` secret enables
coverage upload.

See [.github/workflows/ux-tests.yml](.github/workflows/ux-tests.yml) for the full setup.

## License

Apache 2.0. See [LICENSE](LICENSE).
