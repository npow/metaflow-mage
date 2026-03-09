# metaflow-mage

[![CI](https://github.com/npow/metaflow-mage/actions/workflows/ux-tests.yml/badge.svg)](https://github.com/npow/metaflow-mage/actions/workflows/ux-tests.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-mage)](https://pypi.org/project/metaflow-mage/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/metaflow-mage)](https://pypi.org/project/metaflow-mage/) [![Docs](https://img.shields.io/badge/docs-mintlify-18a34a?style=flat-square)](https://mintlify.com/npow/metaflow-mage)

Run any Metaflow flow as a Mage pipeline without rewriting your steps.

## The problem

Mage is a modern data pipeline tool with a rich UI and flexible block model, but it has no native support for Metaflow's step graph, artifact store, or retry semantics. Teams that want Mage's notebook-style visibility must either re-implement their Metaflow flows as Mage blocks from scratch or give up Metaflow's lineage, versioning, and `@retry` behavior. There is no ready-made compiler that maps a Metaflow DAG to Mage blocks while keeping the full Metaflow runtime intact.

## Quick start

```bash
pip install metaflow-mage
python flow.py mage create --mage-host http://localhost:6789
python flow.py mage trigger --mage-host http://localhost:6789
# Compiling HelloFlow to Mage pipeline...
# Pipeline hello_flow deployed successfully.
# Pipeline run started: http://localhost:6789/pipelines/hello_flow/runs/1
```

## Install

```bash
pip install metaflow-mage
```

From source:

```bash
git clone https://github.com/npow/metaflow-mage
cd metaflow-mage
pip install -e .
```

## Usage

**Deploy and run in one step:**

```bash
python flow.py mage run \
  --mage-host http://localhost:6789 \
  --mage-project metaflow_project
# Compiling HelloFlow to Mage pipeline...
# Pipeline hello_flow deployed successfully.
# Triggering pipeline run...
# Pipeline run started: http://localhost:6789/pipelines/hello_flow/runs/1
# Pipeline run is running...
# Pipeline run 1 completed successfully.
```

**Deploy once, trigger many times with parameters:**

```bash
python flow.py mage create --mage-host http://localhost:6789
python flow.py mage trigger --mage-host http://localhost:6789 \
  --run-param message=hello --run-param n=10
```

**Programmatic API:**

```python
from metaflow import Deployer

with Deployer("flow.py") as d:
    df = d.mage().create(
        mage_host="http://localhost:6789",
        mage_project="metaflow_project",
    )
    run = df.trigger(message="hello")
    print(run.status)       # RUNNING / SUCCEEDED / FAILED
    print(run.mage_ui)      # http://localhost:6789/pipelines/hello_flow/runs/1
    print(run.run.successful)  # True
```

**Delete a deployed pipeline:**

```bash
python flow.py mage delete --mage-host http://localhost:6789
# Pipeline hello_flow deleted from Mage.
```

## How it works

Each Metaflow step becomes a Mage Python block. An `init` block runs first to allocate the Metaflow run ID; subsequent blocks call `python flow.py step <step_name>` with `--run-id`, `--task-id`, and `--retry-count` threaded through as block outputs. Foreach steps emit a `foreach_init` block that enumerates items and fans out to per-item body blocks. A Mage API trigger (pipeline schedule) is created or reused on every `create`, so `trigger` works immediately without manual Mage UI setup.

Supported graph patterns: linear, branch/join, foreach. `@parallel` is not supported.

## Configuration

| CLI option | Environment variable | Default | Description |
|---|---|---|---|
| `--mage-host` | `MAGE_HOST` | `http://localhost:6789` | Mage server base URL |
| `--mage-project` | `MAGE_PROJECT` | `metaflow_project` | Mage project name |
| `--max-workers` | — | `10` | Max concurrent block runs |
| `--branch` | — | — | `@project` branch name |
| `--production` | — | `false` | Deploy to the production project branch |
| `--name` | — | derived from class name | Override the Mage pipeline UUID |

## Development

```bash
git clone https://github.com/npow/metaflow-mage
cd metaflow-mage
pip install -e ".[dev]"
pytest tests/
```

Integration tests require a running Mage instance:

```bash
docker run -it -p 6789:6789 mageai/mageai mage start metaflow_project
pytest tests/ -m integration
```

## License

Apache 2.0. See [LICENSE](LICENSE).
