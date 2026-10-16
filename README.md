# automation-load-test

A synthetic Dagster project for load-testing the **Automations** tab (and
the declarative-automation / `AssetDaemon` sensor that drives it) at scale.
There's no real data here — every asset is a trivial no-op. The only thing
that matters is the shape of the catalog: a large number of independent
pipelines, each asset carrying an `AutomationCondition`.

## What it generates

`src/automation_load_test/definitions.py` builds `NUM_PIPELINES` independent
chains of `ASSETS_PER_PIPELINE` assets each (defaults: 3,000 x 10 = 30,000
assets). Each pipeline is a simple linear dependency chain
(`step_00 -> step_01 -> ... -> step_09`), grouped under its own
`group_name`, with one `AutomationCondition` applied to every asset in the
pipeline. The condition is rotated across five presets (`eager`,
`on_missing`, two different `on_cron` schedules, `any_downstream_conditions`)
so the daemon sees a realistic mix rather than 30k copies of one condition.

Nothing is persisted between runs — the IO manager is in-memory
(`dg.mem_io_manager`) so materializing doesn't write 30k files to disk.

To scale the catalog up or down, edit the two constants at the top of
`definitions.py`:

```python
NUM_PIPELINES = 3000
ASSETS_PER_PIPELINE = 10
```

## Running locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
dagster dev -m automation_load_test.definitions
```

Building all 30,000 `AssetsDefinition` objects takes a few seconds on
import; the webserver and daemon load the full catalog on startup.

## Deploying to Dagster+

`dagster_cloud.yaml` at the repo root defines one code location
(`automation_load_test`) pointing at `automation_load_test.definitions`.
Point a Dagster+ Serverless or Hybrid deployment at this repo and it will
pick that up directly — no further configuration needed.
