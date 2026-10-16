"""Synthetic load-test project.

Generates NUM_PIPELINES independent chains of ASSETS_PER_PIPELINE assets
each (no shared state, no real compute), every asset carrying an
AutomationCondition. The point isn't the data -- it's giving the
Automations tab and the auto-materialize sensor daemon a realistically
large, varied catalog to evaluate, so behavior at scale can be reproduced
and measured outside of a live customer deployment.

Tune NUM_PIPELINES / ASSETS_PER_PIPELINE to scale the catalog up or down;
NUM_PIPELINES * ASSETS_PER_PIPELINE is the total asset count.
"""

import dagster as dg

NUM_PIPELINES = 3000
ASSETS_PER_PIPELINE = 10

# Rotated across pipelines so the daemon sees a realistic mix of condition
# types, not 30k copies of the same one.
_CONDITIONS = [
    dg.AutomationCondition.eager(),
    dg.AutomationCondition.on_missing(),
    dg.AutomationCondition.on_cron("0 * * * *"),
    dg.AutomationCondition.on_cron("0 9 * * *"),
    dg.AutomationCondition.any_downstream_conditions(),
]


def _make_asset(
    key: dg.AssetKey,
    deps: list[dg.AssetKey],
    group_name: str,
    condition: dg.AutomationCondition,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=key,
        deps=deps,
        group_name=group_name,
        automation_condition=condition,
        kinds={"synthetic"},
    )
    def _asset_fn(context: dg.AssetExecutionContext) -> bool:
        return True

    return _asset_fn


def _build_pipeline_assets(pipeline_index: int) -> list[dg.AssetsDefinition]:
    group_name = f"pipeline_{pipeline_index:05d}"
    condition = _CONDITIONS[pipeline_index % len(_CONDITIONS)]
    assets: list[dg.AssetsDefinition] = []
    upstream_key: dg.AssetKey | None = None
    for step in range(ASSETS_PER_PIPELINE):
        key = dg.AssetKey([group_name, f"step_{step:02d}"])
        deps = [upstream_key] if upstream_key else []
        assets.append(_make_asset(key, deps, group_name, condition))
        upstream_key = key
    return assets


all_assets = [
    asset
    for pipeline_index in range(NUM_PIPELINES)
    for asset in _build_pipeline_assets(pipeline_index)
]

defs = dg.Definitions(
    assets=all_assets,
    resources={"io_manager": dg.mem_io_manager},
)
