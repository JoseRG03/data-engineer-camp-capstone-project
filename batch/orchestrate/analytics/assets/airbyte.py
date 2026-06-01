import dagster as dg
from dagster import AssetKey, AssetSpec, AutomationCondition
from dagster_airbyte import (
    AirbyteCloudWorkspace,
    AirbyteConnectionTableProps,
    DagsterAirbyteTranslator,
    build_airbyte_assets_definitions,
)


class CustomDagsterAirbyteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props: AirbyteConnectionTableProps) -> AssetSpec:
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            group_name="airbyte_assets",
            key=AssetKey(["EARTHQUAKES", props.table_name.upper()]),
            automation_condition=AutomationCondition.on_cron("0 7 * * *")
        )

airbyte_workspace = AirbyteCloudWorkspace(
    workspace_id=dg.EnvVar("AIRBYTE_CLOUD_WORKSPACE_ID"),
    client_id=dg.EnvVar("AIRBYTE_CLOUD_CLIENT_ID"),
    client_secret=dg.EnvVar("AIRBYTE_CLOUD_CLIENT_SECRET"),
)

earthquakes_airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomDagsterAirbyteTranslator(),
    connection_selector_fn=lambda connection: connection.name in ["Postgres RAW → DWH Raw"]
)
