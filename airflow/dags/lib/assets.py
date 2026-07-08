"""Assets partagés entre DAGs + résolution des dates à traiter."""
from airflow.sdk import Asset

RAW_ORDERS = Asset("raw_orders")
DWH_ORDERS = Asset("dwh_orders")


def resolve_dts(triggering_asset_events=None, ds=None, ti=None):
    """Dates portées par les évènements d'Asset déclencheurs (peut y en avoir
    plusieurs en cas de backfill), sinon la date logique du run."""
    dts = sorted({
        d
        for events in (triggering_asset_events or {}).values()
        for e in events
        for d in (e.extra or {}).get("dts", [])
    }) or [ds]
    # Version SQL "('d1','d2')" pour les règles templatées du DataQualityOperator
    ti.xcom_push("dts_sql", "('" + "','".join(dts) + "')")
    return dts
