"""Custom Operator de data quality : règles SQL scalaires configurables."""
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseOperator


class DataQualityOperator(BaseOperator):
    """Chaque règle = {"name", "sql", "op" ("eq"|"gt"), "expected"}.
    Le SQL doit renvoyer un scalaire ; toute règle violée fait échouer la tâche."""

    template_fields = ("rules",)
    ui_color = "#ffebee"

    def __init__(self, *, conn_id: str, rules: list[dict], **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.rules = rules

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        failures = []
        for rule in self.rules:
            value = hook.get_first(rule["sql"])[0]
            expected = rule.get("expected", 0)
            ok = value == expected if rule.get("op", "eq") == "eq" else value > expected
            if ok:
                self.log.info("DQ OK   : %s (valeur=%s)", rule["name"], value)
            else:
                failures.append(f"{rule['name']} (valeur={value}, attendu {rule.get('op', 'eq')} {expected})")
                self.log.error("DQ FAIL : %s", failures[-1])
        if failures:
            raise AirflowException(f"{len(failures)} règle(s) de qualité violée(s) : {failures}")
