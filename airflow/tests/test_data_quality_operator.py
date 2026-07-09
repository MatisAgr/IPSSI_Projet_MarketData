"""Tests unitaires du Custom Operator DataQualityOperator : la DB est mockée
(PostgresHook.get_first), aucun Postgres réel n'est nécessaire."""
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException

from lib.data_quality import DataQualityOperator


def make_operator(rules):
    return DataQualityOperator(task_id="dq_test", conn_id="postgres_dwh", rules=rules)


@patch("lib.data_quality.PostgresHook")
def test_gt_rule_passes(mock_hook_cls):
    mock_hook_cls.return_value.get_first.return_value = (5,)
    op = make_operator([{"name": "has_rows", "op": "gt", "sql": "SELECT COUNT(*) FROM x"}])
    op.execute(context={})  # ne doit pas lever


@patch("lib.data_quality.PostgresHook")
def test_eq_rule_default_expects_zero(mock_hook_cls):
    mock_hook_cls.return_value.get_first.return_value = (0,)
    op = make_operator([{"name": "no_nulls", "sql": "SELECT COUNT(*) FROM x WHERE y IS NULL"}])
    op.execute(context={})


@patch("lib.data_quality.PostgresHook")
def test_gt_rule_violation_raises(mock_hook_cls):
    mock_hook_cls.return_value.get_first.return_value = (0,)
    op = make_operator([{"name": "has_rows", "op": "gt", "sql": "SELECT COUNT(*) FROM x"}])
    with pytest.raises(AirflowException, match="has_rows"):
        op.execute(context={})


@patch("lib.data_quality.PostgresHook")
def test_eq_rule_violation_raises(mock_hook_cls):
    mock_hook_cls.return_value.get_first.return_value = (3,)
    op = make_operator([{"name": "no_dupes", "sql": "SELECT COUNT(*) - COUNT(DISTINCT id) FROM x"}])
    with pytest.raises(AirflowException, match="no_dupes"):
        op.execute(context={})


@patch("lib.data_quality.PostgresHook")
def test_multiple_rules_all_failures_reported(mock_hook_cls):
    mock_hook_cls.return_value.get_first.side_effect = [(0,), (2,)]
    op = make_operator([
        {"name": "rule_a", "op": "gt", "sql": "SELECT 1"},
        {"name": "rule_b", "sql": "SELECT 2"},
    ])
    with pytest.raises(AirflowException) as exc_info:
        op.execute(context={})
    assert "rule_a" in str(exc_info.value)
    assert "rule_b" in str(exc_info.value)


@patch("lib.data_quality.PostgresHook")
def test_uses_configured_conn_id(mock_hook_cls):
    mock_hook_cls.return_value.get_first.return_value = (0,)
    make_operator([{"name": "ok", "sql": "SELECT 0"}]).execute(context={})
    mock_hook_cls.assert_called_once_with(postgres_conn_id="postgres_dwh")
