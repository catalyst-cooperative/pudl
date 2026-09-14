"""Unit tests for the ``hot_path`` DAG-debugging script."""

from types import SimpleNamespace

from click.testing import CliRunner

from pudl.scripts.hot_path import Row, _op_priority, _sort_key, main


def _node(priority: object | None) -> SimpleNamespace:
    tags = {} if priority is None else {"dagster/priority": priority}
    return SimpleNamespace(assets_def=SimpleNamespace(op=SimpleNamespace(tags=tags)))


def test_op_priority_coerces_to_int():
    assert _op_priority(_node(10)) == 10
    assert _op_priority(_node("5")) == 5


def test_op_priority_missing_returns_none():
    assert _op_priority(_node(None)) is None
    assert _op_priority(SimpleNamespace(assets_def=None)) is None


def test_op_priority_handles_op_without_node_def():
    class NoOp:
        @property
        def op(self):
            raise RuntimeError("no node_def")

    assert _op_priority(SimpleNamespace(assets_def=NoOp())) is None


def test_sort_key_orders_by_priority_then_name():
    rows: list[Row] = [
        {"ancestor": "b", "dagster/priority": None},
        {"ancestor": "a", "dagster/priority": None},
        {"ancestor": "z", "dagster/priority": 10},
        {"ancestor": "y", "dagster/priority": 5},
    ]
    ordered = [row["ancestor"] for row in sorted(rows, key=_sort_key)]
    assert ordered == ["z", "y", "a", "b"]


def test_main_reports_unknown_asset():
    result = CliRunner().invoke(main, ["out_ferc714__summarized_demandx"])
    assert result.exit_code != 0
    assert "No asset named" in result.output
    assert "out_ferc714__summarized_demand" in result.output


def test_hot_path_end_to_end():
    from pudl.scripts.hot_path import hot_path

    rows = hot_path("out_ferc714__summarized_demand")
    by_name = {row["ancestor"]: row["dagster/priority"] for row in rows}
    assert by_name["out_ferc714__hourly_planning_area_demand"] == 10
