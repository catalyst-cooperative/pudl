"""Unit tests for the pudl.transform.eia860m module."""

from types import SimpleNamespace

import pandas as pd

import pudl.transform.eia860m as eia860m


def test_core_eia860m__changelog_generators_includes_puerto_rico(mocker):
    """All three Puerto Rico generator inputs are included in the changelog."""
    fields = [
        SimpleNamespace(name=column)
        for column in [
            "plant_id_eia",
            "generator_id",
            "report_date",
            "source_table",
            "sector_id_eia",
        ]
    ]
    mocker.patch.object(
        eia860m.Resource,
        "from_id",
        return_value=SimpleNamespace(schema=SimpleNamespace(fields=fields)),
    )
    mocker.patch.object(eia860m, "expand_timeseries", side_effect=lambda df, **_: df)
    mocker.patch.object(eia860m, "make_changelog", side_effect=lambda df, _: df)

    def fake_core_eia860__generators(
        raw_eia860__generator_proposed,
        raw_eia860__generator_existing,
        raw_eia860__generator_retired,
        raw_eia860__generator,
    ):
        return pd.concat(
            [
                raw_eia860__generator_proposed,
                raw_eia860__generator_existing,
                raw_eia860__generator_retired,
                raw_eia860__generator,
            ],
            ignore_index=True,
            sort=True,
        )

    mocker.patch.object(
        eia860m,
        "_core_eia860__generators",
        side_effect=fake_core_eia860__generators,
    )

    def make_raw(source_table, plant_id):
        return pd.DataFrame(
            {
                "plant_id_eia": [plant_id],
                "generator_id": ["1"],
                "report_date": [pd.Timestamp("2025-01-01")],
                "source_table": [source_table],
                "sector_name_eia": ["Electric Utility"],
            }
        )

    changelog = eia860m.core_eia860m__changelog_generators.node_def.compute_fn.decorated_fn(
        raw_eia860m__generator_proposed=make_raw("proposed", 1),
        raw_eia860m__puerto_rico_generator_proposed=make_raw(
            "puerto_rico_proposed", 2
        ),
        raw_eia860m__generator_existing=make_raw("existing", 3),
        raw_eia860m__puerto_rico_generator_existing=make_raw(
            "puerto_rico_existing", 4
        ),
        raw_eia860m__generator_retired=make_raw("retired", 5),
        raw_eia860m__puerto_rico_generator_retired=make_raw(
            "puerto_rico_retired", 6
        ),
    )

    assert set(changelog.source_table) == {
        "proposed",
        "puerto_rico_proposed",
        "existing",
        "puerto_rico_existing",
        "retired",
        "puerto_rico_retired",
    }
