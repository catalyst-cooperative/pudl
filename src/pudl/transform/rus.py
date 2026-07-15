"""Code for transforming RUS data that pertains to more than one RUS Form."""

from enum import StrEnum, auto

import pandas as pd
from dagster import AssetIn, AssetsDefinition, asset

import pudl.helpers as helpers
import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def early_check_pk(
    df: pd.DataFrame,
    pk_early: list[str] = ["report_date", "borrower_id_rus"],
    raise_fail=True,
) -> None:
    """Check the expected primary key of the table.

    By default the expected primary key is ["report_date", "borrower_id_rus"].
    """
    dupes = df[df.duplicated(subset=pk_early, keep=False)]
    if not dupes.empty:
        message = f"Found early indication of {len(dupes)} not good bad bad duplicates:\n{dupes}"
        if raise_fail:
            raise AssertionError(message)
        logger.warning(message)
    return


def early_transform(
    raw_df: pd.DataFrame, boolean_columns_to_fix=[], string_cols_to_simplify=[]
) -> pd.DataFrame:
    """Standard transforms for raw RUS data."""
    df = (
        helpers.standardize_na_values(raw_df)
        # the report_month column is seemingly always 12 so this will default to Dec.
        .pipe(helpers.convert_to_date)
        .pipe(
            helpers.fix_boolean_columns,
            boolean_columns_to_fix=boolean_columns_to_fix,
        )
        .pipe(helpers.simplify_strings, ["borrower_name_rus"] + string_cols_to_simplify)
        .assign(
            borrower_name_rus=lambda x: x.borrower_name_rus.str.replace(
                ",", ""
            ).str.replace(".", "")
        )
    )
    return df


def convert_units(
    df: pd.DataFrame, old_unit: str, new_unit: str | None, converter: float | int
) -> pd.DataFrame:
    """Convert units within a column and rename column with new units.

    This function assumes that the old units are suffixes in the snake-cased column
    names, separated by an underscore.

    Ex: if you want to convert from kWh's to MWh's the df must have column names like
    ``electric_sales_kwh`` or ``purchased_kwh``, the old unit would be ``kwh``, the new
    unit would be ``mwh`` and the converter would be ``0.001``.

    Args:
        df: data table with units you'd like to convert.
        old_unit: the unit in the df. This must be the suffix of the column
            names you'd like to convert.
        new_unit: the new unit label you want as the new suffix of the resulting
            dataframe. If you want no new unit added, this value can be None or an
            empty string ()"").
        converter: the float or integer you need to multiply the old values by to
            convert the units.
    """
    convert_cols = df.filter(regex=rf"_{old_unit}$").columns
    df.loc[:, convert_cols] = df.loc[:, convert_cols].astype("float") * converter
    new_unit_to_add = ""
    if new_unit:
        new_unit_to_add = f"_{new_unit}"
    df = df.rename(
        columns={
            col_name: col_name.removesuffix(f"_{old_unit}") + new_unit_to_add
            for col_name in convert_cols
        }
    )
    return df


class RusEntity(StrEnum):
    """Enum for the different types of RUS entities."""

    BORROWERS = auto()


def finished_rus_asset_factory(
    table_name: str, _core_table_name: str, io_manager_key: str | None = None
) -> AssetsDefinition:
    """An asset factory for finished RUS tables.

    Args:
        table_name: the name of the core table.
        _core_table_name: the name of the unharvested input table
        io_manager_key: the name of the IO Manager of the final asset.

    Returns:
        A RUS asset.
    """

    @asset(
        ins={_core_table_name: AssetIn()},
        name=table_name,
        io_manager_key=io_manager_key,
    )
    def finished_rus_asset(**kwargs) -> pd.DataFrame:
        """Convert RUS _core table to core - the io manager will handle the schema."""
        return kwargs[_core_table_name]

    return finished_rus_asset
