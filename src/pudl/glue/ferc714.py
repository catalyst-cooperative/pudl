"""Extract and transform glue tables between FERC Form 714's CSV and XBRL raw sources."""

import importlib.resources

import pandas as pd

import pudl

logger = pudl.logging_helpers.get_logger(__name__)

RESP_ID_FERC_MAP_CSV = (
    importlib.resources.files("pudl.package_data.glue") / "respondent_id_ferc714.csv"
)
"""Path to the PUDL ID mapping sheet with the plant map."""


def get_respondent_map_ferc714() -> pd.DataFrame:
    """Read in the manual CSV to XBRL FERC714 respondent mapping data."""
    return pd.read_csv(RESP_ID_FERC_MAP_CSV).convert_dtypes()


def glue() -> dict[str : pd.DataFrame]:
    """Make the FERC 714 glue tables out of stored CSVs of association tables.

    This function was mirrored off of ferc1_eia.glue, but is much more
    paired down.
    """
    respondent_map = get_respondent_map_ferc714()

    respondents_pudl_ids = (
        respondent_map.loc[:, ["respondent_id_ferc714"]]
        .drop_duplicates("respondent_id_ferc714")
        .dropna(subset=["respondent_id_ferc714"])
    )
    respondents_csv_ids = (
        respondent_map.loc[:, ["respondent_id_ferc714", "respondent_id_ferc714_csv"]]
        .drop_duplicates("respondent_id_ferc714_csv")
        .dropna(subset=["respondent_id_ferc714_csv"])
    )
    respondents_xbrl_ids = (
        respondent_map.loc[:, ["respondent_id_ferc714", "respondent_id_ferc714_xbrl"]]
        .drop_duplicates("respondent_id_ferc714_xbrl")
        .dropna(subset=["respondent_id_ferc714_xbrl"])
    )

    glue_dfs = {
        "core_pudl__assn_ferc714_pudl_respondents": respondents_pudl_ids,
        "core_pudl__assn_ferc714_csv_pudl_respondents": respondents_csv_ids,
        "core_pudl__assn_ferc714_xbrl_pudl_respondents": respondents_xbrl_ids,
    }

    return glue_dfs
