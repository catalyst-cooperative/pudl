"""Transform Census PEP FIPS codes data."""

from dagster import asset


@asset
def _core_censuspep__yearly_geocodes(raw_censuspep__geocodes):
    """Create a County-level table with FIPS codes."""
    df = raw_censuspep__geocodes
    return df
