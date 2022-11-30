"""Test Dagster Resources."""
import pytest
from dagster import build_init_resource_context

from pudl.settings import dataset_settings


def test_invalid_years():
    """Test we are replacing the data without dropping the table schema."""
    init_context = build_init_resource_context(config={"eia_years": [1990]})
    with pytest.raises(ValueError):
        _ = dataset_settings(init_context)


# TODO (bendnorman): Add tests for default values
