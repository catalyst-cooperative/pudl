"""Tests for metadata not covered elsewhere."""
from pudl.metadata import RESOURCE_METADATA, build_package
from pudl.metadata.helpers import format_errors


def test_all_resources_valid() -> None:
    """All resources in metadata pass validation tests."""
    _ = build_package()


def test_all_excluded_resources_exist() -> None:
    """All resources excluded from foreign key rules exist."""
    errors = []
    for name, meta in RESOURCE_METADATA.items():
        rule = meta.get("schema", {}).get("foreign_key_rules")
        if rule:
            missing = [
                x for x in rule.get("exclude", [])
                if x not in RESOURCE_METADATA
            ]
            if missing:
                errors.append(f"{name}: {missing}")
    if errors:
        raise ValueError(
            format_errors(
                *errors, title="Invalid resources in foreign_key_rules.exclude"
            )
        )
