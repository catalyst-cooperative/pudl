"""Separate DBT config for dagster.

This helps us avoid import issues with putting it in __init__.py.
"""

from dagster_dbt import DbtProject

from pudl.workspace.setup import PUDL_ROOT_DIR

dbt_project = DbtProject(project_dir=PUDL_ROOT_DIR / "dbt")
