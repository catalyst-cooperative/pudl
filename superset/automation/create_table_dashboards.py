"""Script for creating Superset dashboards of PUDL tables."""

import json
import logging
import os
from pathlib import Path

import click
import coloredlogs
import requests
from jinja2 import Template

logger = logging.getLogger(__name__)


class SupersetClient:
    """A client for interacting with the Superset API."""

    def __init__(self, base_url: str = "https://superset.catalyst.coop") -> None:
        """Initialize the Superset client."""
        self.base_url = base_url
        self.session = self._init_session()

    def _get_access_token(self, session: requests.Session) -> str:
        """Get the access token for the Superset API.

        Note: I'm not sure how to authenticate using OAuth.

        Args:
            session: The requests session to use.

        Returns:
            The access token.
        """
        payload = {
            "username": os.environ["SUPERSET_USERNAME"],
            "password": os.environ["SUPERSET_PASSWORD"],
            "provider": "db",
            "refresh": True,
        }
        r = session.post(self.base_url + "/api/v1/security/login", json=payload)

        access_token = r.json()["access_token"]
        return access_token

    def _init_session(self) -> requests.Session:
        """Initialize a requests session with the necessary headers."""
        session = requests.Session()
        session.headers["Authorization"] = "Bearer " + self._get_access_token(session)
        session.headers["Content-Type"] = "application/json"

        csrf_url = f"{self.base_url}/api/v1/security/csrf_token/"
        csrf_res = session.get(csrf_url)
        csrf_token = csrf_res.json()["result"]

        session.headers["Referer"] = csrf_url
        session.headers["X-CSRFToken"] = csrf_token
        return session

    def create_dataset(self, dataset_name, sql=None, database_id=1) -> int:
        """Create a new dataset in Superset.

        Args:
            dataset_name: The name of the dataset.
            sql: The SQL query to generate the dataset.
            database_id: The ID of the database to use. The current default is the PUDL database id.

        Returns:
            The ID of the created dataset.
        """
        data = {
            "database": database_id,
            "schema": "pudl.main",
        }
        data["table_name"] = dataset_name
        if sql:
            data["sql"] = sql

        r = self.session.post(self.base_url + "/api/v1/dataset/", json=data)

        if r.status_code != 201:
            raise ValueError(f"{r.status_code}: {r.content}")
        return r.json()["id"]

    def create_table_chart(self, dataset_id: int, table_name: str) -> int:
        """Create a table chart in Superset.

        Args:
            dataset_id: The ID of the dataset to create the chart for.
            table_name: The name of the table to create the chart for.

        Returns:
            The ID of the created chart.
        """
        file_path = Path(__file__).parent / "templates/charts/table_download.json"

        # Open the file and load the data
        with file_path.open() as file:
            params = json.load(file)

        # Get all the columns from the dataset because there is no "select all" option
        r = self.session.get(self.base_url + f"/api/v1/dataset/{dataset_id}")
        columns = [col["column_name"] for col in r.json()["result"]["columns"]]
        params["all_columns"] = columns

        data = {
            "datasource_id": dataset_id,
            "slice_name": table_name,
            "datasource_type": "table",
            "viz_type": "table",
            "params": json.dumps(params),
        }

        r = self.session.post(self.base_url + "/api/v1/chart/", json=data)
        if r.status_code != 201:
            raise ValueError(f"{r.status_code}: {r.content}")
        return r.json()["id"]

    def create_row_count_chart(self, dataset_id: int, table_name: str) -> int:
        """Create a table row count chart in Superset.

        Args:
            dataset_id: The ID of the dataset to create the chart for.
            table_name: The name of the table to create the chart for.

        Returns:
            The ID of the created chart.
        """
        file_path = Path(__file__).parent / "templates/charts/row_count.json"

        # Open the file and load the data
        with file_path.open() as file:
            params = json.load(file)

        data = {
            "datasource_id": dataset_id,
            "slice_name": f"{table_name} Row Count",
            "datasource_type": "table",
            "viz_type": "big_number_total",
            "params": json.dumps(params),
        }

        r = self.session.post(self.base_url + "/api/v1/chart/", json=data)
        if r.status_code != 201:
            raise ValueError(f"{r.status_code}: {r.content}")
        return r.json()["id"]

    def create_dashboard(
        self,
        table_name: str,
        table_dataset_id: int,
        table_chart_id: int,
        data_dictionary_chart_id: int,
        row_count_chart_id: int,
    ):
        """Create a the PUDL table dashboard.

        Args:
            table_name: The name of the table to create the dashboard for.
            table_dataset_id: The ID of the dataset for the table.
            table_chart_id: The ID of the chart for the table.
            data_dictionary_chart_id: The ID of the chart for the data dictionary.
            row_count_chart_id: The ID of the chart for the row

        """
        # Load JSON template from file
        file_path = (
            Path(__file__).parent / "templates/dashboards/table_download_position.json"
        )
        with file_path.open() as file:
            json_template_str = file.read()

        # Create a jinja2 template object
        template = Template(json_template_str)

        # Define the values to be substituted
        data = {
            "table_chart_id": table_chart_id,
            "data_dictionary_chart_id": data_dictionary_chart_id,
            "row_count_chart_id": row_count_chart_id,
            "table_name": table_name,
            "table_dataset_id": table_dataset_id,
        }

        # Render the template with the actual values
        rendered_json_str = template.render(data)

        # create the dashboard
        data = {
            "dashboard_title": table_name,
            "position_json": rendered_json_str,
            "slug": table_name,
        }
        r = self.session.post(self.base_url + "/api/v1/dashboard/", json=data)
        if r.status_code != 201:
            raise ValueError(f"{r.status_code}: {r.content}")

        dash_id = r.json()["id"]

        # add all the dashboard id to the charts
        # If you don't do this the dashboard layout will be correct but superset won't be able to find the charts
        data = {"dashboards": [dash_id]}
        for chart_id in (table_chart_id, data_dictionary_chart_id, row_count_chart_id):
            r = self.session.put(self.base_url + f"/api/v1/chart/{chart_id}", json=data)
            if r.status_code != 200:
                raise ValueError(f"{r.status_code}: {r.content}")

    def create_all_table_dashboard_assets(self, table_name: str):
        """Create all the assets needed for a PUDL table dashboard.

        Args:
            table_name: The name of the table to create the dashboard for.
        """
        table_dataset_id = self.create_dataset(table_name)

        # create the data dictionary dataset
        sql = f"SELECT column_name, data_type, comment AS description FROM duckdb_columns() where table_name = '{table_name}';"  # noqa: S608
        data_dict_dataset_id = self.create_dataset(
            table_name + " Column Descriptions", sql=sql
        )

        # create the table chart
        table_chart_id = self.create_table_chart(table_dataset_id, table_name)
        # create the data dictionary table chart
        data_dictionary_chart_id = self.create_table_chart(
            data_dict_dataset_id, f"{table_name} Column Descriptions"
        )  # TODO: add searchable option to the table!
        row_count_chart_id = self.create_row_count_chart(table_dataset_id, table_name)

        # create the dashboard
        self.create_dashboard(
            table_name,
            table_dataset_id,
            table_chart_id,
            data_dictionary_chart_id,
            row_count_chart_id,
        )


@click.command()
@click.argument("table_names", nargs=-1)
@click.option(
    "--loglevel",
    help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
    default="INFO",
)
def create_table_dashboards(loglevel, table_names):
    """This command accepts a variable number of arguments."""
    superset_logger = logging.getLogger()
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=superset_logger)

    if not table_names:
        raise click.UsageError("At least one argument is required.")

    client = SupersetClient()
    for table_name in table_names:
        logger.info(f"Creating dashboard for {table_name}")
        client.create_all_table_dashboard_assets(table_name)
        logger.info(f"Dashboard for {table_name} created successfully.")


if __name__ == "__main__":
    create_table_dashboards()
