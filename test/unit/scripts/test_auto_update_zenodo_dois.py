from pathlib import Path

import pytest
import yaml

from pudl.scripts import update_zenodo_dois


def test_get_latest_record_id_path(mocker):
    """Test get_latest_record_id calls requests.get with expected URL, parses id/doi."""
    mock_resp = mocker.Mock()
    mock_resp.json.return_value = {"id": 999999, "doi": "10.5281/zenodo.999999"}
    mock_resp.raise_for_status = mocker.Mock()

    mock_get = mocker.patch(
        "pudl.scripts.update_zenodo_dois.requests.get", return_value=mock_resp
    )

    latest_id, latest_doi = update_zenodo_dois.get_latest_record_id("123456")

    mock_get.assert_called_once_with(
        "https://zenodo.org/api/records/123456/versions/latest", timeout=10
    )
    assert latest_id == "999999"
    assert latest_doi == "10.5281/zenodo.999999"


@pytest.fixture
def tmp_yaml(tmp_path: Path) -> Path:
    """Create a temporary zenodo_dois.yml file."""
    data = {
        "dataset_a": "10.5281/zenodo.111111",
        "dataset_b": "10.5281/zenodo.222222",
        "dataset_other": "10.5281/zenodo.333333",
    }
    path = tmp_path / "zenodo_dois.yml"
    with path.open("w") as f:
        yaml.dump(data, f)
    return path


def test_updates_to_new_latest_doi(mocker, tmp_yaml: Path, capsys):
    """When latest_id differs, DOI is updated and file rewritten only for new record."""
    # Mock get_latest_record_id to return a newer id for dataset_a
    mocker.patch(
        "pudl.scripts.update_zenodo_dois.get_latest_record_id",
        side_effect=lambda record_id: (
            ("999999", "10.5281/zenodo.999999")
            if record_id == "111111"
            else (record_id, f"10.5281/zenodo.{record_id}")
        ),
    )

    mock_logger = mocker.patch.object(update_zenodo_dois, "logger")

    updates = update_zenodo_dois.update_yaml_dois(tmp_yaml, ("dataset_a", "dataset_b"))

    # Check returned updates dict
    assert updates == {
        "dataset_a": {
            "old_doi": "10.5281/zenodo.111111",
            "new_doi": "10.5281/zenodo.999999",
        }
    }

    # File should be rewritten with updated DOI for dataset_a
    with tmp_yaml.open() as f:
        data_out = yaml.safe_load(f)
    assert data_out["dataset_a"] == "10.5281/zenodo.999999"
    assert data_out["dataset_b"] == "10.5281/zenodo.222222"
    assert data_out["dataset_other"] == "10.5281/zenodo.333333"

    # Logging and stdout message
    mock_logger.info.assert_any_call(
        "dataset_a: Updating DOI from 10.5281/zenodo.111111 to 10.5281/zenodo.999999"
    )
    captured = capsys.readouterr()
    assert "Updated" in captured.out
    assert "zenodo_dois.yml" in captured.out


def test_doi_already_current(mocker, tmp_yaml: Path, capsys):
    """When latest_id == record_id, test log of 'already current' and no rewrite."""
    mocker.patch(
        "pudl.scripts.update_zenodo_dois.get_latest_record_id",
        return_value=("111111", "10.5281/zenodo.111111"),
    )
    mock_logger = mocker.patch.object(update_zenodo_dois, "logger")

    updates = update_zenodo_dois.update_yaml_dois(tmp_yaml, ("dataset_a",))

    # No updates written
    assert updates == {}
    # File unchanged
    with tmp_yaml.open() as f:
        data_out = yaml.safe_load(f)
    assert data_out["dataset_a"] == "10.5281/zenodo.111111"

    mock_logger.info.assert_any_call("dataset_a: DOI already current.")
    captured = capsys.readouterr()
    # No success message printed when no updates
    assert "Updated" not in captured.out


def test_unexpected_latest_id_raises(mocker, tmp_yaml: Path):
    """If latest_id is None or doesn't exist, an AssertionError is raised."""
    mocker.patch(
        "pudl.scripts.update_zenodo_dois.get_latest_record_id",
        return_value=(None, None),
    )

    with pytest.raises(AssertionError):
        update_zenodo_dois.update_yaml_dois(tmp_yaml, ("dataset_a",))
