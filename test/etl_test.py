"""
PyTest based testing of the FERC Database & PUDL data package initializations.

This module also contains fixtures for returning connections to the databases.
These connections can be either to the live databases for post-ETL testing or
to new temporary databases, which are created from scratch and dropped after
the tests have completed. See the --live_ferc1_db and --live_pudl_db command
line options by running pytest --help. If you are using live databases, you
will need to tell PUDL where to find them with --pudl_in=<PUDL_IN>.

"""
import logging
import pathlib

import pytest
import yaml

import pudl
from pudl.convert.epacems_to_parquet import epacems_to_parquet
from pudl.extract.ferc1 import get_dbc_map, get_fields

logger = logging.getLogger(__name__)


@pytest.mark.datapkg
def test_datapkg_bundle(datapkg_bundle):
    """Generate limited packages for testing."""
    pass


@pytest.mark.datapkg
def test_pudl_engine(pudl_engine):
    """Try creating a pudl_engine...."""
    pass


def test_ferc1_etl(ferc1_engine):
    """
    Create a fresh FERC Form 1 SQLite DB and attempt to access it.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the ferc1_engine fixture defined in conftest.py
    """
    pass


def test_epacems_to_parquet(datapkg_bundle,
                            pudl_settings_fixture,
                            data_scope,
                            request):
    """Attempt to convert a small amount of EPA CEMS data to parquet format."""
    clobber = request.config.getoption("--clobber")
    epacems_datapkg_json = pathlib.Path(
        pudl_settings_fixture['datapkg_dir'],
        data_scope['datapkg_bundle_name'],
        'epacems-eia-test',
        "datapackage.json"
    )
    logger.info(f"Loading epacems from {epacems_datapkg_json}")
    epacems_to_parquet(
        datapkg_path=epacems_datapkg_json,
        epacems_years=data_scope['epacems_years'],
        epacems_states=data_scope['epacems_states'],
        out_dir=pathlib.Path(pudl_settings_fixture['parquet_dir'], 'epacems'),
        compression='snappy',
        clobber=clobber
    )


def test_ferc1_lost_data(pudl_settings_fixture, data_scope):
    """
    Check to make sure we aren't missing any old FERC Form 1 tables or fields.

    Exhaustively enumerate all historical sets of FERC Form 1 database tables
    and their constituent fields. Check to make sure that the current database
    definition, based on the given reference year and our compilation of the
    DBF filename to table name mapping from 2015, includes every single table
    and field that appears in the historical FERC Form 1 data.
    """
    refyear = max(data_scope['ferc1_years'])
    ds = pudl.extract.ferc1.Ferc1Datastore(
        pathlib.Path(pudl_settings_fixture["pudl_in"]),
        sandbox=True)
    current_dbc_map = pudl.extract.ferc1.get_dbc_map(ds, year=refyear)
    current_tables = list(current_dbc_map.keys())
    logger.info(f"Checking for new, unrecognized FERC1 "
                f"tables in {refyear}.")
    for table in current_tables:
        # First make sure there are new tables in refyear:
        if table not in pudl.constants.ferc1_tbl2dbf:
            raise AssertionError(
                f"New FERC Form 1 table '{table}' in {refyear} "
                f"does not exist in 2015 list of tables"
            )
    # Get all historical table collections...
    dbc_maps = {}
    for yr in data_scope['ferc1_years']:
        logger.info(f"Searching for lost FERC1 tables and fields in {yr}.")
        dbc_maps[yr] = pudl.extract.ferc1.get_dbc_map(ds, year=yr)
        old_tables = list(dbc_maps[yr].keys())
        for table in old_tables:
            # Check to make sure there aren't any lost archaic tables:
            if table not in current_tables:
                raise AssertionError(
                    f"Long lost FERC1 table: '{table}' found in year {yr}. "
                    f"Refyear: {refyear}"
                )
            for field in dbc_maps[yr][table].values():
                if field not in current_dbc_map[table].values():
                    raise AssertionError(
                        f"Long lost FERC1 field '{field}' found in table "
                        f"'{table}' from year {yr}. "
                        f"Refyear: {refyear}"
                    )


def test_ferc1_solo_etl(pudl_settings_fixture,
                        ferc1_engine,
                        live_ferc1_db):
    """Verify that a minimal FERC Form 1 can be loaded without other data."""
    with open(pathlib.Path(
            pathlib.Path(__file__).parent,
            'settings', 'ferc1-solo.yml'), "r") as f:
        datapkg_settings = yaml.safe_load(f)['datapkg_bundle_settings']

    pudl.etl.generate_datapkg_bundle(
        datapkg_settings,
        pudl_settings_fixture,
        datapkg_bundle_name='ferc1-solo',
        clobber=True)


class TestFerc1Datastore:
    """Validate the Ferc1 Datastore and integration functions."""

    def test_ferc_folder(self, pudl_ferc1datastore_fixture):
        """Spot check we get correct folder names per dataset year."""
        ds = pudl_ferc1datastore_fixture

        assert ds.get_folder(1994) == "FORMSADMIN/FORM1/working"
        assert ds.get_folder(2001) == "UPLOADERS/FORM1/working"
        assert ds.get_folder(2002) == "FORMSADMIN/FORM1/working"
        assert ds.get_folder(2010) == "UPLOADERS/FORM1/working"
        assert ds.get_folder(2015) == "UPLOADERS/FORM1/working"

    def test_get_fields(self, pudl_ferc1datastore_fixture):
        """Check that the get fields table works as expected."""
        ds = pudl_ferc1datastore_fixture

        expect_path = pathlib.Path(__file__).parent / \
            "data/ferc1/f1_2018/get_fields.json"

        with expect_path.open() as f:
            expect = yaml.safe_load(f)

        data = ds.get_file(2018, "F1_PUB.DBC")
        result = get_fields(data)
        assert result == expect

    def test_sample_get_dbc_map(self, pudl_ferc1datastore_fixture):
        """Test sample_get_dbc_map."""
        ds = pudl_ferc1datastore_fixture

        table = get_dbc_map(ds, 2018)
        assert table["f1_429_trans_aff"] == {
            "ACCT_CORC": "acct_corc",
            "ACCT_CORC_": "acct_corc_f",
            "AMT_CORC": "amt_corc",
            "AMT_CORC_F": "amt_corc_f",
            "DESC_GOOD2": "desc_good_serv_f",
            "DESC_GOOD_": "desc_good_serv",
            "NAME_COMP": "name_comp",
            "NAME_COMP_": "name_comp_f",
            "REPORT_PRD": "report_prd",
            "REPORT_YEA": "report_year",
            "RESPONDENT": "respondent_id",
            "ROW_NUMBER": "row_number",
            "ROW_PRVLG": "row_prvlg",
            "ROW_SEQ": "row_seq",
            "SPPLMNT_NU": "spplmnt_num"
        }


class TestExcelExtractor:
    """Verify that we can lead excel files as provided via the datastore."""

    def test_excel_filename_eia860(self, pudl_datastore_fixture):
        """Spot check eia860 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        assert extractor.excel_filename(
            2011, "boiler_generator_assn") == "EnviroAssocY2011.xlsx"
        assert extractor.excel_filename(
            2016, "generator_retired") == "3_1_Generator_Y2016.xlsx"
        assert extractor.excel_filename(
            2018, "utility") == "1___Utility_Y2018.xlsx"

    def test_excel_filename_eia923(self, pudl_datastore_fixture):
        """Spot check eia923 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        assert extractor.excel_filename(2009, "plant_frame") == \
            "EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS"
        assert extractor.excel_filename(2019, "energy_storage") == \
            "EIA923_Schedules_2_3_4_5_M_11_2019_21JAN2020.xlsx"
        assert extractor.excel_filename(2012, "puerto_rico") == \
            "EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx"

    def test_extract_eia860(self, pudl_datastore_fixture):
        """Spot check extraction of eia860 excel files."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        assert "Ownership" in extractor.load_excel_file(
            2018, "ownership").sheet_names

    def test_extract_eia923(self, pudl_datastore_fixture):
        """Spot check extraction eia923 excel files."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        assert "Page 3 Boiler Fuel Data" in extractor.load_excel_file(
            2018, "stocks").sheet_names


class TestEpaCemsDatastore:
    """Ensure we can extract csv files from the datastore."""

    # datastore = EpaCemsDatastore(sandbox=True)

    def test_get_csv(self, pudl_epacemsdatastore_fixture):
        """Spot check opening of epacems csv file from datastore."""
        head = b'"STATE","F'

        csv = pudl_epacemsdatastore_fixture.open_csv("ny", 1999, 6)
        assert csv.read()[:10] == head
