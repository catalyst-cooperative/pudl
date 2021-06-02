"""
PyTest based testing of the FERC Database & PUDL data package initializations.

This module also contains fixtures for returning connections to the databases.
These connections can be either to the live databases for post-ETL testing or
to new temporary databases, which are created from scratch and dropped after
the tests have completed.

"""
import logging
from pathlib import Path

import sqlalchemy as sa
import yaml

import pudl
from pudl.convert.epacems_to_parquet import epacems_to_parquet
from pudl.extract.ferc1 import get_dbc_map, get_fields

logger = logging.getLogger(__name__)


def test_datapkg_bundle(datapkg_bundle):
    """Generate limited packages for testing."""
    pass


def test_pudl_engine(pudl_engine):
    """Try creating a pudl_engine...."""
    assert isinstance(pudl_engine, sa.engine.Engine)
    assert "plants_pudl" in pudl_engine.table_names()
    assert "utilities_pudl" in pudl_engine.table_names()


def test_ferc1_etl(ferc1_engine):
    """
    Create a fresh FERC Form 1 SQLite DB and attempt to access it.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the ferc1_engine fixture defined in conftest.py
    """
    assert isinstance(ferc1_engine, sa.engine.Engine)
    assert "f1_respondent_id" in ferc1_engine.table_names()


def test_epacems_to_parquet(
    datapkg_bundle,
    pudl_settings_fixture,
    pudl_etl_params,
    request
):
    """Attempt to convert a small amount of EPA CEMS data to parquet format."""
    epacems_datapkg_json = Path(
        pudl_settings_fixture['datapkg_dir'],
        pudl_etl_params['datapkg_bundle_name'],
        'epacems-eia',
        "datapackage.json"
    )
    logger.info(f"Loading epacems from {epacems_datapkg_json}")
    flat = pudl.etl.get_flattened_etl_parameters(
        pudl_etl_params["datapkg_bundle_settings"]
    )
    epacems_to_parquet(
        datapkg_path=epacems_datapkg_json,
        epacems_years=flat["epacems_years"],
        epacems_states=flat["epacems_states"],
        out_dir=Path(pudl_settings_fixture['parquet_dir'], 'epacems'),
        compression='snappy',
        clobber=False,
    )


def test_ferc1_schema(ferc1_etl_params, pudl_ferc1datastore_fixture):
    """
    Check to make sure we aren't missing any old FERC Form 1 tables or fields.

    Exhaustively enumerate all historical sets of FERC Form 1 database tables
    and their constituent fields. Check to make sure that the current database
    definition, based on the given reference year and our compilation of the
    DBF filename to table name mapping from 2015, includes every single table
    and field that appears in the historical FERC Form 1 data.
    """
    refyear = ferc1_etl_params['ferc1_to_sqlite_refyear']
    ds = pudl_ferc1datastore_fixture
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
    for yr in ferc1_etl_params['ferc1_to_sqlite_years']:
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
            # Check to make sure there aren't any lost archaic fields:
            for field in dbc_maps[yr][table].values():
                if field not in current_dbc_map[table].values():
                    raise AssertionError(
                        f"Long lost FERC1 field '{field}' found in table "
                        f"'{table}' from year {yr}. "
                        f"Refyear: {refyear}"
                    )


class TestFerc1Datastore:
    """Validate the Ferc1 Datastore and integration functions."""

    def test_ferc_folder(self, pudl_ferc1datastore_fixture):
        """Spot check we get correct folder names per dataset year."""
        ds = pudl_ferc1datastore_fixture
        assert ds.get_dir(1994) == Path("FORMSADMIN/FORM1/working")
        assert ds.get_dir(2001) == Path("UPLOADERS/FORM1/working")
        assert ds.get_dir(2002) == Path("FORMSADMIN/FORM1/working")
        assert ds.get_dir(2010) == Path("UPLOADERS/FORM1/working")
        assert ds.get_dir(2015) == Path("UPLOADERS/FORM1/working")

    def test_get_fields(self, pudl_ferc1datastore_fixture, test_dir):
        """Check that the get fields table works as expected."""
        ds = pudl_ferc1datastore_fixture

        expect_path = test_dir / "data/ferc1/f1_2018/get_fields.json"

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

    @staticmethod
    def expected_file_name(extractor, page, year, expected_name):
        """Check if extractor can access files with expected file names."""
        if extractor.excel_filename(page, year=year) != expected_name:
            raise AssertionError(
                f"file name for {page} in {year} doesn't match datastore."
            )

    def test_excel_filename_eia860(self, pudl_datastore_fixture):
        """Spot check eia860 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        self.expected_file_name(
            extractor=extractor,
            page='boiler_generator_assn',
            year=2011,
            expected_name="EnviroAssocY2011.xlsx"
        )
        self.expected_file_name(
            extractor=extractor,
            page='generator_retired',
            year=2016,
            expected_name="3_1_Generator_Y2016.xlsx"
        )
        self.expected_file_name(
            extractor=extractor,
            page='utility',
            year=2018,
            expected_name="1___Utility_Y2018.xlsx"
        )

    def test_excel_filename_eia923(self, pudl_datastore_fixture):
        """Spot check eia923 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        self.expected_file_name(
            extractor=extractor,
            page='plant_frame',
            year=2009,
            expected_name="EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS"
        )
        self.expected_file_name(
            extractor=extractor,
            page='energy_storage',
            year=2019,
            expected_name="EIA923_Schedules_2_3_4_5_M_12_2019_Final.xlsx"
        )
        self.expected_file_name(
            extractor=extractor,
            page='puerto_rico',
            year=2012,
            expected_name="EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx"
        )

    def test_extract_eia860(self, pudl_datastore_fixture):
        """Spot check extraction of eia860 excel files."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        page = 'ownership'
        year = 2018
        if "Ownership" not in extractor.load_excel_file(
                page="ownership", year=2018).sheet_names:
            raise AssertionError(
                f"page {page} not found in datastore for {year}"
            )

    def test_extract_eia923(self, pudl_datastore_fixture):
        """Spot check extraction eia923 excel files."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        page = 'stocks'
        year = 2018
        if "Page 3 Boiler Fuel Data" not in extractor.load_excel_file(
                page=page, year=year).sheet_names:
            raise AssertionError(
                f"page {page} not found in datastore for {year}"
            )
