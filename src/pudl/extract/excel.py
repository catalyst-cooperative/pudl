"""Load excel metadata CSV files form a python data package."""

import glob
import importlib.resources
import logging
import os.path

import pandas as pd

import pudl
import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


class Metadata(object):
    """Loads excel dataset metadata.

    This metadata tells us how the data is stored in the Excel spreadsheet
    and how to extract it into DataFrames.

    The components of the metadata consist of:
      - skiprows: For a given (year, page), how many leading rows should
          be skipped before loading the data.
      - sheet_name: For a given (year, page), what is the name of the
          excel sheet that contains this data.
      - column_map: For a given (year, page), this provides mapping from
          excel column names to the output PUDL column names. This provides
          a tool for unifying disparate naming schemes across years.

    This tool loads the metadata from CSV files from a given python package
    and expects the files to follow consistent naming scheme:
    - skiprows.csv
    - tab_map.csv (for sheet name)
    - column_maps/${page}.csv for column mapping for ${page}
   """

    def sheet_name(self, year, page):
        """Returns name of excel sheet containing data for given year and page."""
        return self._sheet_name.at[year, page]

    def skiprows(self, year, page):
        """Returns number of rows to skip when loading given year and page."""
        return self._skiprows.at[year, page]

    def column_map(self, year, page):
        """Return the dictionary mapping excel_column to pudl_column.

        The underlying metadata CSV file maps pudl_column to excel_column so
        we need to invert the mapping before returning it.
        """
        return {v: k for k, v in self._column_map[page].loc[year].to_dict().items()}

    def all_columns(self, page):
        """Returns set of all columns for a given page (across all years)."""
        return set(self._column_map[page].columns)

    def all_pages(self):
        """Returns list of supported pages."""
        return self._column_map.keys()

    def _load_csv(self, pkg, filename):
        """Load metadata from CSV file from a python package."""
        return pd.read_csv(importlib.resources.open_text(pkg, filename),
                           index_col=0, comment='#')

    def __init__(self, dataset_name):
        """Loads CSV metadata and constructs ExcelMetadata object."""
        pkg = f'pudl.package_data.meta.xlsx_maps.{dataset_name}'
        self._skiprows = self._load_csv(pkg, 'skiprows.csv')
        self._sheet_name = self._load_csv(pkg, 'tab_map.csv')
        column_map_pkg = pkg + '.column_maps'
        self._column_map = {}
        for res in importlib.resources.contents(column_map_pkg):
            # res is expected to be ${page}.csv
            parts = res.split('.')
            if len(parts) != 2 or parts[1] != 'csv':
                continue
            column_map = self._load_csv(column_map_pkg, res)
            self._column_map[parts[0]] = column_map


class GenericExtractor(object):
    """Extracts DataFrames from excel spreadsheets.

    Subclasses for each excel based dataset should implement
    custom logic by overriding class-level constants and methods
    as follows:

    1. Set DATASET attribute such that metadata for the excel
       spreadsheets will be loaded from
       pudl.package_data.meta.xlsx_maps.${DATASET}. See Metadata
       object for how this process works.
    2. Set BLACKLISTED_PAGES to bypass loading of pages that
       are described by the metadata but should not be extracted.
    3. override file_base_path method to return basename globs
       for a given (year, page) combination.
    4. Optionally provide custom data cleanup/processing logic
       in process_raw, process_renamed and process_final_page
       methods.
    5. Optionally provide dtypes for raw data loaded from excel
       by overrinding dtypes method.
    6.
"""
    DATASET = None

    BLACKLISTED_PAGES = []
    """List of supported pages that should not be extracted."""

    def __init__(self, data_dir):
        """Create new extractor object and load metadata."""
        self._data_dir = data_dir
        self._metadata = Metadata(self.DATASET)

    def process_raw(self, year, page, dataframe):
        """Transforms raw dataframe before columns are renamed."""
        return dataframe

    def process_renamed(self, year, page, dataframe):
        """Transforms dataframe after columns are renamed."""
        return dataframe

    def process_final_page(self, page, dataframe):
        """Final processing stage applied to a page DataFrame."""
        return dataframe

    def dtypes(self, year, page):
        """Provide custom dtypes for given page and year."""
        return {}

    def extract(self, years):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.
        """
        if not years:
            logger.info(
                f'No years given. Not extracting {self.DATSET} spreadsheet data.')
            return {}

        bad_years = set(years).difference(set(pc.working_years[self.DATASET]))
        if bad_years:
            raise ValueError(
                f"Requested invalid years for {self.DATASET}: {bad_years}\n"
                f"Supported years: {pc.working_years[self.DATASET]}\n"
            )

        excel_files = {}
        for file_path in self._get_all_file_paths(years):
            logger.info(
                f'Loading excel spreadsheet from {file_path}')
            excel_files[file_path] = pd.ExcelFile(file_path)

        # excel_files now contains pre-loaded excel files, now munch the data
        # per page and put them in raw_dfs[page] = DataFrame
        raw_dfs = {}
        for page in self._metadata.all_pages():
            if page in self.BLACKLISTED_PAGES:
                logger.info(f'Skipping blacklisted page {page}.')
                continue
            df = pd.DataFrame()
            for yr in years:
                data = excel_files[self._get_file_path(yr, page)]

                logger.info(
                    f'Loading dataframe for {self.DATASET} {page} {yr}')
                newdata = pd.read_excel(
                    data,
                    sheet_name=self._metadata.sheet_name(yr, page),
                    skiprows=self._metadata.skiprows(yr, page),
                    dtype=self.dtypes(yr, page))

                newdata = pudl.helpers.simplify_columns(newdata)
                newdata = self.process_raw(yr, page, newdata)
                newdata = newdata.rename(
                    columns=self._metadata.column_map(yr, page))
                newdata = self.process_renamed(yr, page, newdata)
                df = df.append(newdata, sort=True)

            # After all years are loaded, consolidate missing columns
            missing_cols = self._metadata.all_columns(
                page).difference(df.columns)
            empty_cols = pd.DataFrame(columns=missing_cols)
            df = pd.concat([df, empty_cols], sort=True)
            raw_dfs[page] = self.process_final_page(page, df)
        return raw_dfs

    def _get_all_file_paths(self, years):
        """Returns list of all data files that will be loaded for all years."""
        bad_years = []
        all_files = []
        for page in self._metadata.all_pages():
            for yr in years:
                try:
                    all_files.append(self._get_file_path(yr, page))
                except IndexError:
                    bad_years.append(yr)
        if bad_years:
            raise FileNotFoundError(
                f'Missing {self.DATASET} files for years {bad_years}.')
        return all_files

    def verify_years(self, years):
        """Validate that all files are availabe.

        Raises:
            FileNotFoundError: when some files are not found in the datastore.
        """
        self._get_all_file_paths(years)

    def file_basename_glob(self, year, page):
        """Returns base filename glob for a given year and page.

        This is later combined with path from datastore to fetch
        the excel spreadsheet from disk.
        """
        return None

    def _get_file_path(self, year, page):
        """Returns full path to the excel spreadsheet."""
        directory = datastore.path(self.DATASET, year=year, file=False,
                                   data_dir=self._data_dir)
        files = glob.glob(os.path.join(
            directory, self.file_basename_glob(year, page)))
        if len(files) != 1:
            logger.warning(
                f'There are {len(files)} matching files for'
                f'{self.DATASET} {page} {year}. Exactly one expected.')
        return files[0]
