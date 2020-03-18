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
    """Loads excel metadata from python package.

    Excel sheet files may contain many different tables. When we load those
    into dataframes, metadata tells us how to do this. Metadata generally informs
    us about the position of a given page in the file (which sheet and which row)
    and it informs us how to translate excel column names into standardized
    column names.

    When metadata object is instantiated, it is given ${dataset} name and it
    will attempt to load csv files from pudl.package_data.meta.xlsx_maps.${dataset}
    package.

    It expects the following kinds of files:
    - skiprows.csv tells us how many initial rows should be skipped when loading
    data for given (year, page).
    - tab_map.csv tells us what is the excel sheet name that should be read
    when loading data for given (year, page)
    - column_map/${page}.csv currently informs us how to translate input column
    names to standardized pudl names for given (year, input_col_name). Relevant
    page is encoded in the filename.
    """

    # TODO: we could validate whether metadata is valid for all year. We should have
    # existing records for each (year, page) -> sheet_name, (year, page) -> skiprows
    # and for all (year, page) -> column map

    def __init__(self, dataset_name):
        """Create Metadata object and load metadata from python package.

        Args:
            dataset_name: Name of the package/dataset to load the metadata from.
            Files will be loaded from pudl.package_data.meta.xlsx_meta.${dataset_name}.
        """
        pkg = f'pudl.package_data.meta.xlsx_maps.{dataset_name}'
        self._dataset_name = dataset_name
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

    def get_dataset_name(self):
        """Returns the name of the dataset described by this metadata."""
        return self._dataset_name

    def get_sheet_name(self, year, page):
        """Returns name of the excel sheet that contains the data for given year and page."""
        return self._sheet_name.at[year, page]

    def get_skiprows(self, year, page):
        """Returns number of initial rows to skip when loading given year and page."""
        return self._skiprows.at[year, page]

    def get_column_map(self, year, page):
        """Returns the dictionary mapping input columns to pudl columns for given year and page."""
        return {v: k for k, v in self._column_map[page].loc[year].to_dict().items()}

    def get_all_columns(self, page):
        """Returns list of all pudl (standardized) columns for a given page (across all years)."""
        return sorted(self._column_map[page].columns)

    def get_all_pages(self):
        """Returns list of all known pages."""
        return sorted(self._column_map.keys())

    @staticmethod
    def _load_csv(package, filename):
        """Load metadata from a filename that is found in a package."""
        return pd.read_csv(importlib.resources.open_text(package, filename),
                           index_col=0, comment='#')


class GenericExtractor(object):
    """Contains logic for extracting panda.DataFrames from excel spreadsheets.

    This class implements the generic dataset agnostic logic to load data
    from excel spreadsheet simply by using excel Metadata for given dataset.

    It is expected that individual datasets wil subclass this code and add
    custom business logic by overriding necessary methods.

    When implementing custom business logic, the following should be modified:

    1. DATASET class attribute controls which excel metadata should be loaded.

    2. BLACKLISTED_PAGES class attribute specifies which pages should not
    be loaded from the underlying excel files even if the metadata is
    available. This can be used for experimental/new code that should not be
    run yet.

    3. file_basename_glob() tells us what is the basename of the excel file
    that contains the data for a given (year, page).

    4. dtypes() should return dict with {column_name: pandas_datatype} if you
    need to specify which datatypes should be uded upon loading.

    5. If data cleanup is necessary, you can apply custom logic by overriding
    one of the following functions (they all return the modified dataframe):
    - process_raw() is applied right after loading the excel DataFrame
    from the disk.
    - process_renamed() is applied after input columns were renamed to
    standardized pudl columns.
    - process_final_page() is applied when data from all available years
    is merged into single DataFrame for a given page.
    """

    METADATA = None
    """Instance of metadata object to use with this extractor."""

    BLACKLISTED_PAGES = []
    """List of supported pages that should not be extracted."""

    def __init__(self, data_dir, metadata=None):
        """Create new extractor object and load metadata.

        Args:
            data_dir: Path to the data_dir to use when loading excel
              files from disk (passed to datastore).
        """
        self._data_dir = data_dir
        if not self.METADATA:
            raise NotImplementedError('self.METADATA must be set.')
        self._metadata = self.METADATA
        self._dataset_name = self._metadata.get_dataset_name()
        self._file_cache = {}

    @staticmethod
    def process_raw(df, year, page):
        """Transforms raw dataframe before columns are renamed."""
        return df

    @staticmethod
    def process_renamed(df, year, page):
        """Transforms dataframe after columns are renamed."""
        return df

    @staticmethod
    def process_final_page(df, page):
        """Final processing stage applied to a page DataFrame."""
        return df

    @staticmethod
    def get_dtypes(year, page):
        """Provide custom dtypes for given page and year."""
        return {}

    def extract(self, years):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.
        """
        # TODO: should we run verify_years(?) here?
        if not years:
            logger.info(
                f'No years given. Not extracting {self.DATSET} spreadsheet data.')
            return {}

        raw_dfs = {}
        for page in self._metadata.get_all_pages():
            if page in self.BLACKLISTED_PAGES:
                logger.info(f'Skipping blacklisted page {page}.')
                continue
            df = pd.DataFrame()
            for yr in years:
                logger.info(
                    f'Loading dataframe for {self._dataset_name} {page} {yr}')
                newdata = pd.read_excel(
                    self._load_excel_file(yr, page),
                    sheet_name=self._metadata.get_sheet_name(yr, page),
                    skiprows=self._metadata.get_skiprows(yr, page),
                    dtype=self.get_dtypes(yr, page))

                newdata = pudl.helpers.simplify_columns(newdata)
                newdata = self.process_raw(newdata, yr, page)
                newdata = newdata.rename(
                    columns=self._metadata.get_column_map(yr, page))
                newdata = self.process_renamed(newdata, yr, page)
                df = df.append(newdata, sort=True, ignore_index=True)

            # After all years are loaded, consolidate missing columns
            missing_cols = set(self._metadata.get_all_columns(
                page)).difference(df.columns)
            empty_cols = pd.DataFrame(columns=missing_cols)
            df = pd.concat([df, empty_cols], sort=True)
            raw_dfs[page] = self.process_final_page(df, page)
        return raw_dfs

    def _load_excel_file(self, year, page):
        """Returns ExcelFile object corresponding to given (year, page).

        Additionally, loaded files are stored under self._file_cache for reuse.
        """
        full_path = self._get_file_path(year, page)
        if full_path not in self._file_cache:
            logger.info(
                f'{self._dataset_name}: Loading excel file {full_path}')
            self._file_cache[full_path] = pd.ExcelFile(full_path)
        return self._file_cache[full_path]

    def verify_years(self, years):
        """Validate that all files are availabe.

        Raises:
            FileNotFoundError: when some files are not found in the datastore.
        """
        bad_years = set()
        for page in self._metadata.get_all_pages():
            if page in self.BLACKLISTED_PAGES:
                continue
            for yr in years:
                try:
                    self._get_file_path(yr, page)
                except FileNotFoundError:
                    bad_years.add(yr)
        if bad_years:
            raise FileNotFoundError(
                f'Missing {self._dataset_name} files for years {bad_years}.')
        bad_years = set(years).difference(pc.working_years[self._dataset_name])
        if bad_years:
            raise IndexError(
                f"{self._dataset_name} doesn't support years {bad_years}")

    @staticmethod
    def file_basename_glob(year, page):
        """Returns base filename glob for a given year and page.

        This is later combined with path from datastore to fetch
        the excel spreadsheet from disk.
        """
        return NotImplementedError('This method must be implemented.')

    def _get_file_path(self, year, page):
        """Returns full path to the excel spreadsheet."""
        directory = datastore.path(self._dataset_name, year=year, file=False,
                                   data_dir=self._data_dir)
        files = glob.glob(os.path.join(
            directory, self.file_basename_glob(year, page)))
        if len(files) != 1:
            raise FileNotFoundError(
                f'{len(files)} matching files found for ' +
                f'{self._dataset_name} {page} {year}. Exacly one expected.')
        return files[0]
