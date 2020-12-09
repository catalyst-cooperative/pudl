"""Load excel metadata CSV files form a python data package."""
import csv
import importlib.resources
import logging
import zipfile
from pathlib import Path

import pandas as pd

import pudl

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
    data for given (partition, page).
    - skipfooter.csv tells us how many bottom rows should be skipped when
    loading data for given partition (partition, page).
    - tab_map.csv tells us what is the excel sheet name that should be read
    when loading data for given (partition, page)
    - column_map/${page}.csv currently informs us how to translate input column
    names to standardized pudl names for given (partition, input_col_name).
    Relevant page is encoded in the filename.
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
        self._skipfooter = self._load_csv(pkg, 'skipfooter.csv')
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

    def get_sheet_name(self, page, **partition):
        """Returns name of the excel sheet that contains the data for given partition and page."""
        return self._sheet_name.at[page, str(self._get_partition_key(partition))]

    def get_skiprows(self, page, **partition):
        """Returns number of initial rows to skip when loading given partition and page."""
        return self._skiprows.at[page, str(self._get_partition_key(partition))]

    def get_skipfooter(self, page, **partition):
        """Returns number of bottom rows to skip when loading given partition and page."""
        return self._skipfooter.at[page, str(self._get_partition_key(partition))]

    def get_column_map(self, page, **partition):
        """Returns the dictionary mapping input columns to pudl columns for given partition and page."""
        return {v: k for k, v in self._column_map[page].T.loc[str(self._get_partition_key(partition))].to_dict().items() if v != -1}

    def get_all_columns(self, page):
        """Returns list of all pudl (standardized) columns for a given page (across all partition)."""
        return sorted(self._column_map[page].T.columns)

    def get_all_pages(self):
        """Returns list of all known pages."""
        return sorted(self._column_map.keys())

    @staticmethod
    def _load_csv(package, filename):
        """Load metadata from a filename that is found in a package."""
        return pd.read_csv(importlib.resources.open_text(package, filename),
                           index_col=0, comment='#')

    @staticmethod
    def _get_partition_key(partition):
        """Grab the partition key."""
        if len(partition) != 1:
            raise AssertionError(
                f"Expecting exactly one partition attribute (found: {partition})")
        return list(partition.values())[0]


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

    3. dtypes() should return dict with {column_name: pandas_datatype} if you
    need to specify which datatypes should be uded upon loading.

    4. If data cleanup is necessary, you can apply custom logic by overriding
    one of the following functions (they all return the modified dataframe):
    - process_raw() is applied right after loading the excel DataFrame
    from the disk.
    - process_renamed() is applied after input columns were renamed to
    standardized pudl columns.
    - process_final_page() is applied when data from all available years
    is merged into single DataFrame for a given page.

    5. get_datapackage_resources() if partition is anything other than a year,
    this method should be overwritten in the dataset-specific extractor.
    """

    METADATA = None
    """Instance of metadata object to use with this extractor."""

    BLACKLISTED_PAGES = []
    """List of supported pages that should not be extracted."""

    def __init__(self, ds):
        """
        Create new extractor object and load metadata.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
        """
        if not self.METADATA:
            raise NotImplementedError('self.METADATA must be set.')
        self._metadata = self.METADATA
        self._dataset_name = self._metadata.get_dataset_name()
        self._file_cache = {}
        self.ds = ds

    def process_raw(self, df, page, **partition):
        """Transforms raw dataframe and rename columns."""
        self.cols_added = []
        return df.rename(columns=self._metadata.get_column_map(partition, page))

    @staticmethod
    def process_renamed(df, page, **partition):
        """Transforms dataframe after columns are renamed."""
        return df

    @staticmethod
    def process_final_page(df, page):
        """Final processing stage applied to a page DataFrame."""
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Provide custom dtypes for given page and partition."""
        return {}

    def extract(self, **partitions):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            partitions (list, tuple or string): list of partitions to
                extract. (Ex: [2009, 2010] if dataset is partitioned by years
                or '2020-08' if dataset is partitioned by year_month)
        """
        raw_dfs = {}
        if not partitions:
            logger.warning(
                f'No partitions were given. Not extracting {self._dataset_name} spreadsheet data.')
            return raw_dfs
        logger.info(f'Extracting {self._dataset_name} spreadsheet data.')

        for page in self._metadata.get_all_pages():
            if page in self.BLACKLISTED_PAGES:
                logger.debug(f'Skipping blacklisted page {page}.')
                continue
            df = pd.DataFrame()
            for partition in pudl.helpers.iterate_multivalue_dict(**partitions):
                # we are going to skip
                if self.excel_filename(page, **partition) == '-1':
                    logger.debug(
                        f'No page for {self._dataset_name} {page} {partition}')
                    continue
                logger.debug(
                    f'Loading dataframe for {self._dataset_name} {page} {partition}')
                newdata = pd.read_excel(
                    self.load_excel_file(page, **partition),
                    sheet_name=self._metadata.get_sheet_name(
                        page, **partition),
                    skiprows=self._metadata.get_skiprows(page, **partition),
                    skipfooter=self._metadata.get_skipfooter(
                        page, **partition),
                    dtype=self.get_dtypes(page, **partition))
                newdata = pudl.helpers.simplify_columns(newdata)
                newdata = self.process_raw(newdata, page, **partition)
                newdata = self.process_renamed(newdata, page, **partition)
                df = df.append(newdata, sort=True, ignore_index=True)

            # After all years are loaded, consolidate missing columns
            missing_cols = set(self._metadata.get_all_columns(
                page)).difference(df.columns)
            empty_cols = pd.DataFrame(columns=missing_cols)
            df = pd.concat([df, empty_cols], sort=True)
            if (len(self.METADATA._column_map[page].index)
                    + len(self.cols_added)) != len(df.columns):
                # raise AssertionError(
                logger.warning(
                    f'Columns for {page} are off: should be '
                    f'{len(self.METADATA._column_map[page].index)} but got '
                    f'{len(df.columns)}'
                )
            raw_dfs[page] = self.process_final_page(df, page)
        return raw_dfs

    def load_excel_file(self, page, **partition):
        """
        Produce the ExcelFile object for the given (partition, page).

        Args:
            page (str): pudl name for the dataset contents, eg
                  "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or
                "2020-08" for year_month partition)

        Return:
            string name of the xlsx file
        """
        info = self.ds.get_resources(self._dataset_name, **partition)

        if info is None:
            logger.info(f"No resource found for {partition}, {page}")
            return

        item = next(info)
        p = Path(item["path"])
        xlsx_filename = self.excel_filename(page, **partition)

        if xlsx_filename in self._file_cache.keys():
            logger.debug("Grabing cached file.")
            excel_file = self._file_cache[xlsx_filename]
        else:
            logger.debug("Grabing new file.")

        if p.name != xlsx_filename:
            zf = zipfile.ZipFile(p)
            excel_file = pd.ExcelFile(zf.read(xlsx_filename))
        else:
            excel_file = pd.ExcelFile(p)
        self._file_cache[xlsx_filename] = excel_file
        return excel_file

    def excel_filename(self, page, **partition):
        """
        Produce the xlsx document file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg
                  "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or
                "2020-08" for year_month partition)
        Return:
            string name of the xlsx file
        """
        pkg = f"pudl.package_data.meta.xlsx_maps.{self._dataset_name}"

        with importlib.resources.open_text(pkg, "file_map.csv") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if row["page"] == page:
                    return row[str(self.METADATA._get_partition_key(partition))]

        raise ValueError(f"No excel sheet for {partition}, {page}")
