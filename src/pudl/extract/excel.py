"""Load excel metadata CSV files form a python data package."""

import pathlib
import re
from io import BytesIO

import dbfread
import pandas as pd

import pudl
from pudl.extract.extractor import GenericExtractor, GenericMetadata, PartitionSelection

logger = pudl.logging_helpers.get_logger(__name__)


class ExcelMetadata(GenericMetadata):
    """Load Excel metadata from Python package data.

    Excel sheet files may contain many different tables. When we load those
    into dataframes, metadata tells us how to do this. Metadata generally informs
    us about the position of a given page in the file (which sheet and which row)
    and it informs us how to translate excel column names into standardized
    column names.

    When metadata object is instantiated, it is given ${dataset} name and it
    will attempt to load csv files from pudl.package_data.${dataset} package.

    It expects the following kinds of files:

    * skiprows.csv tells us how many initial rows should be skipped when loading
      data for given (partition, page).
    * skipfooter.csv tells us how many bottom rows should be skipped when
      loading data for given partition (partition, page).
    * page_map.csv tells us what is the excel sheet name that should be read
      when loading data for given (partition, page)
    * column_map/${page}.csv currently informs us how to translate input column
      names to standardized pudl names for given (partition, input_col_name).
      Relevant page is encoded in the filename.

    Optional file:

    * page_part_map.csv tells us what secondary partition (e.g. "form") needs to be
      specified to correctly identify the file housing the desired page. This is only
      required when a file can only be uniquely located using a combination of
      partitions (e.g. form and year).

    """

    # TODO: we could validate whether metadata is valid for all year. We should have
    # existing records for each (year, page) -> sheet_name, (year, page) -> skiprows
    # and for all (year, page) -> column map

    def __init__(self, dataset_name: str):
        """Create Metadata object and load metadata from python package.

        Args:
            dataset_name: Name of the package/dataset to load the metadata from.
            Files will be loaded from pudl.package_data.${dataset_name}
        """
        super().__init__(dataset_name)
        self._skiprows = self._load_csv(self._pkg, "skiprows.csv")
        self._skipfooter = self._load_csv(self._pkg, "skipfooter.csv")
        self._sheet_name = self._load_csv(self._pkg, "page_map.csv")
        self._file_name = self._load_csv(self._pkg, "file_map.csv")
        # Most excel extracted datasets do not have a page to part map. If they
        # don't, assign null.
        try:
            self._page_part_map = self._load_csv(self._pkg, "page_part_map.csv")
        except FileNotFoundError:
            self._page_part_map = pd.DataFrame()

    def get_sheet_name(self, page, **partition):
        """Return name of Excel sheet containing data for given partition and page."""
        return self._sheet_name.loc[page, str(self._get_partition_selection(partition))]

    def get_skiprows(self, page, **partition):
        """Return number of header rows to skip when loading a partition and page."""
        return self._skiprows.loc[page, str(self._get_partition_selection(partition))]

    def get_skipfooter(self, page, **partition):
        """Return number of footer rows to skip when loading a partition and page."""
        return self._skipfooter.loc[page, str(self._get_partition_selection(partition))]

    def get_file_name(self, page, **partition):
        """Returns file name of given partition and page."""
        return self._file_name.loc[page, str(self._get_partition_selection(partition))]

    def get_form(self, page) -> str:
        """Returns the form name for a given page."""
        return self._page_part_map.loc[page, "form"]


class ExcelExtractor(GenericExtractor):
    """Logic for extracting :class:`pd.DataFrame` from Excel spreadsheets.

    This class implements the generic dataset agnostic logic to load data
    from excel spreadsheet simply by using excel Metadata for given dataset.

    It is expected that individual datasets will subclass this code and add
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

      * process_raw() is applied right after loading the excel DataFrame
        from the disk.
      * process_renamed() is applied after input columns were renamed to
        standardized pudl columns.
      * process_final_page() is applied when data from all available years
        is merged into single DataFrame for a given page.

    5. get_datapackage_resources() if partition is anything other than a year,
    this method should be overwritten in the dataset-specific extractor.
    """

    METADATA: ExcelMetadata = None

    def __init__(self, ds):
        """Create new extractor object and load metadata.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
        """
        super().__init__(ds)
        self._metadata = self.METADATA
        self._file_cache = {}

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Transforms raw dataframe and rename columns."""
        df = self.add_data_maturity(df, page, **partition)
        return df.rename(columns=self._metadata.get_column_map(page, **partition))

    def add_data_maturity(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Add data_maturity column to indicate the maturity of partition data.

        The three options enumerated here are ``final``, ``provisional`` or
        ``monthly_update`` (``incremental_ytd`` is not currently implemented). We
        determine if a df should be labeled as ``provisional`` by using the file names
        because EIA seems to always include ``Early_Release`` in the file names. We
        determine if a df should be labeled as ``monthly_update`` by checking if the
        ``self.dataset_name`` is ``eia860m``.

        This method adds a column and thus adds ``data_maturity`` to
        ``self.cols_added``.
        """
        maturity = "final"
        file_name = self.source_filename(page, **partition)
        if "early_release" in file_name.lower():
            maturity = "provisional"
        elif self._dataset_name == "eia860m":
            maturity = "monthly_update"
        elif "EIA923_Schedules_2_3_4_5_M_" in file_name:
            release_month = re.search(
                r"EIA923_Schedules_2_3_4_5_M_(\d{2})",
                file_name,
            ).group(1)
            if release_month != "12":
                maturity = "incremental_ytd"
        df = df.assign(data_maturity=maturity)
        self.cols_added.append("data_maturity")
        return df

    @staticmethod
    def get_dtypes(page: str, **partition: PartitionSelection) -> dict:
        """Provide custom dtypes for given page and partition."""
        return {}

    def zipfile_resource_partitions(
        self, page: str, **partition: PartitionSelection
    ) -> dict:
        """Specify the partitions used for returning a zipfile from the datastore.

        By default, this method appends any page to partition mapping in
        :attr:`METADATA._page_part_map`. Most datasets do not have page to part
        maps and just return the same partition that is passed in. If you have
        dataset-specific partition mappings that are needed to return a zipfile from the
        datastore, override this method to return the desired partitions.
        """
        if not self._metadata._page_part_map.empty:
            partition.update(self._metadata._page_part_map.loc[page])
        return partition

    def load_source(self, page: str, **partition: PartitionSelection) -> pd.DataFrame:
        """Produce the ExcelFile object for the given (partition, page).

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks",
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            pd.DataFrame instance with the parsed Excel spreadsheet frame
        """
        xlsx_filename = self.source_filename(page, **partition)

        if xlsx_filename not in self._file_cache:
            excel_file = None
            with self.ds.get_zipfile_resource(
                self._dataset_name,
                **self.zipfile_resource_partitions(page, **partition),
            ) as zf:
                # If loading the excel file from the zip fails then try to open a dbf file.
                extension = pathlib.Path(xlsx_filename).suffix.lower()
                if extension == ".dbf":
                    with zf.open(xlsx_filename) as dbf_filepath:
                        df = pd.DataFrame(
                            iter(dbfread.DBF(xlsx_filename, filedata=dbf_filepath))
                        )
                        excel_file = pudl.helpers.convert_df_to_excel_file(
                            df, index=False
                        )
                else:
                    excel_file = pd.ExcelFile(
                        BytesIO(zf.read(xlsx_filename)), engine="calamine"
                    )
            self._file_cache[xlsx_filename] = excel_file
        # TODO(rousik): this _file_cache could be replaced with @cache or @memoize annotations
        excel_file = self._file_cache[xlsx_filename]

        return pd.read_excel(
            excel_file,
            sheet_name=self._metadata.get_sheet_name(page, **partition),
            skiprows=self._metadata.get_skiprows(page, **partition),
            skipfooter=self._metadata.get_skipfooter(page, **partition),
            dtype=self.get_dtypes(page, **partition),
        )

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the xlsx document file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            string name of the xlsx file
        """
        return self._metadata.get_file_name(page, **partition)
