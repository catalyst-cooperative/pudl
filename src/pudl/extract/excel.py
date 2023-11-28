"""Load excel metadata CSV files form a python data package."""
import importlib.resources
import pathlib
from collections import defaultdict
from io import BytesIO

import dbfread
import pandas as pd
import regex as re
from dagster import (
    AssetsDefinition,
    DynamicOut,
    DynamicOutput,
    OpDefinition,
    graph_asset,
    op,
)

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


class Metadata:
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
        pkg = f"pudl.package_data.{dataset_name}"
        self._dataset_name = dataset_name
        self._skiprows = self._load_csv(pkg, "skiprows.csv")
        self._skipfooter = self._load_csv(pkg, "skipfooter.csv")
        self._sheet_name = self._load_csv(pkg, "page_map.csv")
        self._file_name = self._load_csv(pkg, "file_map.csv")
        column_map_pkg = pkg + ".column_maps"
        self._column_map = {}
        for res_path in importlib.resources.files(column_map_pkg).iterdir():
            # res_path is expected to end with ${page}.csv
            if res_path.suffix == ".csv":
                column_map = self._load_csv(column_map_pkg, res_path.name)
                self._column_map[res_path.stem] = column_map

    def get_dataset_name(self):
        """Returns the name of the dataset described by this metadata."""
        return self._dataset_name

    def get_sheet_name(self, page, **partition):
        """Return name of Excel sheet containing data for given partition and page."""
        return self._sheet_name.at[page, str(self._get_partition_key(partition))]

    def get_skiprows(self, page, **partition):
        """Return number of header rows to skip when loading a partition and page."""
        return self._skiprows.at[page, str(self._get_partition_key(partition))]

    def get_skipfooter(self, page, **partition):
        """Return number of footer rows to skip when loading a partition and page."""
        return self._skipfooter.at[page, str(self._get_partition_key(partition))]

    def get_file_name(self, page, **partition):
        """Returns file name of given partition and page."""
        return self._file_name.at[page, str(self._get_partition_key(partition))]

    def get_column_map(self, page, **partition):
        """Return dictionary for renaming columns in a given partition and page."""
        return {
            v: k
            for k, v in self._column_map[page]
            .T.loc[str(self._get_partition_key(partition))]
            .to_dict()
            .items()
            if v != -1
        }

    def get_all_columns(self, page):
        """Returns list of all pudl columns for a given page across all partitions."""
        return sorted(self._column_map[page].T.columns)

    def get_all_pages(self):
        """Returns list of all known pages."""
        return sorted(self._column_map.keys())

    @staticmethod
    def _load_csv(package, filename):
        """Load metadata from a filename that is found in a package."""
        return pd.read_csv(
            importlib.resources.files(package) / filename, index_col=0, comment="#"
        )

    @staticmethod
    def _get_partition_key(partition):
        """Grab the partition key."""
        if len(partition) != 1:
            raise AssertionError(
                f"Expecting exactly one partition attribute (found: {partition})"
            )
        return list(partition.values())[0]


class GenericExtractor:
    """Logic for extracting :class:`pd.DataFrame` from Excel spreadsheets.

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

      * process_raw() is applied right after loading the excel DataFrame
        from the disk.
      * process_renamed() is applied after input columns were renamed to
        standardized pudl columns.
      * process_final_page() is applied when data from all available years
        is merged into single DataFrame for a given page.

    5. get_datapackage_resources() if partition is anything other than a year,
    this method should be overwritten in the dataset-specific extractor.
    """

    METADATA: Metadata = None
    """Instance of metadata object to use with this extractor."""

    BLACKLISTED_PAGES = []
    """List of supported pages that should not be extracted."""

    def __init__(self, ds):
        """Create new extractor object and load metadata.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
        """
        if not self.METADATA:
            raise NotImplementedError("self.METADATA must be set.")
        self._metadata = self.METADATA
        self._dataset_name = self._metadata.get_dataset_name()
        self._file_cache = {}
        self.ds = ds
        self.cols_added: list[str] = []

    def process_raw(self, df, page, **partition):
        """Transforms raw dataframe and rename columns."""
        df = self.add_data_maturity(df, page, **partition)
        self.cols_added.append("data_label")
        return df.rename(columns=self._metadata.get_column_map(page, **partition))

    def add_data_maturity(self, df: pd.DataFrame, page, **partition) -> pd.DataFrame:
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
        file_name = self.excel_filename(page, **partition)
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
                f"No partitions were given. Not extracting {self._dataset_name} "
                "spreadsheet data."
            )
            return raw_dfs
        logger.info(f"Extracting {self._dataset_name} spreadsheet data.")

        for page in self._metadata.get_all_pages():
            if page in self.BLACKLISTED_PAGES:
                logger.debug(f"Skipping blacklisted page {page}.")
                continue
            dfs = [
                pd.DataFrame(),
            ]
            for partition in pudl.helpers.iterate_multivalue_dict(**partitions):
                # we are going to skip
                if self.excel_filename(page, **partition) == "-1":
                    logger.debug(f"No page for {self._dataset_name} {page} {partition}")
                    continue
                logger.debug(
                    f"Loading dataframe for {self._dataset_name} {page} {partition}"
                )
                newdata = pd.read_excel(
                    self.load_excel_file(page, **partition),
                    sheet_name=self._metadata.get_sheet_name(page, **partition),
                    skiprows=self._metadata.get_skiprows(page, **partition),
                    skipfooter=self._metadata.get_skipfooter(page, **partition),
                    dtype=self.get_dtypes(page, **partition),
                )

                newdata = pudl.helpers.simplify_columns(newdata)
                newdata = self.process_raw(newdata, page, **partition)
                newdata = self.process_renamed(newdata, page, **partition)
                dfs.append(newdata)
                # check if there are any missing or extra columns
                str_part = str(list(partition.values())[0])
                col_map = self.METADATA._column_map[page]
                page_cols = col_map.loc[
                    (col_map[str_part].notnull()) & (col_map[str_part] != -1),
                    [str_part],
                ].index
                expected_cols = page_cols.union(self.cols_added)
                if set(newdata.columns) != set(expected_cols):
                    # TODO (bendnorman): Enforce canonical fields for all raw fields?
                    extra_raw_cols = set(newdata.columns).difference(expected_cols)
                    missing_raw_cols = set(expected_cols).difference(newdata.columns)
                    if extra_raw_cols:
                        logger.warning(
                            f"Extra columns found in extracted table of "
                            f"{page}/{str_part}: {extra_raw_cols}"
                        )
                    if missing_raw_cols:
                        logger.warning(
                            "Expected columns not found in extracted table of "
                            f"{page}/{str_part}: {missing_raw_cols}"
                        )
            df = pd.concat(dfs, sort=True, ignore_index=True)

            # After all years are loaded, add empty columns that could appear
            # in other years so that df matches the database schema
            missing_cols = list(
                set(self._metadata.get_all_columns(page)).difference(df.columns)
            )
            df = pd.concat([df, pd.DataFrame(columns=missing_cols)], sort=True)

            raw_dfs[page] = self.process_final_page(df, page)
        return raw_dfs

    def load_excel_file(self, page, **partition):
        """Produce the ExcelFile object for the given (partition, page).

        Args:
            page (str): pudl name for the dataset contents, eg
                  "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or
                "2020-08" for year_month partition)

        Returns:
            pd.ExcelFile instance with the parsed excel spreadsheet frame
        """
        xlsx_filename = self.excel_filename(page, **partition)

        if xlsx_filename not in self._file_cache:
            excel_file = None
            try:
                # eia860m exports the resources as raw xlsx files that are not
                # embedded in zip archives. To support this, we will first try
                # to retrieve the resource directly. If this fails, we will attempt
                # to open zip archive and locate the xlsx file inside that.

                # TODO(rousik): if we can make it so, it would be useful to normalize
                # the eia860m and zip the xlsx files. Then we could simplify this code.
                res = self.ds.get_unique_resource(
                    self._dataset_name, name=xlsx_filename
                )
                excel_file = pd.ExcelFile(res)
            except KeyError:
                zf = self.ds.get_zipfile_resource(self._dataset_name, **partition)

                # If loading the excel file from the zip fails then try to open a dbf file.
                extension = pathlib.Path(xlsx_filename).suffix.lower()
                if extension == ".dbf":
                    dbf_filepath = zf.open(xlsx_filename)
                    df = pd.DataFrame(
                        iter(dbfread.DBF(xlsx_filename, filedata=dbf_filepath))
                    )
                    excel_file = pudl.helpers.convert_df_to_excel_file(df, index=False)
                else:
                    excel_file = pd.ExcelFile(BytesIO(zf.read(xlsx_filename)))
            finally:
                self._file_cache[xlsx_filename] = excel_file
        # TODO(rousik): this _file_cache could be replaced with @cache or @memoize annotations
        return self._file_cache[xlsx_filename]

    def excel_filename(self, page, **partition):
        """Produce the xlsx document file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or "2020-08" for
                year_month partition)

        Returns:
            string name of the xlsx file
        """
        return self.METADATA.get_file_name(page, **partition)


@op
def concat_pages(paged_dfs: list[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    """Concatenate similar pages of data from different years into single dataframes.

    Transform a list of dictionaries of dataframes into a single dictionary of
    dataframes, where each dataframe is the concatenation of dataframes with identical
    keys from the input list.

    Args:
        paged_dfs: A list of dictionaries whose keys are page names, and values are
            extracted DataFrames. Each element of the list corresponds to a single
            year of the dataset being extracted.

    Returns:
        A dictionary of DataFrames keyed by page name, where the DataFrame contains that
        page's data from all extracted years concatenated together.
    """
    # Transform the list of dictionaries of dataframes into a dictionary of lists of
    # dataframes, in which all dataframes in each list represent different instances of
    # the same page of data from different years
    all_data = defaultdict(list)
    for dfs in paged_dfs:
        for page in dfs:
            all_data[page].append(dfs[page])

    # concatenate the dataframes in each list in the dictionary into a single dataframe
    for page in all_data:
        all_data[page] = pd.concat(all_data[page]).reset_index(drop=True)

    return all_data


def year_extractor_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> OpDefinition:
    """Construct a Dagster op that extracts one year of data, given an extractor class.

    Args:
        extractor_cls: Class of type :class:`GenericExtractor` used to extract the data.
        name: Name of an Excel based dataset (e.g. "eia860").
    """

    def extract_single_year(context, year: int) -> dict[str, pd.DataFrame]:
        """A function that extracts a year of spreadsheet data from an Excel file.

        This function will be decorated with a Dagster op and returned.

        Args:
            context: Dagster keyword that provides access to resources and config.
            year: Year of data to extract.

        Returns:
            A dictionary of DataFrames extracted from Excel, keyed by page name.
        """
        ds = context.resources.datastore
        return extractor_cls(ds).extract(year=[year])

    return op(
        required_resource_keys={"datastore", "dataset_settings"},
        name=f"extract_single_{name}_year",
    )(extract_single_year)


def years_from_settings_factory(name: str) -> OpDefinition:
    """Construct a Dagster op to get target years from settings in the Dagster context.

    Args:
        name: Name of an Excel based dataset (e.g. "eia860"). Currently this must be
            one of the attributes of :class:`pudl.settings.EiaSettings`

    """

    def years_from_settings(context) -> DynamicOutput:
        """Produce target years for the given dataset from the EIA settings object.

        These will be used to kick off worker processes to extract each year of data in
        parallel.

        Yields:
            A Dagster :class:`DynamicOutput` object representing the year to be
            extracted. See the Dagster API documentation for more details:
            https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        """
        eia_settings = context.resources.dataset_settings.eia
        for year in getattr(eia_settings, name).years:
            yield DynamicOutput(year, mapping_key=str(year))

    return op(
        out=DynamicOut(),
        required_resource_keys={"dataset_settings"},
        name=f"{name}_years_from_settings",
    )(years_from_settings)


def raw_df_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> AssetsDefinition:
    """Return a dagster graph asset to extract a set of raw DataFrames from Excel files.

    Args:
        extractor_cls: The dataset-specific Excel extractor used to extract the data.
            Needs to correspond to the dataset identified by ``name``.
        name: Name of an Excel based dataset (e.g. "eia860"). Currently this must be
            one of the attributes of :class:`pudl.settings.EiaSettings`
    """
    # Build a Dagster op that can extract a single year of data
    year_extractor = year_extractor_factory(extractor_cls, name)
    # Get the list of target years to extract from the PUDL ETL settings object which is
    # stored in the Dagster context that is available to all ops.
    years_from_settings = years_from_settings_factory(name)

    def raw_dfs() -> dict[str, pd.DataFrame]:
        """Produce a dictionary of extracted EIA dataframes."""
        years = years_from_settings()
        # Clone dagster op for each year using DynamicOut.map()
        # See https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        dfs = years.map(lambda year: year_extractor(year))
        # Collect the results from all of those cloned ops and concatenate the
        # individual years of data into a single multi-year dataframe for each different
        # page in the spreadsheet based dataset using DynamicOut.collect()
        return concat_pages(dfs.collect())

    return graph_asset(name=f"{name}_raw_dfs")(raw_dfs)
