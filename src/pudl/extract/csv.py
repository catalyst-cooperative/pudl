"""Extractor for CSV data."""
import pandas as pd

import pudl.logging_helpers
from pudl.extract.extractor import GenericExtractor

logger = pudl.logging_helpers.get_logger(__name__)


class CsvExtractor(GenericExtractor):
    """Class for extracting dataframes from CSV files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    def __init__(self, ds, dataset_name):
        """Create a new instance of CsvExtractor.

        This can be used for retrieving data from CSV files.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
            dataset_name: Name of the package/dataset to load
        """
        self.ds = ds
        self._dataset_name = dataset_name

    def extract(self, **partition):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            partitions (list, tuple or string): list of partitions to
                extract. (Ex: [2009, 2010])
        """
        raw_dfs = {}
        logger.info(f"Extracting {self._dataset_name} data.")

        page = "data"
        dfs = [
            pd.DataFrame(),
        ]
        # TODO: Use the below commented-out code for consistency? I'm only expecting individual years at this point
        # for partition in pudl.helpers.iterate_multivalue_dict(**partition):
        logger.debug(f"Loading dataframe for {self._dataset_name} {partition}")

        newdata = self.load_csv_file(**partition)
        newdata = pudl.helpers.simplify_columns(newdata)
        # TODO: We might want to consider these steps for CSVs, for consistency
        # newdata = self.process_raw(newdata, page, **partition)
        # newdata = self.process_renamed(newdata, page, **partition)
        dfs.append(newdata)
        # TODO: Look at extracting and reproducing analogous validation
        # check if there are any missing or extra columns
        # str_part = str(list(partition.values())[0])
        # col_map = self.METADATA._column_map[page]
        # page_cols = col_map.loc[
        #     (col_map[str_part].notnull()) & (col_map[str_part] != -1),
        #     [str_part],
        # ].index
        # expected_cols = page_cols.union(self.cols_added)
        # if set(newdata.columns) != set(expected_cols):
        #     # TODO (bendnorman): Enforce canonical fields for all raw fields?
        #     extra_raw_cols = set(newdata.columns).difference(expected_cols)
        #     missing_raw_cols = set(expected_cols).difference(newdata.columns)
        #     if extra_raw_cols:
        #         logger.warning(
        #             f"{page}/{str_part}:Extra columns found in extracted table:"
        #             f"\n{extra_raw_cols}"
        #         )
        #     if missing_raw_cols:
        #         logger.warning(
        #             f"{page}/{str_part}: Expected columns not found in extracted table:"
        #             f"\n{missing_raw_cols}"
        #         )
        df = pd.concat(dfs, sort=True, ignore_index=True)

        # TODO: We're not expecting missing cols based on existing data
        #  but might want to have this as abstracted functionality/contract
        # After all years are loaded, add empty columns that could appear
        # in other years so that df matches the database schema
        # missing_cols = list(
        #     set(self._metadata.get_all_columns(page)).difference(df.columns)
        # )
        # df = pd.concat([df, pd.DataFrame(columns=missing_cols)], sort=True)

        # TODO: I don't think we need this unless we want to consolidate with Excel/generic extractor
        # raw_dfs[page] = self.process_final_page(df=df, page=page)
        raw_dfs[page] = df
        return raw_dfs

    def load_csv_file(self, **partition) -> pd.DataFrame:
        """Produce the dataframe object for the given partition.

        Args:
            partition: partition to load. (ex: 2009)

        Returns:
            pd.DataFrame instance containing CSV data
        """
        filename = self.csv_filename(partition["year"])

        with self.ds.get_zipfile_resource(self._dataset_name) as zf, zf.open(
            filename
        ) as f:
            df = pd.read_csv(f)

        return df

    def csv_filename(self, year: int) -> str:
        """Produce the CSV file name as it will appear in the archive.

        Args:
            year: year to load. (ex: 2009)

        Returns:
            string name of the CSV file
        """
        return f"{self._dataset_name}-{year}.csv"
