"""
Retrieve data from EIA Form 861 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 861 data.

"""

import importlib.resources
import logging
import pathlib

import pandas as pd

import pudl
import pudl.workspace.datastore as datastore
from pudl import constants as pc

logger = logging.getLogger(__name__)


class ExtractorExcel(object):
    """A class for converting Excel files into DataFrames."""

    def __init__(self, dataset_name, years, pudl_settings):
        """
        Initilize the extractor object.

        Args:
            dataset_name (str) : the pudl-used name of the dataset to be
                extracted. For a list of available datasets, see
                pudl.constants.data_sources.
            years (iterable) : list of years that are extractable by your dataset
            pudl_settings (dict) : a dictionary filled with settings that mostly
                describe paths to various resources and outputs.

        """
        self.xlsx_dict = {}
        self.dataset_name = dataset_name
        self.years = years
        self.data_dir = pudl_settings['data_dir']

    def get_meta(self, meta_name, file_name):
        """
        Grab the metadata file.

        Args:
            meta_name (str): the name of the top level metadata.
            file_name (str): if the metadata is in a nested subdirectory (such
                as 'column_maps' or 'tab_maps') the file_name is the file name.
                This name should correspond to the name of the Excel file being
                extracted.

        Returns:
            pandas.DataFrame

        """
        xlsx_maps_dataset = f'pudl.package_data.meta.xlsx_maps.{self.dataset_name}'
        if meta_name in ('column_maps', 'tab_maps'):
            path = importlib.resources.open_text(
                (xlsx_maps_dataset + '.' + meta_name), f'{file_name}.csv')
        else:
            path = importlib.resources.open_text(
                xlsx_maps_dataset, f'{meta_name}.csv')
        file_df = pd.read_csv(path, index_col=0, comment='#')
        return file_df

    def get_path_name(self, yr, file_name):
        """Get the ExcelFile file path name."""
        return self.get_meta('file_name_map', None).loc[yr, file_name]

    def get_file(self, yr, file_name):
        """
        Construct the appopriate path for a given EIA860 Excel file.

        Args:
            year (int): The year that we're trying to read data for.
            file_name (str): A string containing part of the file name for a
                given EIA 860 file (e.g. '*Generat*')

        Returns:
            str: Path to EIA 861 spreadsheets corresponding to a given year.

        Raises:
            ValueError: If the requested year is not in the list of working
                years for EIA 861.

        """
        if yr not in pc.working_years[self.dataset_name]:
            raise ValueError(
                f"Requested non-working {self.dataset_name} year: {yr}.\n"
                f"{self.dataset_name} is only working for: {pc.working_years[self.dataset_name]}\n"
            )

        eia860_dir = datastore.path(self.dataset_name, year=yr, file=False,
                                    data_dir=self.data_dir)
        eia860_file = pathlib.Path(
            eia860_dir, self.get_path_name(yr, file_name))

        return eia860_file

    def get_xlsx_dict(self, years, file_name):
        """Read in Excel files to create Excel objects.

        Rather than reading in the same Excel files several times, we can just
        read them each in once (one per year) and use the ExcelFile object to
        refer back to the data in memory.

        Args:
            years (list): The years that we're trying to read data for.
            file_name (str): Name of the excel file.

        """
        for yr in years:
            try:
                self.xlsx_dict[yr]
                logger.info(f"we already have an xlsx file for {yr}")
            except KeyError:
                logger.info(
                    f"Extracting data from {self.dataset_name} {file_name} spreadsheet for {yr}.")
                self.xlsx_dict[yr] = pd.ExcelFile(
                    self.get_file(yr, file_name)
                )

    def get_column_map(self, year, file_name, page_name):
        """
        Given a year and page, returns info needed to slurp it from Excel.

        Args:
            year (int)
            file_name (str)
            page_name (str)

        Returns:
            sheet_loc
            skiprows
            column_map
            all_columns

        """
        skiprows = self.get_meta('skiprows', None).loc[year, file_name]
        sheet_loc = self.get_meta('tab_maps', file_name).loc[year, page_name]
        col_loc = self.get_meta('column_maps', file_name).loc[year].to_dict()

        # invert the col_location dictionary
        column_map = {}
        for k, v in col_loc.items():
            column_map[v] = k

        all_columns = list(col_loc.keys())

        return (sheet_loc, skiprows, column_map, all_columns)

    def get_page(self, years, page_name, file_name):
        """
        Get a page from years of excel files and convert them to a DataFrame.

        Args:
            years (list)
            page_name (str)
            file_name (str)
        """
        if page_name not in self.get_meta('tab_maps', file_name).columns:
            raise AssertionError(
                f"Unrecognized {self.dataset_name} page: {page_name}\n"
                f"Acceptable {self.dataset_name} pages: {list(self.get_meta('tab_maps',file_name).columns)}\n"
            )

        df = pd.DataFrame()
        for year in years:
            if year not in pc.working_years[self.dataset_name]:
                raise AssertionError(
                    f"Requested non-working {self.dataset_name} year: {year}.\n"
                    f"{self.dataset_name} works for {pc.working_years[self.dataset_name]}\n"
                )
            logger.info(f"Converting {self.dataset_name} spreadsheet tab {page_name} to pandas "
                        f"DataFrame for {year}.")

            sheet_loc, skiprows, column_map, all_columns = self.get_column_map(
                year, file_name, page_name)

            dtype = {}
            if 'zip_code' in list(all_columns):
                dtype['zip_code'] = pc.column_dtypes['eia']['zip_code']

            newdata = pd.read_excel(self.xlsx_dict[year],
                                    sheet_name=sheet_loc,
                                    skiprows=skiprows,
                                    dtype=dtype,
                                    )

            # if we are using the column position, we don't need this
            newdata = pudl.helpers.simplify_columns(newdata)

            # we're functionally remaking the column map in here so we can
            # use the position of the column not just the column name
            newdata = newdata.rename(columns=dict(
                zip(newdata.columns[list(column_map.keys())], list(column_map.values()))))

            # boiler_generator_assn tab is missing a YEAR column. Add it!
            if 'report_year' not in newdata.columns:
                newdata['report_year'] = year
            df = df.append(newdata, sort=True)

        # We need to ensure that ALL possible columns show up in the dataframe
        # that's being returned, even if they are empty, so that we know we have a
        # consistent set of columns to work with in the transform step of ETL, and
        # the columns match up with the database definition.
        missing_cols = [x for x in all_columns if x not in list(df.columns)]
        empty_cols = pd.DataFrame(columns=missing_cols)
        df = pd.concat([df, empty_cols], sort=True)
        return df

    def create_dfs(self, years):
        """
        Create a dict of pages (keys) to DataDrames (values) from a dataset.

        Args:
            years (list): a list of years

        Returns:
            dict: A dictionary of pages (key) to DataFrames (values)
        """
        # Prep for ingesting the excel files
        # Create excel objects
        # TODO: right now this is very much mirrored on the 860 extraction
        # process...we will probably want to do something other than accumulate
        # thedfs in a dictionary.
        self.dfs = {}
        for file_name in (self.get_meta('file_name_map', None).columns):
            # generate the xlsx dict
            self.get_xlsx_dict(years, file_name)
            # Create DataFrames
            file_pages = self.get_meta(
                'file_page_map', file_name).loc[file_name, ]['page']
            # if there is just on instance of the page, then this we need to put
            # the resulting string into a list so we can iterate over it.
            if isinstance(file_pages, str):
                file_pages = [file_pages]
            for page_name in file_pages:
                self.dfs[file_name + '_' + page_name] = self.get_page(
                    years, page_name, file_name)
        return self.dfs
