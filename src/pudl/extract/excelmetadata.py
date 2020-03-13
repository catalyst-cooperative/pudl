"""Load excel metadata CSV files form a python data package."""

import importlib.resources

import pandas as pd


class ExcelMetadata(object):
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

    TODO: There may be additional file complexity here, but not necessarily
    for simple xlsx datasets.
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
        # TODO: we can assert that page in self.all_pages()
        return set(self._column_map[page].columns).difference(self._drop_columns)

    def all_pages(self):
        """Returns list of supported pages."""
        return self._column_map.keys()

    def _load_csv(self, pkg, filename):
        """Load metadata from CSV file from a python package."""
        return pd.read_csv(importlib.resources.open_text(pkg, filename),
                           index_col=0, comment='#')

    def __init__(self, dataset_name, drop_columns=None):
        """Loads CSV metadata and constructs ExcelMetadata object."""
        pkg = f'pudl.package_data.meta.xlsx_maps.{dataset_name}'
        self._skiprows = self._load_csv(pkg, 'skiprows.csv')
        self._sheet_name = self._load_csv(pkg, 'tab_map.csv')
        self._drop_columns = []
        if drop_columns:
            self._drop_columns = list(drop_columns)
        # TODO(rousik@gmail.com): there may be multiple tab_maps per file (?)
        # TODO(rousik@gmail.com): iterate over pkg + '.column_maps' and load all the column
        # maps. Page name matches the filename (assume this is always possible).
        column_map_pkg = pkg + '.column_maps'
        self._column_map = {}
        for res in importlib.resources.contents(column_map_pkg):
            # res is expected to be ${page}.csv
            parts = res.split('.')
            if len(parts) != 2 or parts[1] != 'csv':
                continue
            column_map = self._load_csv(column_map_pkg, res)
            self._column_map[parts[0]] = column_map
