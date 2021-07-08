"""Clean and combine FERC1 tables.

each of the tables need to get:

- cleaned
- flag totals
- remove bad rows/headers
- label fuel

"""

import logging

# 3rd party libraries
import numpy as np
import pandas as pd
# import pandas as pd
from tqdm import tqdm

# eventually make this just one function
from pudl.analysis.fill_ferc1_fuel_gaps import add_all_tech, create_groups
from pudl.analysis.flag_ferc1_totals import (flag_steam_totals,
                                             flag_totals_basic)

logger = logging.getLogger(__name__)


########################################################################################
# GLOBAL VARIABLES
########################################################################################

steam_value_cols = [
    'asset_retirement_cost',
    'avg_num_employees',
    'capex_equipment',
    'capex_land',
    'capex_structures',
    'capex_total',
    'opex_allowances',
    'opex_boiler',
    'opex_coolants',
    'opex_electric',
    'opex_engineering',
    'opex_fuel',
    'opex_misc_power',
    'opex_misc_steam',
    'opex_operations',
    'opex_plants',
    'opex_production_total',
    'opex_rents',
    'opex_steam',
    'opex_steam_other',
    'opex_structures',
    'opex_transfer',
    'net_generation_mwh']

small_value_cols = [
    'construction_year',
    'fuel_cost_per_mmbtu',
    'net_generation_mwh',
    'opex_fuel',
    'opex_maintenance',
    'opex_total',
    'total_cost_of_plant']

hydro_value_cols = steam_value_cols + [
    'capex_facilities',
    'capex_roads',
    'opex_dams',
    'opex_generation_misc',
    'opex_hydraulic',
    'opex_misc_plant',
    'opex_water_for_power']

pumped_value_cols = [x for x in hydro_value_cols if x != 'opex_hydraulic'] + [
    'capex_equipment_electric',
    'capex_equipment_misc',
    'capex_wheels_turbines_generators',
    'opex_production_before_pumping',
    'opex_pumped_storage',
    'opex_pumping']


########################################################################################
# CLASSES
########################################################################################


class FERCTable:
    """Create a FERC table.."""

    def __init__(self, name, df, value_cols):
        """Initialize a FERC table."""
        self.name = name
        self.raw = df.copy()
        self.df = df
        self.value_cols = value_cols
        self.status = ['raw']
        self.dropped_cols = pd.DataFrame()

    def clean(self):
        """Clean FERC Table."""
        logger.info(f"Cleaning {self.name} table")
        df = self.df
        na_name = df['plant_name_ferc1'].isin(
            ['--blank--', 'n/a', 'none', 'not applicable'])
        self.dropped_cols = self.dropped_cols.append(
            df[na_name].copy()).drop_duplicates()
        df.drop(df[na_name].index, inplace=True)
        self.status.append('cleaned')

    def flag_totals(self):
        """Flagging FERC Table Totals."""
        logger.info(f"Flagging totals rows for {self.name} table")
        self.status.append('flagged totals')

    def label_fuel(self):
        """Label FERC Table Fuel Types."""
        logger.info(f"Labeling fuel types for {self.name} table")
        self.status.append('labeled fuels')

    def transform(self):
        """Transform FERC table."""
        logger.info(F"\n*** TRANSFORMING {self.name.upper()} TABLE ***")
        self.clean()
        self.flag_totals()
        self.label_fuel()


class SteamTable(FERCTable):
    """Create a FERC steam table."""

    def __init__(self, name, df, value_cols, pudl_out):
        """Initialize a FERC steam table."""
        super().__init__(name, df, value_cols)
        self.pudl_out = pudl_out

    def clean(self):
        """Clean FERC Table."""
        super().clean()
        df = self.df
        bad_plants = [
            "ala.elec. coop.", "al elec coop's port.", "al elec coop's",
            "miss power", "mississippi power 's"]
        is_bad_plant = df['plant_name_ferc1'].isin(bad_plants)
        long_dash_name = df['plant_name_ferc1'].str.contains('---')

        self.dropped_cols = self.dropped_cols.append(
            df[is_bad_plant | long_dash_name].copy()).drop_duplicates()

        df.drop(df[is_bad_plant | long_dash_name].index, inplace=True)
        # df.drop(df[long_dash_name].index, inplace=True)

    def flag_totals(self):
        """Flagging FERC Table."""
        super().flag_totals()
        flag_steam_totals(self.df)

    def label_fuel(self):
        """Label FERC Table Fuel Types."""
        super().label_fuel()
        add_all_tech(self.df, self.pudl_out)


class SmallTable(FERCTable):
    """Create a FERC small gens table."""

    # If a potential header column has these strings, it's probably a useful header
    header_strings = [
        'hydro', 'hyrdo', 'internal', 'wind', 'solar', 'gas', 'diesel', 'diesal',
        'steam', 'other', 'combustion', 'combustine', 'fuel cell', 'hydraulic',
        'waste', 'landfill', 'photovoltaic', 'nuclear', 'oil', 'renewable',
        'facilities', 'combined cycle']
    # RMI header lables
    new_header_labels = {
        'hydroelectric': ['hydro', 'hyrdo'],
        'internal combustion': ['internal', 'interal', 'international combustion', ],
        'combustion turbine': ['combustion turbine'],
        'combined cycle': ['combined cycle'],
        'gas turbine': ['gas'],
        'petroleum liquids': ['oil', 'diesel', 'diesal'],
        'solar': ['solar', 'photovoltaic'],
        'wind': ['wind'],
        'geothermal': ['geothermal'],
        'waste': ['waste', 'landfill'],
        'steam': ['steam'],
        'nuclear': ['nuclear'],
        'fuel_cell': ['fuel cell']}
    # Header names that match the one's that zane used in his manual mapping (so we can
    # compare processes)
    zane_header_labels = {
        'solar_pv': ['solar', 'photovoltaic'],
        'wind': ['wind'],
        'hydro': ['hydro', 'hyrdo'],
        'internal_combustion': ['internal', 'interal', 'international combustion', ],
        'combustion_turbine': ['combustion turbine', 'combustine turbine'],
        'combined_cycle': ['combined cycle'],
        'diesel_turbine': ['oil', 'diesel', 'diesal'],
        'gas_turbine': ['gas'],
        'geothermal': ['geothermal'],
        'waste_heat': ['waste', 'landfill'],
        'steam_heat': ['steam'],
        'nuclear': ['nuclear'],
        'fuel_cell': ['fuel cell']
    }

    def __init__(self, name, df, value_cols):
        """Initialize a FERC small gens table."""
        super().__init__(name, df, value_cols)

    def _add_ferc_lic_col(self, show_proof=False):
        """Extract FERC license number from the plant name and add it to a new column.

        Args:
            df (pandas.DataFrame): The FERC small generators table passed through the
                current transform step and assigned manual ferc license ids.
            show_proof (bool): If True will print out a slice of the dataframe showing any
                instances where the programtically assigned ferc license does not match
                the manually assigned one.

        """
        logger.info("Adding a column for FERC licenses")
        df = self.df

        # Extract all 3-6 digit numbers from the name
        df['ferc_license'] = (
            df.plant_name_original.str.extract(r'(\d{3,6})')  # noqa: FS003
            .astype('float').astype('Int64'))  # annying but have to do it twice

        # Some of these numbers are years not licenses so they must be removed.
        # Years are defined here as between 1900 and 2020
        # The names included here are names of plants that don't have an indicator that the number
        # is a license (ie: #, no. ferc, etc.) and are also "year" numbers. They must be called
        # out individually to prevent their becoming nan.
        no_license = (~df['plant_name_original'].str.contains(
            'no.|license|#|tomahawk|otter rapids|wausau|alexander'))
        no_license_strict = ~df['plant_name_original'].str.contains('no.|license|ferc')
        year_vs_num = (df['ferc_license'] > 1900) & (df['ferc_license'] < 2020)
        other_non_license = df['plant_name_original'].str.contains(
            'surrendered|nonutility')
        three_digit_license = df['ferc_license'] < 1000

        # Replace all the non-license numbers with nan
        df.loc[no_license & year_vs_num, 'ferc_license'] = np.nan
        df.loc[other_non_license, 'ferc_license'] = np.nan
        # Three digit numbers are a little more finnicky so we are going to make the license
        # specifications stricter -- i.e. it must contain specific licensing language in
        # addition to the number
        df.loc[three_digit_license & no_license_strict, 'ferc_license'] = np.nan

        df = df.rename(columns={'ferc_license_id': 'ferc_license_manual'})
        df['ferc_license_manual'] = df.ferc_license_manual.astype('Int64')
        # figure out how not to do this twice....
        df['ferc_license'] = df.ferc_license.astype('Int64')

        logger.info(f" - manually added licenses: \
            {len(df[df['ferc_license_manual'].notna()])}")
        logger.info(f" - programatically scraped licenses: \
            {len(df[df['ferc_license'].notna()])}")

        # Proof -- two mapped license records don't match my programatic records, but I
        # think mine are right:
        if show_proof:
            only_manual = df['ferc_license_manual'].notna()
            not_matching_ids = df['ferc_license_manual'] != df['ferc_license']
            print("\nPROOF: ROWS WHERE PROG ID DOES NOT MATCH MANUAL ID: \n",
                  df[only_manual & not_matching_ids][[
                      'plant_name_ferc1', 'report_year', 'ferc_license_manual',
                      'ferc_license']], "\n")

    def _label_possible_headers(self):

        logger.info("Labeling possible headers")
        df = self.df

        # If a potential header has these strings, it's not a header...
        exclude = [
            '#', r'\*', 'pg', 'solargenix', 'solargennix', r'\@', 'rockton',
            'albany steam']
        # ...unless it also has one of these strings
        exceptions = [
            'hydro plants: licensed proj. no.', 'hydro license no.',
            'hydro: license no.', 'hydro plants: licensed proj no.']
        # What we will rename the headers once we remove them as rows

        # Label possible header rows (based on the nan cols specified above)
        df.insert(3, 'is_header', False)
        df.insert(3, 'header_type', np.nan)
        df.loc[df.filter(small_value_cols).isna().all(1), 'is_header'] = True

        # Label good header rows (based on whether they contain key strings)
        is_header = df['is_header']
        is_good_header = df['plant_name_original'].str.contains(
            '|'.join(self.header_strings))
        not_bad = ~df['plant_name_original'].str.contains('|'.join(exclude))

        df.loc[is_header & is_good_header & not_bad, 'header_type'] = 'good_header'
        df.loc[df['plant_name_original'].isin(
            exceptions), 'header_type'] = 'good_header'

    def _label_header_clumps_all(self):
        """
        Remove clumps of consecutive rows flagged as possible headers.

        FERC has lots of note rows that are not headers but are also not useful for
        analysis. This function looks for rows flagged as possible headers (based on NAN
        values) and checks to see if there are multiple in a row. A header row is
        (usually) defined as a row with NAN values followed by rows without NAN values,
        so when there are more than one clumped together they are likely either notes or
        not helpful.

        Sometimes note clumps will end with a meaningful header. This function also
        checks for this and will unclump any headers at the bottom of clumps. There is
        one exception to this case which is a header that is followed by a plant that
        had no values reported... Unfortunately I haven't built a work around, but
        hopefully there aren't very many of these. Currently, that header and plant will
        be categorized as clumps and removed.

        """
        logger.info("Identifying header clumps...this'll take a second")
        df = self.df
        util_year_groups = df.groupby(['utility_id_ferc1', 'report_year'])

        def label_header_clumps(util_year_group):

            # Create mini groups that count pockets of true and false for each utility
            # and year create_groups() is a function from the fill_ferc1_fuel_gaps
            # module-- basically what it does is create a df where each row represents a
            # clump of adjecent, equal values for a given column. Ex: a column of True,
            # True, True, False, True, False, False, will appear as True, False, True,
            # False with value counts for each
            group, header_count = create_groups(util_year_group, 'is_header')

            # These are used later to enable exceptions
            # max_idx_val = header_count.index.max()
            max_df_val = util_year_group.index.max()

            # Create a list of the index values that comprise each of the header clumps
            # It's only considered a clump if it is greater than 1.
            idx_list = list(header_count[
                (header_count['fuel']) & (header_count['val_count'] > 1)].index)

            # If the last row is not a clump (i.e. there is just one value) but it is a
            # header (i.e. has nan values) then also include it in the index values to
            # be flagged because it might be a one-liner note. And because it is at the
            # bottom there is no chance it can actually be a useful header because there
            # are no value rows below it.
            last_row = header_count.tail(1)
            if (last_row['fuel'].item()) & (last_row['val_count'].item() == 1):
                idx_list = idx_list + list(last_row.index)
            # If there are any clumped/end headers:
            if idx_list:
                for idx in idx_list:
                    # Check to see if last clump bit is not a header... sometimes you
                    # might find a clump of notes FOLLOWED by a useful header. This next
                    # bit will check the last row in each of the identified clumps and
                    # "unclump" it if it looks like a valid header. We only need to
                    # check clumps that fall in the middle because, as previously
                    # mentioned, the last row cannot contain any meaningful header
                    # information because there are no values below it.
                    idx_range = group.groups[idx + 1]
                    is_middle_clump = group.groups[idx + 1].max() < max_df_val
                    is_good_header = (
                        util_year_group.loc[util_year_group.index.isin(
                            group.groups[idx + 1])]
                        .tail(1)['plant_name_original']
                        .str.contains('|'.join(self.header_strings)).all())

                    # If the clump is in the middle and the last row looks like a
                    # header, then drop it from the idx range
                    if is_middle_clump & is_good_header:
                        idx_range = [x for x in idx_range if x != idx_range.max()]
                    # Label the clump as a clump
                    util_year_group.loc[
                        util_year_group.index.isin(idx_range), 'header_type'] = 'clump'
            return util_year_group

        # Add progress bar as you run through the clump identification
        tqdm.pandas()
        self.df = util_year_groups.progress_apply(lambda x: label_header_clumps(x))

        logger.info(" - validating clump findings")
        assert 'clump' in self.df.header_type.unique(), 'no clumps!'

    def _show_header_assumptions(self, show_bad_headers=True, show_good_headers=True):

        # List of strings from the plant name column that did or didn't make the cut as
        # headers after removing all the clump rows
        if show_bad_headers:
            bad_headers = list(self.df[
                (self.df['is_header'])
                & (self.df['header_type'].isna())].plant_name_original.unique())
            bad_headers.sort()
            print("\nPOSSIBLE HEADERS THAT DIDN'T MAKE THE CUT:\n",
                  "\n".join(bad_headers), "\n")

        if show_good_headers:
            good_headers = list(self.df[
                self.df['header_type'] == 'good_header'].plant_name_original.unique())
            good_headers.sort()
            print("\nPOSSIBLE HEADERS THAT MADE THE CUT\n:",
                  "\n".join(good_headers), "\n")

    def _create_header_col(self, header_labels, drop_headers=True):
        """
        Group the data by header and create header column.

        This function gets rid of the clumps and turns the header row into a header column.
        It does this by grouping the data into groupby object based on the presence of "good"
        header columns throughout the data. Each group begins with a good header and ends
        right before the next one, if applicable. This doesn't *always* work (see utility id
        120 year 2011) but that's usually because of bad reporting not a program glitch.

        The function also cleans up the header names.

        remove_headers may be switched to false if you want to double check how well
        the program maps headers / whether there are any glitches.

        """
        logger.info("Assigning headers to groups")
        df = self.df

        # Start by dropping the clumps
        df = df.drop(df[(df['is_header']) & (df['header_type']
                     == 'clump')].index).reset_index(drop=True)

        # Turn good headers into booleans
        df['is_good_header'] = False
        df.loc[df['header_type'] == 'good_header', 'is_good_header'] = True

        # Clean up header names - made a new col just to preserve any of the original
        # names
        for tech in header_labels:
            is_good_header = df['is_good_header']
            has_tech = df['plant_name_original'].str.contains(
                '|'.join(header_labels[tech]))
            df.loc[is_good_header & has_tech, 'header_prep_clean'] = tech

        # Note likely header rows that haven't been categorized
        not_mapped = df[(df['header_prep_clean'].isna()) & (
            df['is_good_header'])]['plant_name_original'].unique()
        print(' - likely headers that have not been mapped:', not_mapped)

        # Make groups based on the year and whether there is a header
        logger.info(" - creating header groups")
        header_groups = df.groupby(
            ['utility_id_ferc1', 'report_year', df['is_good_header'].cumsum()])

        # Assign that header to the rows that follow
        def _assign_header(header_group):
            header_value = header_group[
                header_group['is_good_header']]['header_prep_clean']
            if not header_value.empty:
                header_group.insert(3, 'header', header_value.item())
            return header_group

        logger.info(" - assigning headers to groups")
        self.df = header_groups.apply(lambda x: _assign_header(x))

    def _add_obvious_headers(self, header_labels, show_assumptions=False):
        """Add plant type when included in plant name.

        When plants have their tech type embeded in their name it's fairly safe to
        assume that that is also the given plant type. This isn't always the case,
        however, and this function also accounts for some of those outliers.

        """
        logger.info("Labeling all obvious headers")
        df = self.df
        # wierd plants that have two fuel/plant types in their name.
        exceptions = {
            np.nan: ['windsor rd', 'gaston', 'sc-etwind'],
            'solar_pv': ['las vegas solar'],
            'gas_turbine': ['solar centaur']}

        if show_assumptions:
            for tech in header_labels:
                print('')
                print('header: ', tech)
                print('qualifying strings: ', header_labels[tech])
                print('strings in the data:')
                print(df[df['plant_name_original'].str.contains(
                    '|'.join(header_labels[tech]))]
                    .plant_name_original.sort_values().unique())
            for tech in exceptions:
                print('the following exceptions are made for plants that do not fit \
                       into these criteria:')
                print('header: ', tech)
                print('qualifying strings: ', exceptions[tech])

        # label all "obvious" plant types
        logger.info(" - labeling all headers with a tech name in their name")
        for tech in header_labels:
            df.loc[df['plant_name_original'].str.contains(
                '|'.join(header_labels[tech])), 'header'] = tech

        # fix ones that aren't so "obvious"
        for tech in exceptions:
            df.loc[df['plant_name_original'].str.contains(
                '|'.join(exceptions[tech])), 'header'] = tech

        logger.info(" - labeling all records with a ferc license hydro")
        # Mark any plant with a license number as hydro
        df.loc[df['ferc_license'].notna(), 'header'] = 'hydro'

    def _show_header_stats(self):

        df = self.df
        # Create a new column that merges zane's manual labeling with my programatic labeling
        df.insert(3, 'header_manual_code_combo', df.header.fillna(df.plant_type))

        print('')
        print('header matches manual plant type:', len(
            df[df['header'] == df['plant_type']]))
        print('total manual plant types:', len(df[df['plant_type'].notna()]))
        print('total headers mapped:', len(df[df['header'].notna()]))
        print('total headers with manual:', len(
            df[df['header_manual_code_combo'].notna()]))
        print('total rows:', len(df))
        print('')

    def clean(self):
        """Clean FERC Table."""
        super().clean()
        logger.info(f" - Pre-clean length: {len(self.df)}")
        # It's important to drop these values BEFORE running the header_to_col function
        # because some of the dash lines come below a header row (ex: row 1: 'hydro',
        # row 2: '--------'. This is a sad programatic interpretation of an underlined
        # header... The current header recognition function will interpret these two
        # rows as possible headers....

        # MAYBE WE CAN GET RID OF THIS IF AND ONLY IF THERE IS A WAY TO FLAG HEADERS AND THEN REMOVE ALL NON
        # OFFICIAL HEADERS AND THEN RUN THE HEADER TO COLUMN THING....?)
        long_dash_name = self.df['plant_name_ferc1'].str.contains('---')

        self.dropped_cols = self.dropped_cols.append(
            self.df[long_dash_name].copy()).drop_duplicates()

        self.df.drop(self.df[long_dash_name].index, inplace=True)
        logger.info(f" - Post-clean length: {len(self.df)}")

    def flag_totals(self):
        """Flagging FERC Table Totals."""
        super().flag_totals()
        flag_totals_basic(self.df)

    def label_fuel(self):
        """Label FERC Table Fuel Types."""
        super().label_fuel()
        self._add_ferc_lic_col(show_proof=True)
        self._label_possible_headers()
        self._label_header_clumps_all()
        self._show_header_assumptions(show_bad_headers=False, show_good_headers=False)
        self._create_header_col(self.zane_header_labels)
        self._add_obvious_headers(self.zane_header_labels)
        self._show_header_stats()


class HydroTable(FERCTable):
    """Create a FERC hydro table."""

    def __init__(self, name, df, value_cols):
        """Initialize a FERC hydro table."""
        super().__init__(name, df, value_cols)

    def flag_totals(self):
        """Flagging FERC Table Totals."""
        super().flag_totals()
        flag_totals_basic(self.df)
