"""Clean up the data from FERC Form 1.

The data from FERC Form 1 is notoriously bad. The small generators table, for example,
contains rows that represent different types of information: headers, values, and notes.
It's like someone took a paper form, copied it into Microsoft Excel, and called it a
day. Needless to say, it needs a lot of help before it can be used programatically for
bulk analysis.

This module is indented as a hub for some of the more drastic cleaning measures
required to make the data from FERC Form 1 useful.
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

########################################################################################
# --------------------------------------------------------------------------------------
#  S M A L L  G E N E R A T O R S  T A B L E
# --------------------------------------------------------------------------------------
########################################################################################

# If these columns are nan, we can assume it is either a header row or isn't useful
nan_cols = [
    'construction_year',
    'net_generation_mwh',
    'total_cost_of_plant',
    'capex_per_mw',
    'opex_total',
    'opex_fuel',
    'opex_maintenance',
    'fuel_cost_per_mmbtu'
]

# If a potential header column has these strings, it's probably a useful header
header_strings = [
    'hydro',
    'hyrdo',
    'internal',
    'wind',
    'solar',
    'gas',
    'diesel',
    'diesal',
    'steam',
    'other',
    'combustion',
    'combustine',
    'fuel cell',
    'hydraulic',
    'waste',
    'landfill',
    'photovoltaic',
    'nuclear',
    'oil',
    'renewable',
    'facilities',
    'combined cycle'
]

# If a potential header has these strings, it is not a header...
exclude = [
    '#',
    r'\*',
    'pg',
    'solargenix',
    'solargennix',
    r'\@',
    'rockton',
    'albany steam'
]

# ...unless it also has one of these strings
exceptions = [
    'hydro plants: licensed proj. no.',
    'hydro license no.',
    'hydro: license no.',
    'hydro plants: licensed proj no.'
]

# plants with two fuel names in the plant_name
two_fuel_names_dict = {
    'las vegas solar': 'solar_pv',
    'solar centaur': 'gas_turbine'
}

# What we will rename the headers once we remove them as rows
new_header_labels = {
    'hydroelectric': ['hydro', 'hyrdo'],
    'internal combustion': ['internal', 'interal', 'international combustion'],
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
    'fuel_cell': ['fuel cell'],
    'other': ['other'],
    'renewables': ['renewables'],
}

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
    'fuel_cell': ['fuel cell'],
}


########################################################################################
# Helper Functions
########################################################################################

def expand_dict(dic):
    """Change format of header_labels."""
    d = {}
    for k, lst in dic.items():
        for i in range(len(lst)):
            d[lst[i]] = k
            # new_d = d.copy()
            # l.append(new_d)
    return d


def show_removed_rows(df_pre_drop, df_post_drop, cols, message, view=None):
    """Show rows that were removed.

    Args:
        df_pre_drop (pandas.DataFrame): The df before dropping rows.
        df_post_drop (pandas.DataFrame): the df post dropping rows.
        cols (list): A list of the column names you'd like to view as part of
            the review.
        message (str): The print out message that describes what you are seeing.
        view (str): Either 'info' or 'value_counts' depending on what analysis you
            want to see

    """
    assert view in ['info', 'value_counts'], 'view must be info or value_counts'
    removed_rows = (
        pd.concat([df_pre_drop, df_post_drop])
        .drop_duplicates(keep=False))
    if view == 'info':
        print('\n', message, '\n')
        print(removed_rows[cols].info(), '\n')
    if view == 'value_counts':
        print('\n', message, '\n\n', removed_rows[cols].value_counts(), '\n')


########################################################################################
# Analysis Functions
########################################################################################

def remove_bad_rows(sg_df, show_removed=False):
    """Test."""
    # Remove utilities with all NAN rows because these won't contain anything meaningful
    logger.info("Removing rows where an entire utility has reported NA in key columns")
    sg_clean1 = (
        sg_df.groupby('utility_id_ferc1')
        .filter(lambda x: ~x[nan_cols].isna().all().all()))

    # Remove rows with three or more dashes for plant name
    logger.info("Removing rows with three or more dashes for plant name")
    sg_clean2 = sg_clean1[~sg_clean1['plant_name_ferc1'].str.contains('---')].copy()

    # Remove rows with NA for plant names
    logger.info("Removing rows with NA for plant name")
    na_names_list = ['', 'none', 'na', 'n/a', 'not applicable']
    sg_clean3 = sg_clean2[~sg_clean2['plant_name_ferc1'].isin(na_names_list)].copy()

    if show_removed:
        show_removed_rows(sg_df, sg_clean1, nan_cols,
                          'REMOVED NAN VALUES:', view='info',)
        show_removed_rows(sg_clean1, sg_clean2, ['plant_name_ferc1'],
                          'REMOVED DASH NAMES:', view='value_counts')
        show_removed_rows(sg_clean2, sg_clean3, ['plant_name_ferc1'],
                          'REMOVED NA NAMES:', view='value_counts')

    return sg_clean3


def _label_total_rows(sg_df):
    """Label total rows."""
    logger.info("Labeling total rows")
    sg_df.loc[sg_df['plant_name_ferc1'].str.contains('total'), 'row_type'] = 'total'

    # Now deal with some outliers:

    # Move total row values that are reported in notes row below to correct row above
    # and null the values in the notes row.
    num_cols = [
        x for x in sg_df.select_dtypes(include=['float', 'Int64']).columns.tolist(
        ) if x not in ['utility_id_ferc1', 'report_year', 'ferc_license_id']]
    bad_row = sg_df[(sg_df['plant_name_ferc1'].str.contains(
        "amounts are for")) & (sg_df['capacity_mw'] > 0)]
    assert len(bad_row) == 1, 'found more bad rows than expected'

    sg_df.loc[bad_row.index, num_cols] = sg_df.loc[bad_row.index - 1][num_cols].values
    sg_df.loc[bad_row.index - 1, num_cols] = np.nan

    return sg_df


def _label_header_rows(sg_df):
    """Label header rows.

    This function alone will label lots of notes rows as headers. It must be superceded
    by the notes labeling function.

    """
    logger.info("Labeling header rows")

    # Label possible header rows (based on the nan cols specified above)
    sg_df.loc[sg_df.filter(nan_cols).isna().all(1), 'possible_header'] = True

    # Label good header rows (based on whether they contain key strings)
    possible_header = sg_df['possible_header']
    good_header = sg_df['plant_name_ferc1'].str.contains('|'.join(header_strings))
    not_bad = ~sg_df['plant_name_ferc1'].str.contains('|'.join(exclude))

    sg_df.loc[possible_header & good_header & not_bad, 'row_type'] = 'header'
    sg_df.loc[sg_df['plant_name_ferc1'].isin(exceptions), 'row_type'] = 'header'

    return sg_df


def _find_header_clumps(group, group_col):
    """Count groups of headers in a given utiltiy group.

    This function takes a utility group and regroups it by rows where
    possible_header = True (i.e.: all values in the specified nan_cols are NA)
    vs. False. Rows where possible_header = True can be bad data, headers, or notes.
    The result is a DataFrame that contains one row per clump of similar adjecent
    possible_header values with columns val_col depicting the number of rows per
    possible_header clump.

    Ex: If you pass in a df with the possible_header values: True, False False, True,
    True, the header_groups output df will look like this: {'header':[True, False,
    True], 'val_col: [1, 2, 2]}.

    Args:
        group (pandas.DataFrameGroupBy): A groupby object that you'd like to condense
            by group_col
        group_col (str): The name of the column you'd like to make sub
            groups from.

    Returns:
        pandas.DataFrame: A condensed version of that dataframe input grouped by
            breaks in header row designation.

    """
    # Make groups based on consecutive sections where the group_col is alike.
    header_groups = group.groupby((group[f'{group_col}'].shift() !=
                                   group[f'{group_col}']).cumsum(), as_index=False)

    # Identify the first (and only) group_col value for each group and count how many
    # rows are in each group.
    header_groups_df = header_groups.agg(
        header=(f'{group_col}', 'first'), val_count=(f'{group_col}', 'count'))

    return header_groups, header_groups_df


def _label_notes_rows(sg_df):
    """
    Remove clumps of consecutive rows flagged as possible headers.

    FERC has lots of note rows that are not headers but are also not useful for
    analysis. This function looks for rows flagged as possible headers (based on NAN
    values) and checks to see if there are multiple in a row. A header row is (usually)
    defined as a row with NAN values followed by rows without NAN values, so when there
    are more than one clumped together they are likely either notes or not helpful.

    Sometimes note clumps will end with a meaningful header. This function also checks
    for this and will unclump any headers at the bottom of clumps. There is one
    exception to this case which is a header that is followed by a plant that had no
    values reported... Unfortunately I haven't built a work around, but hopefully there
    aren't very many of these. Currently, that header and plant will be categorized as
    clumps and removed.

    """
    logger.info("Labeling notes rows")

    util_groups = sg_df.groupby(['utility_id_ferc1', 'report_year'])

    def _label_notes_rows_group(util_year_group):
        """Find an label notes rows in a designated sub-group of the sg table.

        Utilities report to FERC on a yearly basis therefore it is on a utility and
        yearly basis by which we need to parse the data. Each year for each utility
        appears in the data like a copy of a pdf form. If you are looking for rows that
        are notes or headers, this context is extremely important. For example. Flagged
        headers that appear at the bottom of a given utility-year subgroup are notes
        rather than headers strictly due to their location in the group. For this
        reason, we must parse the notes from the header groups at the utility-year
        level rather than the dataset as a whole.

        Args:
            util_year_group (pandas.DataFrame): A groupby object that contains a single
                year and utility.

        """
        # Create mini groups that count pockets of true and false for each utility and
        # year create_groups() is a function from the fill_ferc1_fuel_gaps module--
        # basically what it does is create a df where each row represents a clump of
        # adjecent, equal values for a given column. Ex: a column of True, True, True,
        # False, True, False, False, will appear as True, False, True, False with value
        # counts for each
        group, header_count = _find_header_clumps(
            util_year_group, 'possible_header')

        # Used later to enable exceptions
        max_df_val = util_year_group.index.max()

        # Create a list of the index values that comprise each of the header clumps
        # It's only considered a clump if it is greater than 1.
        idx_list = list(header_count[
            (header_count['header']) & (header_count['val_count'] > 1)].index)

        # If the last row is not a clump (i.e. there is just one value) but it is a
        # header (i.e. has nan values) then also include it in the index values to be
        # flagged because it might be a one-liner note. And because it is at the bottom
        # there is no chance it can actually be a useful header because there are no
        # value rows below it.
        last_row = header_count.tail(1)
        if (last_row['header'].item()) & (last_row['val_count'].item() == 1):
            idx_list = idx_list + list(last_row.index)
        # If there are any clumped/end headers:
        if idx_list:
            for idx in idx_list:
                # Check to see if last clump bit is not a header... sometimes you might
                # find a clump of notes FOLLOWED by a useful header. This next bit will
                # check the last row in each of the identified clumps and "unclump" it
                # if it looks like a valid header. We only need to check clumps that
                # fall in the middle because, as previously mentioned, the last row
                # cannot contain any meaningful header information because there are no
                # values below it.
                idx_range = group.groups[idx + 1]
                is_middle_clump = group.groups[idx + 1].max() < max_df_val
                is_good_header = (
                    util_year_group.loc[
                        util_year_group.index.isin(group.groups[idx + 1])]
                    .tail(1)['plant_name_ferc1']
                    .str.contains('|'.join(header_strings))
                    .all())
                # If the clump is in the middle and the last row looks like a header,
                # then drop it from the idx range
                if is_middle_clump & is_good_header:
                    idx_range = [x for x in idx_range if x != idx_range.max()]
                # Label the clump as a clump
                util_year_group.loc[
                    util_year_group.index.isin(idx_range), 'row_type'] = 'note'

        return util_year_group

    return util_groups.apply(lambda x: _label_notes_rows_group(x))


def label_row_type(sg_df):
    """Label rows as headers, notes, or totals.

    This function coordinates all of the row labeling functions.
    """
    # Add some new helper columns
    sg_df.insert(3, 'possible_header', False)
    sg_df.insert(3, 'row_type', np.nan)

    # Label the row types
    sg_labeled = (
        sg_df.pipe(_label_header_rows)
        .pipe(_label_total_rows)
        .pipe(_label_notes_rows)
        .drop(columns=['possible_header']))

    return sg_labeled


def _map_header_fuels(sg_df, show_unmapped_headers=False):
    """Apply the fuel type indicated in the header row to the relevant rows.

    This function groups the data by utility, year, and header and forward fills the
    cleaned technology type based on that.

    """
    logger.info("Mapping header fuels to relevant rows")

    # Clean header names
    sg_df['header_clean'] = np.nan
    d = expand_dict(zane_header_labels)

    # Map cleaned header names onto df in a new column
    sg_df.loc[sg_df['row_type'] == 'header', 'header_clean'] = (
        sg_df['plant_name_ferc1']
        .str.extract(fr"({'|'.join(d.keys())})", expand=False)
        .map(d))

    # Make groups based on utility, year, and header
    header_groups = sg_df.groupby(
        ['utility_id_ferc1', 'report_year', (sg_df['row_type'] == 'header').cumsum()])

    # Forward fill based on headers
    sg_df['fuel_type'] = np.nan
    sg_df.loc[sg_df['row_type'] != 'note', 'fuel_type'] = (
        header_groups.header_clean.ffill())

    if show_unmapped_headers:
        print(sg_df[(sg_df['row_type'] == 'header')
                    & (sg_df['header_clean'].isna())]
              .plant_name_ferc1.value_counts())

    return sg_df


def _map_plant_name_fuels(sg_df, show_labels=False):
    """Get fuel type from plant name."""
    logger.info("Getting fuel type from plant name")

    # Check for non-labeled hydro in name
    non_labeled_hydro = sg_df[
        (sg_df['fuel_type'] != 'hydro')
        & (sg_df['row_type'] != 'note')
        & (sg_df['plant_name_ferc1'].str.contains('hydro'))]

    if not non_labeled_hydro.empty:
        # Fill in hydro
        not_note = sg_df['row_type'] != 'note'
        contains_hydro = sg_df['plant_name_ferc1'].str.contains('hydro')
        sg_df.loc[not_note & contains_hydro, 'fuel_type'] = 'hydro'

    if show_labels:
        print(non_labeled_hydro.plant_name_ferc1.value_counts())

    return sg_df


def improve_fuel_type(sg_df):
    """Pull fuel type from header rows and plant name."""
    sg_fuel = (
        sg_df.pipe(_map_header_fuels)
        .pipe(_map_plant_name_fuels)
        .drop(columns=['header_clean']))

    return sg_fuel


def extract_ferc_license(sg_df):
    """Extract FERC license number from plant_name.

    Many of FERC license numbers are embedded in the plant_name_ferc1 field, whether
    thats a note row or an actual plant name. This function extracts those license
    numbers and puts them in a new column. There are still more licenses that are
    referenced as notes at the bottom of a utility-year grouping. These are extracted
    and added to the ferc_license column in the associate_notes_with_values() function.

    """
    logger.info("Extracting FERC license from plant name")

    # Extract all numbers greater than 2 digits from plant_name_ferc1 and put then in a
    # new column as integers. Rename manually collected FERC id column to reflect that.
    sg_lic = (
        sg_df.assign(
            ferc_license=lambda x: (
                x.plant_name_ferc1.str.extract(r'(\d{3,})')
                .astype('float').astype('Int64')),
            ferc_license_id=lambda x: x.ferc_license_id.astype('Int64'))
        .rename(columns={'ferc_license_id': 'ferc_license_manual'}))

    # Not all of these 3+ digit numbers are FERC licenses. Some are dates, dollar
    # amounts, page numbers, or numbers of wind turbines. These next distinctions help
    # to weed out the non-licesnse values and keep the good ones.
    obvious_license = (
        sg_lic.plant_name_ferc1
        .str.contains(r'no\.|license|ferc|project', regex=True))
    not_license = (
        sg_lic.plant_name_ferc1
        .str.contains(r'page|pg|\$|wind|nonutility|units|surrendered', regex=True))
    exceptions = (
        sg_lic.plant_name_ferc1
        .str.contains(
            r'tomahawk|otter rapids|wausau|alexander|hooksett|north umpqua', regex=True))
    year_vs_num = (sg_lic['ferc_license'] > 1900) & (sg_lic['ferc_license'] < 2050)
    not_hydro = ~sg_lic.plant_type.isin(['hydro', np.nan])  # figure this one out.....
    extracted_license = sg_lic.ferc_license.notna()

    # Replace all the non-license numbers with nan
    # figure this one out.....
    sg_lic.loc[extracted_license & not_hydro, 'ferc_license'] = pd.NA
    extracted_license = sg_lic.ferc_license.notna()  # reset
    sg_lic.loc[extracted_license & not_license, 'ferc_license'] = pd.NA
    extracted_license = sg_lic.ferc_license.notna()  # reset
    sg_lic.loc[
        extracted_license
        & year_vs_num
        & ~obvious_license
        & ~exceptions, 'ferc_license'
    ] = pd.NA

    # figure out how not to do this twice....
    sg_lic['ferc_license'] = sg_lic.ferc_license.astype('Int64')

    return sg_lic


def associate_notes_with_values(sg_df):
    """Use footnotes to map string and ferc license to value rows.

    There are many utilities that report a bunch of note rows at the bottom of their
    yearly entry. These note rows often pertain directly to specific plant rows above.
    Sometimes, the notes and their respective plant rows are connected by a footnote
    such as (a) or (1) etc.

    This function finds those footnotes, associates the "note" version with the regular
    value row, maps the note content from the note row into a new note column that's
    associated with the value row, also maps any ferc license extracted from this note
    column up to the value row it references.

    """
    logger.info("Mapping notes and ferc license from footnotes")

    def associate_notes_with_values_group(group):
        """Map footnotes within a given utility year group.

        Because different utilities may use the same footnotes or the same utility
        could reuse footnotes each year, we must do the footnote association within
        utility-year groups.

        """
        regular_row = group['row_type'].isna()
        has_note = group['row_type'] == 'note'
        # has_footnote = group.plant_name_ferc1.str.contains(footnote_pattern)

        # Shorten execution time by only looking at groups with discernable footnotes
        if group.footnote.any():

            # Make a df that combines notes and ferc license with the same footnote
            footnote_df = (
                group[has_note]
                .groupby('footnote')
                .agg({'plant_name_ferc1': ', '.join,
                      'ferc_license': 'first'})
                .rename(columns={'plant_name_ferc1': 'notes'}))

            # Map these new licnese and note values onto the original df
            updated_ferc_license_col = group.footnote.map(footnote_df['ferc_license'])
            notes_col = group.footnote.map(footnote_df['notes'])
            # We update the ferc lic col because some were already there from the
            # plant name extraction. However, we want to override with the notes
            # ferc licenses because they are more likely to be accurate.
            group.ferc_license.update(updated_ferc_license_col)
            group.loc[regular_row, 'notes'] = notes_col

        return group

    footnote_pattern = r'(\(\d?[a-z]?[A-Z]?\))'
    sg_df['notes'] = pd.NA
    sg_df['footnote'] = pd.NA
    # Create new footnote column
    sg_df.loc[:, 'footnote'] = sg_df.plant_name_ferc1.str.extract(
        footnote_pattern, expand=False)
    # Group by year and utility and run footnote association
    groups = sg_df.groupby(['report_year', 'utility_id_ferc1'])
    sg_notes = groups.apply(lambda x: associate_notes_with_values_group(x))
    # Remove footnote column now that rows are associated
    sg_notes = sg_notes.drop(columns=['footnote'])

    return sg_notes


########################################################################################
# Final Functions
########################################################################################

def clean_small_gens(sg_df):
    """Run all the small gen cleaning functions together."""
    print('test')
    # sg_df = sg_df.rename(columns={'ferc_license_id': 'ferc_license_manual'})
    sg_clean = (
        sg_df.dropna(subset=['plant_name_ferc1'])
        .pipe(remove_bad_rows)
        .pipe(label_row_type)
        .pipe(improve_fuel_type)
        .pipe(extract_ferc_license)
        .pipe(associate_notes_with_values))

    return sg_clean
