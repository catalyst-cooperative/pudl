"""
Aggregate plant parts to make an EIA master plant-part table.

Practically speaking, a plant is a collection of generator(s). There are many
attributes of generators (i.e. prime mover, primary fuel source, technology
type). We can use these generator attributes to group generator records into
larger aggregate records which we call "plant-parts". A plant part is a record
which corresponds to a particular collection of generators that all share an
identical attribute. E.g. all of the generators with unit_id=2, or all of the
generators with coal as their primary fuel source.

The EIA data about power plants (from EIA 923 and 860) is reported in tables
with records that correspond to mostly generators and plants. Other datasets
(cough cough FERC1) are less well organized and include plants, generators and
other plant-parts all in the same table without any clear labels. The master
plant-part table is an attempt to create records corresponding to many
different plant-parts in order to connect specific slices of EIA plants to
other datasets.

Because generators are often owned by multiple utilities, another dimention of
the master unit list involves generating two records for each owner: one of the
portion of the plant part they own and one for the plant part as a whole. The
portion records are labeled in the `ownership` column as "owned" and the total
records are labeled as "total".

This module refers to "true granularies". Many plant parts we cobble together
here in the master plant-part list refer to the same collection of
infrastructure as other plant-part list records. For example, if we have a
"plant_prime_mover" plant part record and a "plant_unit" plant part record
which were both cobbled together from the same two generators. We want to be
able to reduce the master unit list to only unique collections of generators,
so we label the first unique granularity as a true granularity and label the
subsequent records as false granularities with the `true_gran` column. In order
to choose which plant part to keep in these instances, we assigned a
`PLANT_PARTS_ORDERED` and effectively keep the first instance of a unique
granularity.

Overview of flow for generating the master unit list:

`PLANT_PARTS` is the main recipe book for how each of the plant-parts need to
be compiled.

The three main classes which enable the generation of the plant-part table are:
* ``MakeMegaGenTbl``: All of the plant parts are compiled from generators. So
this class generates a big dataframe of generators with any ID and data columns
we'll need. This is also where we add records regarding utility ownership
slices. The table includes two records for every generator-owner: one for the
"total" generator (assuming the owner owns 100% of the generator) and one for
the report ownership fraction of that generator with all of the data columns
scaled to the ownership fraction.
* ``LabelTrueGranularities``: This class creates labels for all generators
which note wether the plant-part records that will be compiled from each
generator will be a "true granulary", as described above.
* ``MakePlantParts``: This class uses the generator dataframe, the granularity
dataframe from the above two classes as well as the information stored in
`PLANT_PARTS` to know how to aggregate each of the plant parts. Then we have
plant part dataframes with the columns which identify the plant part and all of
the data columns aggregated to the level of the plant part. With that compiled
plant part dataframe we also add in qualifier columns with `AddQualifier`. A
qualifer column is a column which contain data that is not endemic to the plant
part record (it is not one of the identifying columns or aggregated data
columns) but the data is still useful data that is attributable to each of the
plant part records. For more detail on what a qualifier column is, see the
`AddQualifier.execute()` method.

Lines needed to generate the plant-parts df:
``
import pudl
# make the pudl_out object
pudl_engine = sa.create_engine(pudl.workspace.setup.get_defaults()['pudl_db'])
pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine,freq='AS')
``
AND:
Make the table via pudl_out:
``
plant_parts_eia = pudl_out.plant_parts_eia()
``

OR
Make the table via objects in this module:
``
mega_gens = MakeMegaGenTbl(pudl_out).execute()
true_grans = LabelTrueGranularities(mega_gens).execute()

parts_compiler = MakePlantParts(pudl_out, mega_gens=mega_gens, true_grans=true_grans)
plant_parts_eia = parts_compiler.execute()
``

"""

import logging
import warnings
from copy import deepcopy

import numpy as np
import pandas as pd

import pudl

logger = logging.getLogger(__name__)

PLANT_PARTS = {
    'plant': {
        'id_cols': ['plant_id_eia'],
    },
    'plant_gen': {
        'id_cols': ['plant_id_eia', 'generator_id'],
    },
    'plant_unit': {
        'id_cols': ['plant_id_eia', 'unit_id_pudl'],
    },
    'plant_technology': {
        'id_cols': ['plant_id_eia', 'technology_description'],
    },
    'plant_prime_fuel': {
        'id_cols': ['plant_id_eia', 'energy_source_code_1'],
    },
    'plant_prime_mover': {
        'id_cols': ['plant_id_eia', 'prime_mover_code'],
    },
    'plant_ferc_acct': {
        'id_cols': ['plant_id_eia', 'ferc_acct_name'],
    },
}
"""
dict: this dictionary contains a key for each of the 'plant parts' that should
end up in the mater unit list. The top-level value for each key is another
dictionary, which contains keys:
    * id_cols (the primary key type id columns for this plant part). The
    plant_id_eia column must come first.
"""

PLANT_PARTS_ORDERED = [
    'plant',
    'plant_unit',
    'plant_prime_mover',
    'plant_technology',
    'plant_prime_fuel',
    'plant_ferc_acct',
    'plant_gen'
]


IDX_TO_ADD = ['report_date', 'operational_status_pudl']
"""
iterable: list of additional columns to add to the id_cols in `PLANT_PARTS`.
The id_cols are the base columns that we need to aggregate on, but we also need
to add the report date to keep the records time sensitive and the
operational_status_pudl to separate the operating plant-parts from the
non-operating plant-parts.
"""

IDX_OWN_TO_ADD = ['utility_id_eia', 'ownership']
"""
iterable: list of additional columns beyond the IDX_TO_ADD to add to the
id_cols in `PLANT_PARTS` when we are dealing with plant-part records that have
been broken out into "owned" and "total" records for each of their owners.
"""

SUM_COLS = [
    'total_fuel_cost',
    'net_generation_mwh',
    'capacity_mw',
    'capacity_mw_eoy',
    'total_mmbtu',
]
"""
iterable: list of columns to sum when aggregating a table.
"""

WTAVG_DICT = {
    'fuel_cost_per_mwh': 'capacity_mw',
    'heat_rate_mmbtu_mwh': 'capacity_mw',
    'fuel_cost_per_mmbtu': 'capacity_mw',
}
"""
dict: a dictionary of columns (keys) to perform weighted averages on and
the weight column (values)"""


QUAL_RECORDS = [
    'fuel_type_code_pudl',
    'operational_status',
    'planned_retirement_date',
    'retirement_date',
    'generator_id',
    'unit_id_pudl',
    'technology_description',
    'energy_source_code_1',
    'prime_mover_code',
    'ferc_acct_name',
    # 'installation_year'
]
"""
dict: a dictionary of qualifier column name (key) and original table (value).
"""

DTYPES_MUL = {
    "plant_id_eia": "int64",
    "report_date": "datetime64[ns]",
    "plant_part": "object",
    "generator_id": "object",
    "unit_id_pudl": "object",
    "prime_mover_code": "object",
    "energy_source_code_1": "object",
    "technology_description": "object",
    "ferc_acct_name": "object",
    "utility_id_eia": "object",
    "true_gran": "bool",
    "appro_part_label": "object",
    "appro_record_id_eia": "object",
    "capacity_factor": "float64",
    "capacity_mw": "float64",
    "fraction_owned": "float64",
    "fuel_cost_per_mmbtu": "float64",
    "fuel_cost_per_mwh": "float64",
    "heat_rate_mmbtu_mwh": "float64",
    "installation_year": "Int64",
    "net_generation_mwh": "float64",
    "ownership": "category",
    "plant_id_pudl": "Int64",
    "plant_name_eia": "string",
    "total_fuel_cost": "float64",
    "total_mmbtu": "float64",
    "utility_id_pudl": "Int64",
    "utility_name_eia": "string",
    "report_year": "int64",
    "plant_id_report_year": "object",
    "plant_name_new": "string"
}

FIRST_COLS = ['plant_id_eia', 'report_date', 'plant_part', 'generator_id',
              'unit_id_pudl', 'prime_mover_code', 'energy_source_code_1',
              'technology_description', 'ferc_acct_name',
              'utility_id_eia', 'true_gran', 'appro_part_label']


class MakeMegaGenTbl(object):
    """Compiler for a MEGA generator table with ownership integrated."""

    def __init__(self, pudl_out):
        """
        Initialize object which creates a MEGA generator table.

        The coordinating function here is ``execute()``.

        Args:
            pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
                the tables for EIA and FERC Form 1 analysis.
        Raises:
            AssertionError: If the frequency of the pudl_out object is not 'AS'
        """
        self.pudl_out = pudl_out
        if pudl_out.freq != 'AS':
            raise AssertionError(
                "The frequency of the pudl_out object must be `AS` for the "
                f"plant-parts table and we got {pudl_out.freq}"
            )
        self.id_cols_list = make_id_cols_list()

    def execute(self):
        """
        Make the mega generators table with ownership integrated.

        Returns:
            pandas.DataFrame: a table of all of the generators with identifying
            columns and data columns, sliced by ownership which makes
            "total" and "owned" records for each generator owner. The "owned"
            records have the generator's data scaled to the ownership percentage
            (e.g. if a 100 MW generator has a 75% stake owner and a 25% stake
            owner, this will result in two "owned" records with 75 MW and 25
            MW). The "total" records correspond to the full plant for every
            owner (e.g. using the same 2-owner 100 MW generator as above, each
            owner will have a records with 100 MW).
        """
        logger.info('Generating the mega generator table with ownership.')
        gens_mega = (
            self.get_mega_gens_table()
            .pipe(self.ensure_column_completeness)
            .pipe(self.slice_by_ownership)
        )
        return gens_mega

    def get_mega_gens_table(self):
        """
        Compile the main generators table that will be used as base of MUL.

        Get a table of all of the generators there ever were and all of the
        data PUDL has to offer about those generators. This generator table
        will be used to compile all of the "plant-parts", so we need to ensure
        that any of the id columns from the other plant-parts are in this
        generator table as well as all of the data columns that we are going to
        aggregate to the various plant-parts.

        Returns:
            pandas.DataFrame
        """
        # pull in the main two tables
        gens = self.pudl_out.gens_eia860()
        mcoe = self.pudl_out.mcoe()

        # because lots of these input dfs include same info columns, this
        # generates drop columnss for fuel_cost. This avoids needing to hard
        # code columns.
        merge_cols = ['plant_id_eia', 'generator_id', 'report_date']
        drop_cols = [x for x in mcoe if x in gens and x not in merge_cols]

        all_gens = (
            pd.merge(
                gens.pipe(pudl.helpers.convert_cols_dtypes, 'eia'),
                mcoe.pipe(pudl.helpers.convert_cols_dtypes, 'eia')
                .drop(drop_cols, axis=1),
                on=merge_cols,
                validate='1:1',
                how='left'
            )
            .merge(
                pudl.helpers.get_eia_ferc_acct_map(),
                on=['technology_description', 'prime_mover_code'],
                validate='m:1',
                how='left'
            )
            .assign(installation_year=lambda x: x.operating_date.dt.year)
            .astype({'installation_year': 'Int64'})
            .pipe(self.label_operating_gens)
        )
        return all_gens

    def label_operating_gens(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """
        Label the operating generators.

        We want to distinguish between "operating" generators (those that
        report as "existing" and those that retire mid-year) and everything
        else so that we can group the operating generators into their own
        plant-parts separate from retired or proposed generators. We do this by
        creating

        This method also adds a column called "capacity_mw_eoy", which is the
        end of year capacity of the generators. We assume that if a generator
        isn't "existing", its EOY capacity should be zero.

        Args:
            gen_df (pandas.DataFrame): annual table of all generators from EIA.

        Returns
            pandas.DataFrame: annual table of all generators from EIA that
            operated within each reporting year.
        """
        mid_year_retiree_mask = (
            gen_df.retirement_date.dt.year == gen_df.report_date.dt.year)
        existing_mask = (gen_df.operational_status == 'existing')
        operating_mask = existing_mask | mid_year_retiree_mask

        # we've going to make a new column which combines both the mid-year
        # reitrees and the fully existing gens into one code so we can group
        # them together later on
        gen_df.loc[operating_mask, 'operational_status_pudl'] = 'operating'
        gen_df.loc[~operating_mask, 'operational_status_pudl'] = (
            gen_df.loc[~operating_mask, 'operational_status']
        )

        gen_df.loc[~existing_mask, 'capacity_mw_eoy'] = 0
        gen_df.loc[existing_mask, 'capacity_mw_eoy'] = (
            gen_df.loc[existing_mask, 'capacity_mw']
        )

        logger.info(
            f"Labeled {len(gen_df.loc[~existing_mask])/len(gen_df):.02%} of "
            "generators as non-operative."
        )
        return gen_df

    def ensure_column_completeness(self, all_gens):
        """
        Ensure the generators table has all the columns we need.

        Check to see if the master gens table has all of the columns we want
        extract columns from PLANT_PARTS + a few extra.
        """
        other_cols = [
            'plant_name_eia',
            'installation_year',
            'utility_id_eia',
            'fuel_type_code_pudl',
            'operational_status',
            'planned_retirement_date',
            'retirement_date'
        ]
        all_cols = (
            self.id_cols_list + SUM_COLS + list(WTAVG_DICT.keys()) + other_cols
        )

        missing = [c for c in all_cols if c not in all_gens]
        if missing:
            raise AssertionError(
                f'The main generator table is missing {missing}'
            )
        # bb test to ensure that we are getting all of the possible records
        # w/ net generation
        generation = self.pudl_out.gen_eia923()
        assert (
            len(generation[generation.net_generation_mwh.notnull()]) ==
            len(all_gens[all_gens.net_generation_mwh.notnull()]
                .drop_duplicates(
                    subset=['plant_id_eia', 'report_date', 'generator_id']
            ))
        )
        return all_gens[all_cols]

    def slice_by_ownership(self, gens_mega):
        """Generate proportional data by ownership %s."""
        own860 = (
            self.pudl_out.own_eia860()
            [['plant_id_eia', 'generator_id', 'report_date',
              'fraction_owned', 'owner_utility_id_eia']]
            .pipe(pudl.helpers.convert_cols_dtypes, 'eia')
        )

        logger.debug(f'# of generators before munging: {len(gens_mega)}')
        gens_mega = gens_mega.merge(
            own860,
            how='left',  # we're left merging BC we've removed the retired gens
            on=['plant_id_eia', 'generator_id', 'report_date'],
            validate='1:m'
        )

        # clean the remaining nulls
        # assign 100% ownership for records not in the ownership table
        gens_mega = gens_mega.assign(
            fraction_owned=gens_mega.fraction_owned.fillna(value=1),
            # assign the operator id as the owner if null
            owner_utility_id_eia=gens_mega.owner_utility_id_eia.fillna(
                gens_mega.utility_id_eia),
            ownership='owned'
        )

        fake_totals = self.make_fake_totals(gens_mega)

        gens_mega = gens_mega.append(fake_totals, sort=False)
        logger.debug(f'# of generators post-fakes:     {len(gens_mega)}')
        gens_mega = (
            gens_mega.drop(columns=['utility_id_eia'])
            .rename(columns={'owner_utility_id_eia': 'utility_id_eia'})
            .drop_duplicates()
        )

        gens_mega[SUM_COLS] = (
            gens_mega[SUM_COLS]
            .multiply(gens_mega['fraction_owned'], axis='index')
        )
        if (len(gens_mega[gens_mega.ownership == 'owned']) >
                len(gens_mega[gens_mega.ownership == 'total'])):
            warnings.warn(
                'There should be more records labeled as total.')
        return gens_mega

    def make_fake_totals(self, gens_mega):
        """Generate total versions of generation-owner records."""
        # make new records for generators to replicate the total generator
        fake_totals = gens_mega[[
            'plant_id_eia', 'report_date', 'utility_id_eia',
            'owner_utility_id_eia']].drop_duplicates()
        # asign 1 to all of the fraction_owned column
        fake_totals = fake_totals.assign(fraction_owned=1,
                                         ownership='total')
        fake_totals = pd.merge(
            gens_mega.drop(
                columns=['ownership', 'utility_id_eia',
                         'owner_utility_id_eia', 'fraction_owned']),
            fake_totals)
        return fake_totals


class LabelTrueGranularities(object):
    """True Granularity Labeler."""

    def __init__(self, mega_gens):
        """
        Initialize the true granulary labeler.

        The coordinating function here is ``execute()``.

        Args:
            mega_gens (pandas.DataFrame):
        """
        self.mega_gens = mega_gens
        self.parts_to_parent_parts = self.get_parts_to_parent_parts()
        self.id_cols_list = make_id_cols_list()
        self.parts_to_ids = make_parts_to_ids_dict()
        {v: k for k, v in self.parts_to_ids.items()}

    def execute(self, drop_extra_cols=True):
        """
        Prep the table that denotes true_gran for all generators.

        This method will generate a dataframe based on ``self.gens_mega``
        that has boolean columns that denotes whether each plant-part is a true
        or false granularity.

        There are four main steps in this process:
          * For every combinations of plant-parts, count the number of unique
            types of peer plant-parts (see ``make_all_the_counts()`` for more
            details).
          * Convert those counts to boolean values if there is more or less
            than one unique type parent or child plant-part (see
            ``make_all_the_bools()`` for more details).
          * Using the boolean values label each plant-part as a True or False
            granularies if both the boolean for the parent-to-child and
            child-to-parent (see ``label_true_grans_by_part()`` for more
            details).
          * For each plant-part, label it with its the appropriate plant-part
            counterpart - if it is a True granularity, the appropriate label is
            itself (see ``label_true_id_by_part()`` for more details).

        Args:
            drop_extra_cols (boolean): if True, the extra columns used to
                generate the true_gran columns. Default is True.

        """
        true_gran_labels = (
            self.make_all_the_counts()
            .pipe(self.make_all_the_bools)
            .pipe(self.label_true_grans_by_part)
            .pipe(self.label_true_id_by_part)
        )
        if drop_extra_cols:
            for drop_cols in ['_v_', '_has_only_one_', 'count_per']:
                true_gran_labels = true_gran_labels.drop(
                    columns=true_gran_labels.filter(like=drop_cols)
                )
        return true_gran_labels

    def get_parts_to_parent_parts(self):
        """
        Make a dictionary of each plant-part's parent parts.

        We have imposed a hierarchy on the plant-parts with the
        ``PLANT_PARTS_ORDERED`` list and this method generates a dictionary of
        each plant-part's (key) parent-parts (value).
        """
        parts_to_parent_parts = {}
        n = 0
        for part_name in PLANT_PARTS_ORDERED:
            parts_to_parent_parts[part_name] = PLANT_PARTS_ORDERED[:n]
            n = n + 1
        return parts_to_parent_parts

    def make_all_the_counts(self):
        """
        For each plant-part, count the unique child and parent parts.

        Returns:
            pandas.DataFrame: an agumented version of the ``gens_mega``
            dataframe with new columns for each of the child and parent
            plant-parts with counts of unique instances of those parts. The
            columns will be named in the following format:
            {child/parent_part_name}_count_per_{part_name}
        """
        gens_mega = self.mega_gens.loc[
            :,
            self.id_cols_list  # id_cols_list already has IDX_TO_ADD
            + IDX_OWN_TO_ADD
        ]
        # grab the plant-part id columns from the generator table
        count_ids = gens_mega.loc[:, self.id_cols_list].drop_duplicates()

        # we want to compile the count results on a copy of the generator table
        all_the_counts = gens_mega.copy()
        for part_name in PLANT_PARTS_ORDERED:
            logger.debug(f"making the counts for: {part_name}")
            all_the_counts = all_the_counts.merge(
                self.count_child_and_parent_parts(part_name, count_ids),
                how='left')

        # check the expected # of columns
        # id columns minus the added columns
        len_ids = len(self.id_cols_list) - len(IDX_TO_ADD)
        expected_col_len = (
            len(gens_mega.columns) +  # the gens_mega colums
            len_ids * (len_ids - 1) + 1  # the count columns (we add one bc we
            # added straggler plant_count_per_plant column bc we make a
            # plant_id_eia_temp column)
        )
        if expected_col_len != len(all_the_counts.columns):
            raise AssertionError(
                f"We got {len(all_the_counts.columns)} columns from "
                f"all_the_counts when we should have gotten {expected_col_len}"
            )
        return all_the_counts

    def count_child_and_parent_parts(self, part_name, count_ids):
        """
        Count the child- and parent-parts contained within a plant-part.

        Args:
            part_name (string): name of plant-part
            count_ids (pandas.DataFrame): a table of generator records with

        Returns:
            pandas.DataFrame: an agumented version of the ``gens_mega``
            dataframe with new columns for each of the child and parent
            plant-parts with counts of unique instances of those parts. The
            columns will be named in the following format:
            {child/parent_part_name}_count_per_{part_name}

        """
        part_cols = PLANT_PARTS[part_name]['id_cols'] + IDX_TO_ADD
        # because the plant_id_eia is always a part of the groupby columns
        # and we want to count the plants as well, we need to make a duplicate
        # plant_id_eia column to count on
        df_count = (
            count_ids.assign(plant_id_eia_temp=lambda x: x.plant_id_eia)
            .groupby(by=part_cols, dropna=False).nunique()
            .rename(columns={'plant_id_eia_temp': 'plant_id_eia'})
            .rename(columns={v: k for k, v in self.parts_to_ids.items()})
            .add_suffix(f'_count_per_{part_name}')
        )
        # merge back into the og df
        df_w_count = count_ids.merge(
            df_count,
            how='left',
            right_index=True,
            left_on=part_cols,
        )
        return df_w_count

    def make_all_the_bools(self, counts):
        """
        Count consistency of records and convert that to bools.

        Args:
            all_the_counts (pandas.DataFrame): result of
                ``make_all_the_counts()``

        Returns:
            pandas.DataFrame: a table with generator records where we have new
            boolean columns which indicated whether or not the plant-part
            has more than one child/parent-part. These columns are formated
            as: {child/parent_part_name}_has_only_one_{part_name}

        """
        counts.loc[:, counts.filter(like='_count_per_').columns] = (
            counts.loc[:, counts.filter(like='_count_per_').columns]
            .astype(pd.Int64Dtype())
        )

        # convert the count columns to bool columns
        for col in counts.filter(like='_count_per_').columns:
            bool_col = col.replace("_count_per_", "_has_only_one_")
            counts.loc[counts[col].notnull(), bool_col] = counts[col] == 1
        # force the nullable bool type for all our count cols
        counts.loc[:, counts.filter(like='_has_only_one_').columns] = (
            counts.filter(like='_has_only_one_').astype(pd.BooleanDtype())
        )
        return counts

    def label_true_grans_by_part(self, part_bools):
        """
        Label the true/false granularies for each part/parent-part combo.

        This method uses the indicator columns which let us know whether or not
        there are more than one unique value for both the parent and child
        plant-part ids to generate an additional indicator column that let's us
        know whether the child plant-part is a true or false granularity when
        compared to the parent plant-part. With all of the indicator columns
        from each plant-part's parent plant-parts, if all of those determined
        that the plant-part is a true granularity, then this method will label
        the plant-part as being a true granulary and vice versa.

        Because we have forced a hierarchy within the ``PLANT_PARTS_ORDERED``,
        the process for labeling true or false granularities must investigate
        bi-directionally. This is because all of the plant-parts besides
        'plant' and 'plant_gen' are not necessarily bigger of smaller than
        their parent plant-part and thus there is overlap. Because of this,
        this method uses the checks in both directions (from partent to child
        and from child to parent).

        Args:
            part_bools (pandas.DataFrame): result of ``make_all_the_bools()``
        """
        # assign a bool for the true gran only if all
        for part_name, parent_parts in self.parts_to_parent_parts.items():
            for parent_part_name in parent_parts:
                # let's save the input boolean columns
                bool_cols = [f'{part_name}_has_only_one_{parent_part_name}',
                             f'{parent_part_name}_has_only_one_{part_name}']
                false_gran_col = f'false_gran_{part_name}_v_{parent_part_name}'
                # the long awaited ALL.. label them as
                part_bools[false_gran_col] = (
                    part_bools[bool_cols].all(axis='columns'))
                part_bools = part_bools.astype(
                    {false_gran_col: pd.BooleanDtype()})
                # create the inverse column as true_grans
                part_bools[f'true_gran_{part_name}_v_{parent_part_name}'] = (
                    ~part_bools[false_gran_col])
            # if all of the true_gran part v parent part columns are false,
            # than this part is a false gran. if they are all true, then wahoo
            # the record is truly unique
            part_bools[f'true_gran_{part_name}'] = (
                part_bools.filter(like=f'true_gran_{part_name}')
                .all(axis='columns'))
            trues_found = (
                part_bools[part_bools[f'true_gran_{part_name}']]
                .drop_duplicates(subset=[self.parts_to_ids[part_name],
                                         'plant_id_eia', ] + IDX_TO_ADD))
            logger.info(
                f'true grans found for {part_name}: {len(trues_found)}'
            )
        part_trues = part_bools.drop(
            columns=part_bools.filter(like='false_gran').columns)
        return part_trues

    def label_true_id_by_part(self, part_trues):
        """
        Label the appropriate plant-part.

        For each plant-part, we need to make a label which indicates what the
        "true" unique plant-part is.. if a gen vs a unit is a non-unique set a
        records, we only want to label one of them as false granularities. We
        are going to use the ``parts_to_parent_parts`` dictionary to help us
        with this. We want to "save" the biggest parent plant-part as true
        granularity.

        Because we have columns in ``part_trues`` that indicate whether a
        plant-part is a true gran vs each parent part, we can cycle through
        the possible parent-parts from biggest to smallest and the first time
        we find that a plant-part is a false gran, we label it's true id as
        that parent-part.
        """
        for part_name, parent_parts in self.parts_to_parent_parts.items():
            # make column which will indicate which part is the true/unique
            # plant-part...
            appro_part_col = f"appro_part_label_{part_name}"
            # make this col null so we can fill in
            part_trues[appro_part_col] = pd.NA
            for parent_part_name in parent_parts:
                # find the reords where the true gran col is false, and label
                # the appropriate part column name with that parent part
                mask_loc = (
                    ~part_trues[f'true_gran_{part_name}_v_{parent_part_name}'],
                    appro_part_col
                )
                part_trues.loc[mask_loc] = part_trues.loc[mask_loc].fillna(
                    parent_part_name)
            # for all of the plant-part records which we didn't find any false
            # gran's the appropriate label is itself! it is a unique snowflake
            part_trues[appro_part_col] = part_trues[appro_part_col].fillna(
                part_name)
            part_trues = (
                assign_record_id_eia(
                    part_trues, plant_part_col=appro_part_col)
                .rename(
                    columns={'record_id_eia': f'record_id_eia_{part_name}'})
            )
            # do a little check
            if not all(part_trues[part_trues[f'true_gran_{part_name}']]
                       [appro_part_col] == part_name):
                raise AssertionError(
                    f'eeeeEEeEe... the if true_gran_{part_name} is true, the '
                    f'{appro_part_col} should {part_name}.'
                )
        return part_trues


class MakePlantParts(object):
    """Compile plant parts."""

    def __init__(self, pudl_out, mega_gens, true_grans):
        """
        Compile the plant parts for the master unit list.

        The coordinating function here is ``execute()``.

        Args:
            pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
                the tables for EIA and FERC Form 1 analysis.
            gens_maker (object): an instance of ``MakeMegaGenTbl``
            true_grans_labeler (object): an instance of
                ``LabelTrueGranularities``
        """
        self.pudl_out = pudl_out
        self.freq = pudl_out.freq
        self.mega_gens = mega_gens
        self.true_grans = true_grans
        self.parts_to_ids = make_parts_to_ids_dict()

        # get a list of all of the id columns that constitue the primary keys
        # for all of the plant parts
        self.id_cols_list = make_id_cols_list()

    def execute(self):
        """
        Aggreate and slice data points by each plant part.

        This method generates a master list of different "plant-parts", which
        are various collections of generators - i.e. units, fuel-types, whole
        plants, etc. - as well as various ownership arrangements. Each
        plant-part is included in the master plant-part table associated with
        each of the plant-part's owner twice - once with the data scaled to the
        fraction of each owners' ownership and another for a total plant-part
        for each owner.

        This master plant parts table is generated by first creating a complete
        generators table - with all of the data columns we will be aggregating
        to different plant-part's and sliced and scaled by ownership. Then we
        make a label for each plant-part record which indicates whether or not
        the record is a unique grouping of generator records. Then we use the
        complete generator table to aggregate by each of the plant-part
        categories.

        Returns:
            pandas.DataFrame:
        """
        # 3) aggreate everything by each plant part
        plant_parts_eia = pd.DataFrame()
        for part_name in PLANT_PARTS_ORDERED:
            part_df = (
                PlantPart(
                    part_name,
                    self.mega_gens,
                    self.true_grans)
                .execute()
            )
            # add in the qualifier records
            for qual_record in QUAL_RECORDS:
                part_df = (
                    AddQualifier(
                        qual_record,
                        part_name,
                        self.mega_gens)
                    .execute(part_df)
                )
            plant_parts_eia = plant_parts_eia.append(part_df, sort=True)
        # clean up, add additional columns
        plant_parts_eia = (
            self.add_additonal_cols(plant_parts_eia)
            .pipe(pudl.helpers.organize_cols, FIRST_COLS)
            .pipe(self._clean_plant_parts)
        )
        self.test_ownership_for_owned_records(plant_parts_eia)
        return plant_parts_eia

    #######################################
    # Add Entity Columns and Final Cleaning
    #######################################

    def add_additonal_cols(self, plant_parts_eia):
        """
        Add additonal data and id columns.

        This method adds a set of either calculated columns or PUDL ID columns.

        Returns:
            pandas.DataFrame: master unit list table with these additional
            columns:
                * utility_id_pudl +
                * plant_id_pudl +
                * capacity_factor +
                * ownership_dupe (boolean): indicator of whether the "owned"
                    record has a corresponding "total" duplicate.


        """
        plant_parts_eia = (
            pudl.helpers.calc_capacity_factor(
                plant_parts_eia, -0.5, 1.5, self.freq)
            .merge(
                self.pudl_out.plants_eia860()
                [['plant_id_eia', 'plant_id_pudl']]
                .drop_duplicates(),
                how='left',
                on=['plant_id_eia', ]
            )
            .merge(
                self.pudl_out.utils_eia860()
                [['utility_id_eia', 'utility_id_pudl']]
                .drop_duplicates(),
                how='left',
                on=['utility_id_eia']
            )
            .assign(ownership_dupe=lambda x: np.where(
                (x.ownership == 'owned') & (x.fraction_owned == 1),
                True, False)
            )
        )
        return plant_parts_eia

    def _clean_plant_parts(self, plant_parts_eia):
        return (
            plant_parts_eia
            .assign(
                report_year=lambda x: x.report_date.dt.year,
                plant_id_report_year=lambda x:
                    x.plant_id_pudl.astype(str)
                    + "_" + x.report_year.astype(str)
            )
            .pipe(pudl.helpers.cleanstrings_snake, ['record_id_eia'])
            # we'll eventually take this out... once Issue #20
            .drop_duplicates(subset=['record_id_eia'])
            .set_index('record_id_eia')
        )

    #################
    # Testing Methods
    #################

    def test_ownership_for_owned_records(self, plant_parts_eia):
        """
        Test ownership - fraction owned for owned records.

        This test can be run at the end of or with the result of
        `generate_master_unit_list()`. It tests a few aspects of the the
        fraction_owned column and raises assertions if the tests fail.
        """
        test_own_df = (
            plant_parts_eia.groupby(
                by=self.id_cols_list + ['plant_part', 'ownership'],
                dropna=False
            )
            [['fraction_owned', 'capacity_mw']].sum(min_count=1).reset_index())

        owned_one_frac = test_own_df[
            (~np.isclose(test_own_df.fraction_owned, 1))
            & (test_own_df.capacity_mw != 0)
            & (test_own_df.capacity_mw.notnull())
            & (test_own_df.ownership == 'owned')]

        if not owned_one_frac.empty:
            self.test_own_df = test_own_df
            self.owned_one_frac = owned_one_frac
            raise AssertionError(
                "Hello friend, you did bad. It happens... Error with the "
                "fraction_owned col/slice_by_ownership(). There are "
                f"{len(owned_one_frac)} rows where fraction_owned != 1 for "
                "owned records. Check cached `owned_one_frac` & `test_own_df`"
            )

        no_frac_n_cap = test_own_df[
            (test_own_df.capacity_mw == 0)
            & (test_own_df.fraction_owned == 0)
        ]
        if len(no_frac_n_cap) > 60:
            self.no_frac_n_cap = no_frac_n_cap
            warnings.warn(
                f"""Too many nothings, you nothing. There shouldn't been much
                more than 60 instances of records with zero capacity_mw (and
                therefor zero fraction_owned) and you got {len(no_frac_n_cap)}.
                """
            )


class PlantPart(object):
    """Plant-part table maker."""

    def __init__(self, part_name, mega_gens, true_grans):
        """
        Initialize an object which makes a tbl for a specific plant-part.

        The coordinating method here is ``get_part_df()``.

        Args:
            part_name (str): the name of the part to aggregate to. Names can be
                only those in `PLANT_PARTS`
            gens_maker (object): an instance of ``MakeMegaGenTbl``
            true_grans_labeler (object): an instance of
                ``LabelTrueGranularities``
        """
        self.part_name = part_name
        self.id_cols = PLANT_PARTS[part_name]['id_cols']

        self.mega_gens = mega_gens
        self.true_grans = true_grans

    def execute(self):
        """
        Get a table of data aggregated by a specific plant-part.

        This method will use ``gens_mega`` (or generate if it doesn't
        exist yet) to aggregate the generator records to the level of the
        plant-part. This is mostly done via ``ag_part_by_own_slice()``. Then
        several additional columns are added and the records are labeled as
        true or false granularities.

        Returns:
            pandas.DataFrame
        """
        part_df = (
            self.ag_part_by_own_slice()
            .pipe(self.ag_fraction_owned)
            .assign(plant_part=self.part_name)
            .pipe(self.add_install_year)
            .pipe(self.assign_true_gran)
            .pipe(
                add_record_id,
                id_cols=self.id_cols,
                plant_part_col='plant_part'
            )
            .pipe(self.add_new_plant_name)
            .pipe(self.add_record_count_per_plant)
        )
        return part_df

    def ag_part_by_own_slice(self):
        """
        Aggregate the plant part by seperating ownership types.

        There are total records and owned records in this master unit list.
        Those records need to be aggregated differently to scale. The total
        owned slice is now grouped and aggregated as a single version of the
        full plant and then the utilities are merged back. The owned slice is
        grouped and aggregated with the utility_id_eia, so the portions of
        generators created by slice_by_ownership will be appropriately
        aggregated to each plant part level.

        Returns:
            pandas.DataFrame : dataframe aggregated to the level of the
                part_name
        """
        logger.info(f'begin aggregation for: {self.part_name}')
        # id_cols = PLANT_PARTS[self.part_name]['id_cols']
        # split up the 'owned' slices from the 'total' slices.
        # this is because the aggregations are different
        gens_mega = self.mega_gens
        part_own = gens_mega.loc[gens_mega.ownership == 'owned'].copy()
        part_tot = gens_mega.loc[gens_mega.ownership == 'total'].copy()
        if len(gens_mega) != len(part_own) + len(part_tot):
            raise AssertionError(
                "Error occured in breaking apart ownership types."
                "The total and owned slices should equal the total records."
                "Check for nulls in the ownership column."
            )
        dedup_cols = list(part_tot.columns)
        dedup_cols.remove('utility_id_eia')
        dedup_cols.remove('unit_id_pudl')
        part_tot = part_tot.drop_duplicates(subset=dedup_cols)
        part_own = pudl.helpers.agg_cols(
            df_in=part_own,
            id_cols=self.id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
            sum_cols=SUM_COLS,
            wtavg_dict=WTAVG_DICT
        )
        # still need to re-calc the fraction owned for the part
        part_tot = (
            pudl.helpers.agg_cols(
                df_in=part_tot,
                id_cols=self.id_cols + IDX_TO_ADD,
                sum_cols=SUM_COLS,
                wtavg_dict=WTAVG_DICT
            )
            .merge(  # why is this here?
                gens_mega[self.id_cols + ['report_date', 'utility_id_eia']]
                .dropna()
                .drop_duplicates())
            .assign(ownership='total')
        )
        part_ag = (
            part_own.append(part_tot, sort=False)
        )

        return part_ag

    def ag_fraction_owned(self, part_ag):
        """
        Calculate the fraction owned for a plant-part df.

        This method takes a dataframe of records that are aggregated to the
        level of a plant-part (with certain `id_cols`) and appends a
        fraction_owned column, which indicates the % ownership that a
        particular utility owner has for each aggreated plant-part record.

        For partial owner records (ownership == "owned"), fraction_owned is
        calcuated based on the portion of the capacity and the total capacity
        of the plant. For total owner records (ownership == "total"), the
        fraction_owned is always 1.

        This method is meant to be run within `ag_part_by_own_slice()`.

        Args:
            part_ag (pandas.DataFrame):
            id_cols (list): list of identifying columns
                (stored as: `PLANT_PARTS[part_name]['id_cols']`)
        """
        # we must first get the total capacity of the full plant
        # Note: we could simply not include the ownership == "total" records
        # We are automatically assign fraction_owned == 1 to them, but it seems
        # cleaner to run the full df through this same grouby
        frac_owned = (
            part_ag.groupby(
                by=self.id_cols +
                ['ownership', 'operational_status_pudl', 'report_date'])
            [['capacity_mw']].sum(min_count=1)
        )
        # then merge the total capacity with the plant-part capacity to use to
        # calculate the fraction_owned
        part_frac = (
            pd.merge(
                part_ag,
                frac_owned,
                right_index=True,
                left_on=frac_owned.index.names,
                suffixes=("", "_total")
            )
            .assign(
                fraction_owned=lambda x:
                    np.where(
                        x.ownership == 'owned',
                        x.capacity_mw / x.capacity_mw_total,
                        1
                    )
            )
            .drop(columns=['capacity_mw_total'])
        )
        return part_frac

    def add_install_year(self, part_df):
        """Add the install year from the entities table to your plant part."""
        logger.debug(f'pre count of part DataFrame: {len(part_df)}')
        # we want to sort to have the most recent on top
        install = (
            self.mega_gens
            [self.id_cols + ['operational_status_pudl', 'installation_year']]
            .sort_values('installation_year', ascending=False)
            .drop_duplicates(subset=self.id_cols, keep='first')
            .dropna(subset=self.id_cols)
        )
        part_df = part_df.merge(
            install, how='left',
            on=self.id_cols + ['operational_status_pudl'], validate='m:1')
        logger.debug(
            f'count of install years for part: {len(install)} \n'
            f'post count of part DataFrame: {len(part_df)}'
        )
        return part_df

    def assign_true_gran(self, part_df):
        """
        Merge the true granularity labels into the plant part df.

        Args:
            part_df (pandas.DataFrame)

        """
        bool_df = self.true_grans
        # get only the columns you need for this part and drop duplicates
        bool_df = (
            bool_df[
                self.id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD +
                [f'true_gran_{self.part_name}',
                 f'appro_part_label_{self.part_name}',
                 f'record_id_eia_{self.part_name}', ]
            ]
            .drop_duplicates()
        )

        prop_true_len1 = len(
            bool_df[bool_df[f'true_gran_{self.part_name}']]) / len(bool_df)
        logger.debug(f'proportion of trues: {prop_true_len1:.02}')
        logger.debug(f'number of records pre-merge:  {len(part_df)}')

        part_df = (
            part_df.merge(
                bool_df,
                on=self.id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
                how='left',
            )
            .rename(columns={
                f'true_gran_{self.part_name}': 'true_gran',
                f'appro_part_label_{self.part_name}': 'appro_part_label',
                f'record_id_eia_{self.part_name}': 'appro_record_id_eia'
            })
        )

        prop_true_len2 = len(part_df[part_df.true_gran]) / len(part_df)
        logger.debug(f'proportion of trues: {prop_true_len2:.02}')
        logger.debug(f'number of records post-merge: {len(part_df)}')
        return part_df

    def add_new_plant_name(self, part_df):
        """
        Add plants names into the compiled plant part df.

        Args:
            part_df (pandas.DataFrame)
            part_name (string)
        """
        part_df = (
            pd.merge(
                part_df,
                self.mega_gens
                [self.id_cols + ['plant_name_eia']].drop_duplicates(),
                on=self.id_cols,
                how='left'
            )
            .assign(plant_name_new=lambda x: x.plant_name_eia))
        # we don't want the plant_id_eia to be part of the plant name, but all
        # of the other parts should have their id column in the new plant name
        if self.part_name != 'plant':
            col = [x for x in self.id_cols if x != 'plant_id_eia'][0]
            part_df.loc[part_df[col].notnull(), 'plant_name_new'] = (
                part_df['plant_name_new'] + " " + part_df[col].astype(str))
        return part_df

    def add_record_count_per_plant(self, part_df):
        """
        Add a record count for each set of plant part records in each plant.

        Args:
            part_df (pandas.DataFrame): dataframe containing records associated
                with one plant part.
        """
        group_cols = ['plant_id_eia'] + IDX_TO_ADD + IDX_OWN_TO_ADD
        # count unique records per plant
        part_count = (
            part_df.groupby(group_cols, as_index=False)
            [['record_id_eia']].count()
            .rename(columns={'record_id_eia': 'record_count'})
        )
        part_df = pd.merge(
            part_df, part_count, on=group_cols, how='left'
        )
        return part_df


class AddQualifier(object):
    """Adder of qualifier records to a plant-part table."""

    def __init__(self, qual_record, part_name, mega_gens):
        """
        Initialize a qualifer record adder.

        Args:
            qual_record (string): name of qualifer record that you want added.
                Must be in ``QUAL_RECORDS``.
            part_name (str): the name of the part to aggregate to. Names can be
                only those in `PLANT_PARTS`
            gens_maker (object): an instance of ``MakeMegaGenTbl``

        """
        self.qual_record = qual_record
        # the base columns will be the id columns, plus the other two main ids
        self.part_name = part_name
        self.id_cols = PLANT_PARTS[part_name]['id_cols']
        # why no util id? otoh does ownership need to be here at all?
        # qualifiers should be independent of ownership
        self.base_cols = self.id_cols + IDX_TO_ADD + ['ownership']
        self.mega_gens = mega_gens

    def execute(self, part_df):
        """
        Get qualifier records.

        For an individual dataframe of one plant part (e.g. only
        "plant_prime_mover" plant part records), we typically have identifying
        columns and aggregated data columns. The identifying columns for a
        given plant part are only those columns which are required to uniquely
        specify a record of that type of plant part. For example, to uniquely
        specify a plant_unit record, we need both plant_id_eia and the
        unit_id_pudl, and nothing else. In other words, the identifying columns
        for a given plant part would make up a natural composite primary key
        for a table composed entirely of that type of plant part. Every plant
        part is cobbled together from generator records, so each record in
        each part_df can be thought of as a collection of generators.

        Identifier and qualifier columns are the same columns; whether a column
        is an identifier or a qualifier is a function of the plant part you're
        considering. All the other columns which could be identifiers in the
        context of other plant parrts (but aren't for this plant part) are
        qualifiers.

        This method takes a part_df and goes and checks whether or not the data
        we are trying to grab from the record_name column is consistent across
        every component genertor from each record.

        When record_name is "operational_status", we are not going to check for
        consistency; instead we will grab the highest level of operational
        status that is associated with each records' component generators. The
        order of operational status is defined within the method as:
        'existing', 'proposed', then 'retired'. For example if a plant_unit is
        composed of two generators, and one of them is "existing" and another
        is "retired" the entire plant_unit will be considered "existing".

        Args:
            part_df (pandas.DataFrame): dataframe containing records associated
                with one plant part.
            part_name (string): name of plant-part.

        """
        qual_record = self.qual_record
        if qual_record in part_df.columns:
            logger.debug(f'{qual_record} already here.. ')
            return part_df

        record_df = self.mega_gens.copy()

        category_sorters = {
            'operational_status': ['existing', 'proposed', 'retired'],
        }
        if qual_record not in category_sorters.keys():
            consistent_records = self.get_consistent_qualifiers(record_df)
        if qual_record == 'operational_status':
            logger.debug(f'getting max {qual_record}')
            # restric the number of columns in here to only include the ones we
            # need, unlike get_consistent_qualifiers, dedup_on_category
            # preserves all of the columns from record_df
            record_df = record_df[self.base_cols + [qual_record]]
            consistent_records = pudl.helpers.dedup_on_category(
                record_df,
                self.base_cols,
                qual_record,
                category_sorters[qual_record]
            )
        non_nulls = consistent_records[
            consistent_records[qual_record].notnull()]
        logger.debug(
            f'merging in consistent {qual_record}: {len(non_nulls)}')
        return part_df.merge(consistent_records, how='left')

    def get_consistent_qualifiers(self, record_df):
        """
        Get fully consistent qualifier records.

        When data is a qualifer column is identical for every record in a
        plant part, we associate this data point with the record. If the data
        points for the related generator records are not identical, then
        nothing is associated with the record.

        Args:
            record_df (pandas.DataFrame): the dataframe with the record
            base_cols (list) : list of identifying columns.
            record_name (string) : name of qualitative record
        """
        # TODO: determine if we can move this up the chain so we can do this
        # once per plant-part, not once per plant-part * qualifer record
        qual_record = self.qual_record
        base_cols = self.base_cols
        entity_count_df = (
            pudl.helpers.count_records(
                record_df, base_cols, 'entity_occurences')
            .pipe(pudl.helpers.convert_cols_dtypes, 'eia')
        )
        record_count_df = (
            pudl.helpers.count_records(
                record_df, base_cols + [qual_record], 'record_occurences')
            . pipe(pudl.helpers.convert_cols_dtypes, 'eia')
        )
        re_count = (
            record_df[base_cols + [qual_record]]
            .merge(entity_count_df, how='left', on=base_cols)
            .merge(record_count_df, how='left', on=base_cols + [qual_record])
        )
        # find all of the matching records..
        consistent_records = (
            re_count[
                re_count['entity_occurences'] == re_count['record_occurences']]
            .drop(columns=['entity_occurences', 'record_occurences'])
            .drop_duplicates())
        return consistent_records


#################
# Data Validation
#################


def test_run_aggregations(plant_parts_eia, gens_mega):
    """
    Run a test of the aggregated columns.

    This test will used the plant_parts_eia, re-run groubys and check
    similarity.
    """
    for part_name in PLANT_PARTS_ORDERED:
        logger.info(f'Begining tests for {part_name}:')
        test_merge = _test_prep_merge(part_name, plant_parts_eia, gens_mega)
        for test_col in SUM_COLS:
            # Check if test aggregation is the same as generated aggreation
            # Apply a boolean column to the test df.
            test_merge[f'test_{test_col}'] = (
                (test_merge[f'{test_col}_test'] == test_merge[f'{test_col}'])
                | (test_merge[f'{test_col}_test'].isnull()
                    & test_merge[f'{test_col}'].isnull())
            )
            result = list(test_merge[f'test_{test_col}'].unique())
            logger.info(f'  Results for {test_col}: {result}')
            if not all(result):
                raise AssertionError(
                    f"{test_col}'s '"
                )


def _test_prep_merge(part_name, plant_parts_eia, gens_mega):
    """Run the test groupby and merge with the aggregations."""
    id_cols = PLANT_PARTS[part_name]['id_cols']
    plant_cap = (
        gens_mega
        .groupby(
            by=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD)
        [SUM_COLS]
        .sum(min_count=1)
        .reset_index()
    )
    test_merge = pd.merge(
        plant_parts_eia[plant_parts_eia.plant_part == part_name],
        plant_cap,
        on=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
        how='outer',
        indicator=True,
        suffixes=('', '_test'))
    return test_merge


#########################
# Module Helper Functions
#########################


def make_id_cols_list():
    """
    Get a list of the id columns (primary keys) for all of the plant parts.

    Returns:
        iterable: a list of the ID columns for all of the plant-parts,
            including `report_date`
    """
    return (
        IDX_TO_ADD + pudl.helpers.dedupe_n_flatten_list_of_lists(
            [x['id_cols'] for x in PLANT_PARTS.values()])
    )


def make_parts_to_ids_dict():
    """
    Make dict w/ plant-part names (keys) to the main id column (values).

    All plant-parts have 1 or 2 ID columns in ``PLANT_PARTS``: plant_id_eia and
    a secondary column (with the exception of the "plant" plant-part). The
    plant_id_eia column is always first, so we're going to grab the last column.

    Returns:
        dictionary: plant-part names (keys) cooresponding to the main ID column
        (value).

    """
    parts_to_ids = {}
    for part, part_dict in PLANT_PARTS.items():
        parts_to_ids[part] = PLANT_PARTS[part]['id_cols'][-1]
    return parts_to_ids


def add_record_id(part_df, id_cols, plant_part_col='plant_part'):
    """
    Add a record id to a compiled part df.

    We need a standardized way to refer to these compiled records that
    contains enough information in the id itself that in theory we could
    deconstruct the id and determine which plant id and plant part id
    columns are associated with this record.
    """
    ids = deepcopy(id_cols)
    # we want the plant id first... mostly just bc it'll be easier to read
    part_df = part_df.assign(record_id_eia=part_df.plant_id_eia.map(str))
    ids.remove('plant_id_eia')
    for col in ids:
        part_df = part_df.assign(
            record_id_eia=part_df.record_id_eia + "_" +
            part_df[col].astype(str))
    part_df = part_df.assign(
        record_id_eia=part_df.record_id_eia + "_" +
        part_df.report_date.dt.year.astype(str) + "_" +
        part_df[plant_part_col] + "_" +
        part_df.ownership.astype(str) + "_" +
        part_df.utility_id_eia.astype('Int64').astype(str))
    # add operational status only when records are not "operating" (i.e.
    # existing or retiring mid-year see MakeMegaGenTbl.abel_operating_gens()
    # for more details)
    non_op_mask = part_df.operational_status_pudl != 'operating'
    part_df.loc[non_op_mask, 'record_id_eia'] = (
        part_df.loc[non_op_mask, 'record_id_eia'] + "_"
        + part_df.loc[non_op_mask, 'operational_status_pudl'])
    return part_df


def assign_record_id_eia(test_df, plant_part_col='plant_part'):
    """
    Assign record ids to a df with a mix of plant parts.

    Args:
        test_df (pandas.DataFrame)
        plant_part_col (string)

    """
    dfs = []
    for part in PLANT_PARTS:
        dfs.append(add_record_id(
            part_df=test_df[test_df[plant_part_col] == part],
            id_cols=PLANT_PARTS[part]['id_cols'],
            plant_part_col=plant_part_col
        ))
    test_df_ids = pd.concat(dfs)
    return test_df_ids
