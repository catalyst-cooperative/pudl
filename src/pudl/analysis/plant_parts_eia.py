"""Aggregate plant parts to make an EIA master plant-part table.

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
portion records are labeled in the ``ownership_record_type`` column as "owned"
and the total records are labeled as "total".

This module refers to "true granularies". Many plant parts we cobble together
here in the master plant-part list refer to the same collection of
infrastructure as other plant-part list records. For example, if we have a
"plant_prime_mover" plant part record and a "plant_unit" plant part record
which were both cobbled together from the same two generators. We want to be
able to reduce the plant-part list to only unique collections of generators,
so we label the first unique granularity as a true granularity and label the
subsequent records as false granularities with the ``true_gran`` column. In
order to choose which plant-part to keep in these instances, we assigned a
hierarchy of plant parts, the order of the keys in :py:const:`PLANT_PARTS`
and label whichever plant-part comes first as the unique granularity.

**Recipe Book for the plant-part list**

:py:const:`PLANT_PARTS` is the main recipe book for how each of the plant-parts
need to be compiled. These plant-parts represent ways to group generators based
on widely reported values in EIA. All of these are logical ways to group
collections of generators - in most cases - but some groupings of generators
are more prevelant or relevant than others for certain types of plants.

The canonical example here is the ``plant_unit``. A unit is a collection of
generators that operate together - most notably the combined-cycle natural gas
plants. Combined-cycle units generally consist of a number of gas turbines
which feed excess steam to a number of steam turbines.

>>> df_gens = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1],
...     'generator_id': ['a', 'b', 'c'],
...     'unit_id_pudl': [1, 1, 1],
...     'prime_mover_code': ['CT', 'CT', 'CA'],
...     'capacity_mw': [50, 50, 100],
... })
>>> df_gens
    plant_id_eia    generator_id    unit_id_pudl    prime_mover_code    capacity_mw
0              1               a               1                  CT             50
1              1               b               1                  CT             50
2              1               c               1                  CA            100

A good example of a plant-part that isn't really logical also comes from a
combined-cycle unit. Grouping this example plant by the ``prime_mover_code``
would generate two records that would basically never show up in FERC1.
This stems from the inseparability of the generators.

>>> df_plant_prime_mover = pd.DataFrame({
...     'plant_id_eia': [1, 1],
...     'plant_part': ['plant_prime_mover', 'plant_prime_mover'],
...     'prime_mover_code': ['CT', 'CA'],
...     'capacity_mw': [100, 100],
... })
>>> df_plant_prime_mover
    plant_id_eia         plant_part    prime_mover_code    capacity_mw
0              1  plant_prime_mover                  CT            100
1              1  plant_prime_mover                  CA            100

In this case the unit is more relevant:

>>> df_plant_unit = pd.DataFrame({
...     'plant_id_eia': [1],
...     'plant_part': ['plant_unit'],
...     'unit_id_pudl': [1],
...     'capacity_mw': [200],
... })
>>> df_plant_unit
    plant_id_eia    plant_part    unit_id_pudl    capacity_mw
0              1    plant_unit               1            200

But if this same plant had both this combined-cycle unit and two more
generators that were self contained "GT" or gas combustion turbine, a frequent
way to group these generators is differnt for the combined-cycle unit and the
gas-turbine.

>>> df_gens = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1, 1, 1],
...     'generator_id': ['a', 'b', 'c', 'd', 'e'],
...     'unit_id_pudl': [1, 1, 1, 2, 3],
...     'prime_mover_code': ['CT', 'CT', 'CA', 'GT', 'GT'],
...     'capacity_mw': [50, 50, 100, 75, 75],
... })
>>> df_gens
    plant_id_eia    generator_id    unit_id_pudl    prime_mover_code    capacity_mw
0              1               a               1                  CT             50
1              1               b               1                  CT             50
2              1               c               1                  CA            100
3              1               d               2                  GT             75
4              1               e               3                  GT             75

>>> df_plant_part = pd.DataFrame({
...     'plant_id_eia': [1, 1],
...     'plant_part': ['plant_unit', 'plant_prime_mover'],
...     'unit_id_pudl': [1, pd.NA],
...     'prime_mover_code': [pd.NA, 'GT',],
...     'capacity_mw': [200, 150],
... })
>>> df_plant_part
    plant_id_eia           plant_part    unit_id_pudl    prime_mover_code    capacity_mw
0              1           plant_unit               1                <NA>            200
1              1    plant_prime_mover            <NA>                  GT            150

In this case last, the ``plant_unit`` record would have a null
``plant_prime_mover`` because the unit contains more than one
``prime_mover_code``. Same goes for the ``unit_id_pudl`` of the
``plant_prime_mover``. This is handled in the :class:``AddConsistentAttributes``.

**Overview of flow for generating the master unit list:**

The two main classes which enable the generation of the plant-part table are:

* :class:`MakeMegaGenTbl`: All of the plant parts are compiled from generators.
  So this class generates a big dataframe of generators with any ID and data
  columns we'll need. This is also where we add records regarding utility
  ownership slices. The table includes two records for every generator-owner:
  one for the "total" generator (assuming the owner owns 100% of the generator)
  and one for the report ownership fraction of that generator with all of the
  data columns scaled to the ownership fraction.
* :class:`MakePlantParts`: This class uses the generator dataframe as well as
  the information stored in :py:const:`PLANT_PARTS` to know how to aggregate each
  of the plant parts. Then we have plant part dataframes with the columns which
  identify the plant part and all of the data columns aggregated to the level of
  the plant part. With that compiled plant part dataframe we also add in qualifier
  columns with :class:`AddConsistentAttributes`. A qualifer column is a column which
  contain data that is not endemic to the plant part record (it is not one of
  the identifying columns or aggregated data columns) but the data is still
  useful data that is attributable to each of the plant part records. For more
  detail on what a qualifier column is, see :meth:`AddConsistentAttributes.execute`.

**Generating the plant-parts list**

There are two ways to generate the plant-parts table: one directly using the
:class:`pudl.output.pudltabl.PudlTabl` object and the other using the classes
from this module. Either option needs a :class:`pudl.output.pudltabl.PudlTabl`
object.

Create the :class:`pudl.output.pudltabl.PudlTabl` object:

.. code-block:: python

    import pudl
    pudl_engine = sa.create_engine(pudl.workspace.setup.get_defaults()['pudl_db'])
    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine,freq='AS')

Then make the table via pudl_out:

.. code-block:: python

    plant_parts_eia = pudl_out.plant_parts_eia()


OR make the table via objects in this module:

.. code-block:: python

    gens_mega = MakeMegaGenTbl().execute(mcoe, own_eia860)
    parts_compiler = MakePlantParts(pudl_out)
    plant_parts_eia = parts_compiler.execute(gens_mega=gens_mega)
"""
import warnings
from collections import OrderedDict
from copy import deepcopy
from typing import Literal

import numpy as np
import pandas as pd

import pudl
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)

# HALP: I need both of these setting set in order for the dfs in the docstrings
# to pass the doctests. Without them the formatting get all jumbled.
# but obviously this is the wrong place to do this.
# I tried adding these into conftest.py in pandas_terminal_width().
# I tried adding this into __init__.py.
# I tried adding this into the module docstring.
pd.options.display.width = 1000
pd.options.display.max_columns = 1000

PLANT_PARTS: OrderedDict[str, dict[str, list]] = OrderedDict(
    {
        "plant": {
            "id_cols": ["plant_id_eia"],
        },
        "plant_unit": {
            "id_cols": ["plant_id_eia", "unit_id_pudl"],
        },
        "plant_prime_mover": {
            "id_cols": ["plant_id_eia", "prime_mover_code"],
        },
        "plant_technology": {
            "id_cols": ["plant_id_eia", "technology_description"],
        },
        "plant_prime_fuel": {  # 'plant_primary_fuel': {
            "id_cols": ["plant_id_eia", "energy_source_code_1"],
        },
        "plant_ferc_acct": {
            "id_cols": ["plant_id_eia", "ferc_acct_name"],
        },
        "plant_operating_year": {
            "id_cols": ["plant_id_eia", "generator_operating_year"],
        },
        "plant_gen": {
            "id_cols": ["plant_id_eia", "generator_id"],
        },
    }
)
"""
dict: this dictionary contains a key for each of the 'plant parts' that should
end up in the plant parts list. The top-level value for each key is another
dictionary, which contains keys:

* id_cols (the primary key type id columns for this plant part). The
  plant_id_eia column must come first.

"""

PLANT_PARTS_LITERAL = Literal[
    "plant",
    "plant_unit",
    "plant_prime_mover",
    "plant_technology",
    "plant_prime_fuel",
    "plant_ferc_acct",
    "plant_operating_year",
    "plant_gen",
]


IDX_TO_ADD: list[str] = ["report_date", "operational_status_pudl"]
"""
list: list of additional columns to add to the id_cols in :py:const:`PLANT_PARTS`.
The id_cols are the base columns that we need to aggregate on, but we also need
to add the report date to keep the records time sensitive and the
operational_status_pudl to separate the operating plant-parts from the
non-operating plant-parts.
"""

IDX_OWN_TO_ADD: list[str] = ["utility_id_eia", "ownership_record_type"]
"""
list: list of additional columns beyond the :py:const:`IDX_TO_ADD` to add to the
id_cols in :py:const:`PLANT_PARTS` when we are dealing with plant-part records
that have been broken out into "owned" and "total" records for each of their
owners.
"""

SUM_COLS: list[str] = [
    "total_fuel_cost",
    "net_generation_mwh",
    "capacity_mw",
    "capacity_eoy_mw",
    "total_mmbtu",
]
"""list: list of columns to sum when aggregating a table."""

WTAVG_DICT = {
    "fuel_cost_per_mwh": "capacity_mw",
    "heat_rate_mmbtu_mwh": "capacity_mw",
    "fuel_cost_per_mmbtu": "capacity_mw",
}
"""
dict: a dictionary of columns (keys) to perform weighted averages on and
the weight column (values)
"""

CONSISTENT_ATTRIBUTE_COLS = [
    "fuel_type_code_pudl",
    "planned_generator_retirement_date",
    "generator_retirement_date",
    "generator_id",
    "unit_id_pudl",
    "technology_description",
    "energy_source_code_1",
    "prime_mover_code",
    "ferc_acct_name",
    "generator_operating_year",
]
"""
list: a list of column names to add as attributes when they are consistent into
the aggregated plant-part records. All the plant part ID columns must be in
consistent attributes.
"""

PRIORITY_ATTRIBUTES_DICT = {
    "operational_status": ["existing", "proposed", "retired"],
}

MAX_MIN_ATTRIBUTES_DICT = {
    "installation_year": {
        "assign_col": {"installation_year": lambda x: x.generator_operating_year},
        "dtype": "Int64",
        "keep": "first",
    },
    "construction_year": {
        "assign_col": {"construction_year": lambda x: x.generator_operating_year},
        "dtype": "Int64",
        "keep": "last",
    },
}

FIRST_COLS = [
    "plant_id_eia",
    "report_date",
    "plant_part",
    "generator_id",
    "unit_id_pudl",
    "prime_mover_code",
    "energy_source_code_1",
    "technology_description",
    "ferc_acct_name",
    "utility_id_eia",
    "true_gran",
    "appro_part_label",
]

PPE_COLS = [
    "record_id_eia",
    "plant_name_ppe",
    "plant_part",
    "report_year",
    "report_date",
    "ownership_record_type",
    "plant_name_eia",
    "plant_id_eia",
    "generator_id",
    "unit_id_pudl",
    "prime_mover_code",
    "energy_source_code_1",
    "technology_description",
    "ferc_acct_name",
    "generator_operating_year",
    "utility_id_eia",
    "utility_id_pudl",
    "true_gran",
    "appro_part_label",
    "appro_record_id_eia",
    "record_count",
    "fraction_owned",
    "ownership_dupe",
    "operational_status",
    "operational_status_pudl",
]


class MakeMegaGenTbl:
    """Compiler for a MEGA generator table with ownership integrated.

    Examples
    --------
    **Input Tables**

    Here is an example of one plant with three generators. We will use
    ``capacity_mw`` as the data column.

    >>> mcoe = pd.DataFrame({
    ...     'plant_id_eia': [1, 1, 1],
    ...     'report_date': ['2020-01-01', '2020-01-01','2020-01-01'],
    ...     'generator_id': ['a', 'b', 'c'],
    ...     'utility_id_eia': [111, 111, 111],
    ...     'unit_id_pudl': [1, 1, 1],
    ...     'prime_mover_code': ['CT', 'CT', 'CA'],
    ...     'technology_description': [
    ...         'Natural Gas Fired Combined Cycle', 'Natural Gas Fired Combined Cycle', 'Natural Gas Fired Combined Cycle'
    ...     ],
    ...     'operational_status': ['existing', 'existing','existing'],
    ...     'generator_retirement_date': [pd.NA, pd.NA, pd.NA],
    ...     'capacity_mw': [50, 50, 100],
    ... }).astype({
    ...     'generator_retirement_date': "datetime64[ns]",
    ...     'report_date': "datetime64[ns]",
    ... })
    >>> mcoe
        plant_id_eia    report_date 	generator_id   utility_id_eia 	unit_id_pudl 	prime_mover_code              technology_description   operational_status  generator_retirement_date 	capacity_mw
    0 	           1     2020-01-01 	           a              111 	           1 	              CT    Natural Gas Fired Combined Cycle             existing               NaT 	         50
    1 	           1     2020-01-01 	           b              111 	           1 	              CT    Natural Gas Fired Combined Cycle             existing               NaT 	         50
    2 	           1     2020-01-01 	           c              111 	           1 	              CA    Natural Gas Fired Combined Cycle             existing               NaT 	        100

    The ownership table from EIA 860 includes one record for every owner of
    each generator. In this example generator ``c`` has two owners.

    >>> df_own_eia860 = pd.DataFrame({
    ...     'plant_id_eia': [1, 1, 1, 1],
    ...     'report_date': ['2020-01-01', '2020-01-01','2020-01-01', '2020-01-01'],
    ...     'generator_id': ['a', 'b', 'c', 'c'],
    ...     'utility_id_eia': [111, 111, 111, 111],
    ...     'owner_utility_id_eia': [111, 111, 111, 888],
    ...     'fraction_owned': [1, 1, .75, .25]
    ... }).astype({'report_date': "datetime64[ns]"})
    >>> df_own_eia860
        plant_id_eia 	report_date   generator_id 	utility_id_eia 	owner_utility_id_eia  fraction_owned
    0 	           1 	 2020-01-01 	         a 	           111 	                 111 	        1.00
    1 	           1 	 2020-01-01 	         b 	           111 	                 111 	        1.00
    2 	           1 	 2020-01-01 	         c 	           111 	                 111 	        0.75
    3 	           1 	 2020-01-01 	         c 	           111 	                 888 	        0.25

    **Output Mega Generators Table**

    ``MakeMegaGenTbl().execute(mcoe, df_own_eia860, slice_cols=['capacity_mw'])``
    produces the output table ``gens_mega`` which includes two main sections:
    the generators with a "total" ownership stake for each of their owners and
    the generators with an "owned" ownership stake for each of their
    owners. For the generators that are owned 100% by one utility, the
    records are identical except the ``ownership_record_type`` column. For the
    generators that have more than one owner, there are two "total" records
    with 100% of the capacity of that generator - one for each owner - and
    two "owned" records with the capacity scaled to the ownership stake
    of each of the owner utilites - represented by ``fraction_owned``.
    """

    def __init__(self):
        """Initialize object which creates a MEGA generator table.

        The coordinating function here is :meth:`execute`.
        """
        self.id_cols_list = make_id_cols_list()

    def execute(
        self,
        mcoe: pd.DataFrame,
        own_eia860: pd.DataFrame,
        slice_cols: list[str] = SUM_COLS,
        validate_own_merge: str = "1:m",
    ) -> pd.DataFrame:
        """Make the mega generators table with ownership integrated.

        Args:
            mcoe: generator-based mcoe table from :meth:`pudl.output.PudlTabl.mcoe()`
            own_eia860: ownership table from :meth:`pudl.output.PudlTabl.own_eia860()`
            scale_cols: list of columns to slice by ownership fraction in
                :meth:`MakeMegaGenTbl.scale_by_ownership`. Default is :py:const:`SUM_COLS`
            validate_own_merge: how the merge between ``mcoe`` and ``own_eia860``
                is to be validated via ``pd.merge``. If there should be one
                record for each plant/generator/date in ``mcoe`` then the default
                `1:m` should be used.

        Returns:
            a table of all of the generators with identifying columns and data
            columns, sliced by ownership which makes "total" and "owned"
            records for each generator owner. The "owned" records have the
            generator's data scaled to the ownership percentage (e.g. if a 200
            MW generator has a 75% stake owner and a 25% stake owner, this will
            result in two "owned" records with 150 MW and 50 MW). The "total"
            records correspond to the full plant for every owner (e.g. using
            the same 2-owner 200 MW generator as above, each owner will have a
            records with 200 MW).
        """
        logger.info("Generating the mega generator table with ownership.")

        gens_mega = (
            self.get_gens_mega_table(mcoe)
            .pipe(self.label_operating_gens)
            .pipe(self.scale_by_ownership, own_eia860, slice_cols, validate_own_merge)
        )
        return gens_mega

    def get_gens_mega_table(self, mcoe):
        """Compile the main generators table that will be used as base of PPL.

        Get a table of all of the generators there ever were and all of the
        data PUDL has to offer about those generators. This generator table
        will be used to compile all of the "plant-parts", so we need to ensure
        that any of the id columns from the other plant-parts are in this
        generator table as well as all of the data columns that we are going to
        aggregate to the various plant-parts.

        Returns:
            pandas.DataFrame
        """
        all_gens = pd.merge(  # Add EIA FERC acct fields
            mcoe,
            pudl.helpers.get_eia_ferc_acct_map(),
            on=["technology_description", "prime_mover_code"],
            validate="m:1",
            how="left",
        )
        all_gens.loc[:, "generator_operating_year"] = all_gens[
            "generator_operating_date"
        ].dt.year
        all_gens = all_gens.astype({"generator_operating_year": "Int64"})
        return all_gens

    def label_operating_gens(self, gen_df: pd.DataFrame) -> pd.DataFrame:
        """Label the operating generators.

        We want to distinguish between "operating" generators (those that
        report as "existing" and those that retire mid-year) and everything
        else so that we can group the operating generators into their own
        plant-parts separate from retired or proposed generators. We do this by
        creating a new label column called "operational_status_pudl".

        This method also adds a column called "capacity_eoy_mw", which is the
        end of year capacity of the generators. We assume that if a generator
        isn't "existing", its EOY capacity should be zero.

        Args:
            gen_df (pandas.DataFrame): annual table of all generators from EIA.

        Returns
            pandas.DataFrame: annual table of all generators from EIA that
            operated within each reporting year.

        TODO:
            This function results in warning: `PerformanceWarning: DataFrame
            is highly fragmented...` I expect this is because of the number of
            columns that are being assigned here via `.loc[:, col_to_assign]`.
        """
        mid_year_retiree_mask = (
            gen_df.generator_retirement_date.dt.year == gen_df.report_date.dt.year
        )
        existing_mask = gen_df.operational_status == "existing"
        operating_mask = existing_mask | mid_year_retiree_mask
        # we've going to make a new column which combines both the mid-year
        # reitrees and the fully existing gens into one code so we can group
        # them together later on
        gen_df.loc[:, "operational_status_pudl"] = gen_df.loc[
            :, "operational_status"
        ].mask(operating_mask, "operating")
        gen_df.loc[:, "capacity_eoy_mw"] = gen_df.loc[:, "capacity_mw"].mask(
            ~existing_mask, 0
        )

        logger.info(
            f"Labeled {len(gen_df.loc[~existing_mask])/len(gen_df):.02%} of "
            "generators as non-operative."
        )
        return gen_df

    def scale_by_ownership(
        self, gens_mega, own_eia860, scale_cols=SUM_COLS, validate="1:m"
    ):
        """Generate proportional data by ownership %s.

        Why do we have to do this at all? Sometimes generators are owned by
        many different utility owners that own slices of that generator. EIA
        reports which portion of each generator is owned by which utility
        relatively clearly in their ownership table. On the other hand, in
        FERC1, sometimes a partial owner reports the full plant-part, sometimes
        they report only their ownership portion of the plant-part. And of
        course it is not labeld in FERC1. Because of this, we need to compile
        all of the possible ownership slices of the EIA generators.

        In order to accumulate every possible version of how a generator could
        be reported, this method generates two records for each generator's
        reported owners: one of the portion of the plant part they own and one
        for the plant-part as a whole. The portion records are labeled in the
        ``ownership_record_type`` column as "owned" and the total records are labeled as
        "total".

        In this function we merge in the ownership table so that generators
        with multiple owners then have one record per owner with the
        ownership fraction (in column ``fraction_owned``). Because the ownership
        table only contains records for generators that have multiple owners,
        we assume that all other generators are owned 100% by their operator.
        Then we generate the "total" records by duplicating the "owned" records
        but assigning the ``fraction_owned`` to be 1 (i.e. 100%).
        """
        # grab the ownership table, and reduce it to only the columns we need
        own860 = own_eia860[
            [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "fraction_owned",
                "owner_utility_id_eia",
            ]
        ].pipe(pudl.helpers.convert_cols_dtypes, "eia")
        # we're left merging BC we've removed the retired gens, which are
        # reported in the ownership table
        gens_mega = (
            gens_mega.merge(
                own860,
                how="left",
                on=["plant_id_eia", "generator_id", "report_date"],
                validate=validate,
            )
            .assign(  # assume gens that don't show up in the own table have one 100% owner
                fraction_owned=lambda x: x.fraction_owned.fillna(value=1),
                # assign the operator id as the owner if null bc if a gen isn't
                # reported in the own_eia860 table we can assume the operator
                # is the owner
                owner_utility_id_eia=lambda x: x.owner_utility_id_eia.fillna(
                    x.utility_id_eia
                ),
                ownership_record_type="owned",
            )  # swap in the owner as the utility
            .drop(columns=["utility_id_eia"])
            .rename(columns={"owner_utility_id_eia": "utility_id_eia"})
        )

        # duplicate all of these "owned" records, asign 1 to all of the
        # fraction_owned column to indicate 100% ownership, and add these new
        # "total" records to the "owned"
        gens_mega = pd.concat(
            [
                gens_mega,
                gens_mega.copy().assign(
                    fraction_owned=1, ownership_record_type="total"
                ),
            ]
        )
        gens_mega.loc[:, scale_cols] = gens_mega.loc[:, scale_cols].multiply(
            gens_mega["fraction_owned"], axis="index"
        )
        return gens_mega


class MakePlantParts:
    """Compile the plant parts for the master unit list.

    This object generates a master list of different "plant-parts", which
    are various collections of generators - i.e. units, fuel-types, whole
    plants, etc. - as well as various ownership arrangements. Each
    plant-part is included in the master plant-part table associated with
    each of the plant-part's owner twice - once with the data scaled to the
    fraction of each owners' ownership and another for a total plant-part
    for each owner.

    This master plant parts table is generated by first creating a complete
    generators table - with all of the data columns we will be aggregating
    to different plant-part's and sliced and scaled by ownership. Then we use the
    complete generator table to aggregate by each of the plant-part
    categories. Next we add a label for each plant-part record which indicates
    whether or not the record is a unique grouping of generator records.

    The coordinating function here is :meth:`execute`.
    """

    def __init__(self, pudl_out):
        """Initialize instance of :class:`MakePlantParts`.

        Args:
            pudl_out (pudl.output.pudltabl.PudlTabl): An object used to create
                the tables for EIA and FERC Form 1 analysis.
        """
        self.pudl_out = pudl_out
        self.freq = pudl_out.freq
        self.parts_to_ids = make_parts_to_ids_dict()

        # get a list of all of the id columns that constitue the primary keys
        # for all of the plant parts
        self.id_cols_list = make_id_cols_list()

    def execute(self, gens_mega):
        """Aggregate and slice data points by each plant part.

        Returns:
            pandas.DataFrame: The complete plant parts list
        """
        # aggregate everything by each plant part
        df_keys = list(self.pudl_out._dfs.keys())
        for k in df_keys:
            del self.pudl_out._dfs[k]
        part_dfs = []
        for part_name in PLANT_PARTS:
            part_df = PlantPart(part_name).execute(gens_mega)
            # add in the attributes!
            for attribute_col in CONSISTENT_ATTRIBUTE_COLS:
                part_df = AddConsistentAttributes(attribute_col, part_name).execute(
                    part_df, gens_mega
                )
            for attribute_col in PRIORITY_ATTRIBUTES_DICT.keys():
                part_df = AddPriorityAttribute(attribute_col, part_name).execute(
                    part_df, gens_mega
                )
            for attribute_col in MAX_MIN_ATTRIBUTES_DICT.keys():
                part_df = AddMaxMinAttribute(
                    attribute_col,
                    part_name,
                    assign_col_dict=MAX_MIN_ATTRIBUTES_DICT[attribute_col][
                        "assign_col"
                    ],
                ).execute(
                    part_df,
                    gens_mega,
                    att_dtype=MAX_MIN_ATTRIBUTES_DICT[attribute_col]["dtype"],
                    keep=MAX_MIN_ATTRIBUTES_DICT[attribute_col]["keep"],
                )
            # assert that all the plant part ID columns are now in part_df
            assert {
                col for part in PLANT_PARTS for col in PLANT_PARTS[part]["id_cols"]
            }.issubset(part_df.columns)
            part_dfs.append(part_df)
        plant_parts_eia = pd.concat(part_dfs)
        plant_parts_eia = TrueGranLabeler().execute(plant_parts_eia)
        # clean up, add additional columns
        self.plant_parts_eia = (
            self.add_additonal_cols(plant_parts_eia)
            .pipe(pudl.helpers.organize_cols, FIRST_COLS)
            .pipe(self._clean_plant_parts)
            .pipe(Resource.from_id("plant_parts_eia").format_df)
        )
        self.plant_parts_eia.index = self.plant_parts_eia.index.astype("string")
        self.validate_ownership_for_owned_records(self.plant_parts_eia)
        validate_run_aggregations(self.plant_parts_eia, gens_mega)
        return self.plant_parts_eia

    #######################################
    # Add Entity Columns and Final Cleaning
    #######################################

    def add_additonal_cols(self, plant_parts_eia):
        """Add additonal data and id columns.

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
                df=plant_parts_eia, min_cap_fact=-0.5, max_cap_fact=1.5, freq=self.freq
            )
            .merge(
                self.pudl_out.plants_eia860()[
                    ["plant_id_eia", "plant_id_pudl"]
                ].drop_duplicates(),
                how="left",
                on=[
                    "plant_id_eia",
                ],
            )
            .merge(
                self.pudl_out.utils_eia860()[
                    ["utility_id_eia", "utility_id_pudl"]
                ].drop_duplicates(),
                how="left",
                on=["utility_id_eia"],
            )
            .assign(
                ownership_dupe=lambda x: np.where(
                    (x.ownership_record_type == "owned") & (x.fraction_owned == 1),
                    True,
                    False,
                )
            )
        )
        return plant_parts_eia

    def _clean_plant_parts(self, plant_parts_eia):
        plant_parts_eia = (
            plant_parts_eia.assign(
                report_year=lambda x: x.report_date.dt.year,
                plant_id_report_year=lambda x: x.plant_id_pudl.astype(str)
                + "_"
                + x.report_year.astype(str),
            )
            .pipe(
                pudl.helpers.cleanstrings_snake,
                ["record_id_eia", "appro_record_id_eia"],
            )
            .set_index("record_id_eia")
        )
        return plant_parts_eia[~plant_parts_eia.index.duplicated(keep="first")]

    #################
    # Testing Methods
    #################

    def validate_ownership_for_owned_records(self, plant_parts_eia):
        """Test ownership - fraction owned for owned records.

        This test can be run at the end of or with the result of
        :meth:`MakePlantParts.execute`. It tests a few aspects of the the
        fraction_owned column and raises assertions if the tests fail.
        """
        test_own_df = (
            plant_parts_eia.groupby(
                by=self.id_cols_list + ["plant_part", "ownership_record_type"],
                dropna=False,
                observed=True,
            )[["fraction_owned", "capacity_mw"]]
            .sum(min_count=1)
            .reset_index()
        )

        owned_one_frac = test_own_df[
            (~np.isclose(test_own_df.fraction_owned, 1))
            & (test_own_df.capacity_mw != 0)
            & (test_own_df.capacity_mw.notnull())
            & (test_own_df.ownership_record_type == "owned")
        ]

        if not owned_one_frac.empty:
            self.test_own_df = test_own_df
            self.owned_one_frac = owned_one_frac
            raise AssertionError(
                "Hello friend, you did a bad. It happens... There are "
                f"{len(owned_one_frac)} rows where fraction_owned does not sum "
                "to 100% for the owned records. "
                "Check cached `owned_one_frac` & `test_own_df` and `scale_by_ownership()`"
            )

        no_frac_n_cap = test_own_df[
            (test_own_df.capacity_mw == 0) & (test_own_df.fraction_owned == 0)
        ]
        self.no_frac_n_cap = no_frac_n_cap
        if len(no_frac_n_cap) > 60:
            self.no_frac_n_cap = no_frac_n_cap
            warnings.warn(
                f"""Too many nothings, you nothing. There shouldn't been much
                more than 60 instances of records with zero capacity_mw (and
                therefor zero fraction_owned) and you got {len(no_frac_n_cap)}.
                """
            )


class PlantPart:
    """Plant-part table maker.

    The coordinating method here is :meth:`execute`.

    **Examples**

    Below are some examples of how the main processing step in this class
    operates: :meth:`PlantPart.ag_part_by_own_slice`. If we have a plant with
    four generators that looks like this:

    >>> gens_mega = pd.DataFrame({
    ...     'plant_id_eia': [1, 1, 1, 1],
    ...     'report_date': ['2020-01-01', '2020-01-01', '2020-01-01', '2020-01-01',],
    ...     'utility_id_eia': [111, 111, 111, 111],
    ...     'generator_id': ['a', 'b', 'c', 'd'],
    ...     'prime_mover_code': ['ST', 'GT', 'CT', 'CA'],
    ...     'energy_source_code_1': ['BIT', 'NG', 'NG', 'NG'],
    ...     'ownership_record_type': ['total', 'total', 'total', 'total',],
    ...     'operational_status_pudl': ['operating', 'operating', 'operating', 'operating'],
    ...     'capacity_mw': [400, 50, 125, 75],
    ... }).astype({
    ...     'report_date': 'datetime64[ns]',
    ... })
    >>> gens_mega
        plant_id_eia   report_date 	utility_id_eia 	generator_id 	prime_mover_code 	energy_source_code_1 	ownership_record_type 	operational_status_pudl 	capacity_mw
    0 	           1    2020-01-01 	           111 	           a 	              ST 	                 BIT 	                total 	              operating 	        400
    1 	           1    2020-01-01 	           111 	           b 	              GT 	                  NG 	                total 	              operating 	         50
    2 	           1    2020-01-01 	           111 	           c 	              CT 	                  NG 	                total 	              operating 	        125
    3 	           1    2020-01-01 	           111 	           d 	              CA 	                  NG 	                total 	              operating 	         75

    This ``gens_mega`` table can then be aggregated by ``plant``, ``plant_prime_fuel``,
    ``plant_prime_mover``, or ``plant_gen``.
    """

    def __init__(self, part_name: PLANT_PARTS_LITERAL):
        """Initialize an object which makes a tbl for a specific plant-part.

        Args:
            part_name (str): the name of the part to aggregate to. Names can be
                only those in :py:const:`PLANT_PARTS`
        """
        self.part_name = part_name
        self.id_cols = PLANT_PARTS[part_name]["id_cols"]

    def execute(
        self,
        gens_mega: pd.DataFrame,
        sum_cols: list[str] = SUM_COLS,
        wtavg_dict: dict = WTAVG_DICT,
    ) -> pd.DataFrame:
        """Get a table of data aggregated by a specific plant-part.

        This method will take ``gens_mega`` and aggregate the generator records
        to the level of the plant-part. This is mostly done via
        :meth:`ag_part_by_own_slice`. Then several additional columns are added
        and the records are labeled as true or false granularities.

        Returns:
            a table with records that have been aggregated to a plant-part.
        """
        part_df = (
            self.ag_part_by_own_slice(
                gens_mega, sum_cols=sum_cols, wtavg_dict=wtavg_dict
            )
            .pipe(self.ag_fraction_owned)
            .assign(plant_part=self.part_name)
            .pipe(  # add standard record id w/ year
                add_record_id,
                id_cols=self.id_cols,
                plant_part_col="plant_part",
                year=True,
            )
            .pipe(  # add additional record id that DOESN'T CARE ABOUT TIME
                add_record_id,
                id_cols=self.id_cols,
                plant_part_col="plant_part",
                year=False,
            )
            .pipe(self.add_new_plant_name, gens_mega)
            .pipe(self.add_record_count_per_plant)
        )
        return part_df

    def ag_part_by_own_slice(
        self,
        gens_mega,
        sum_cols=SUM_COLS,
        wtavg_dict=WTAVG_DICT,
    ) -> pd.DataFrame:
        """Aggregate the plant part by seperating ownership types.

        There are total records and owned records in this master unit list.
        Those records need to be aggregated differently to scale. The "total"
        ownership slice is now grouped and aggregated as a single version of the
        full plant and then the utilities are merged back. The "owned"
        ownership slice is grouped and aggregated with the utility_id_eia, so
        the portions of generators created by scale_by_ownership will be
        appropriately aggregated to each plant part level.

        Returns:
            pandas.DataFrame: dataframe aggregated to the level of the
            part_name
        """
        logger.info(f"begin aggregation for: {self.part_name}")
        # id_cols = PLANT_PARTS[self.part_name]['id_cols']
        # split up the 'owned' slices from the 'total' slices.
        # this is because the aggregations are different
        part_own = gens_mega.loc[gens_mega.ownership_record_type == "owned"].copy()
        part_tot = gens_mega.loc[gens_mega.ownership_record_type == "total"].copy()
        if len(gens_mega) != len(part_own) + len(part_tot):
            raise AssertionError(
                "Error occured in breaking apart ownership types."
                "The total and owned slices should equal the total records."
                "Check for nulls in the ownership_record_type column."
            )
        part_own = pudl.helpers.sum_and_weighted_average_agg(
            df_in=part_own,
            by=self.id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
            sum_cols=sum_cols,
            wtavg_dict=wtavg_dict,
        )

        # we want a "total" record for each of the utilities that own any slice
        # of a particular plant-part. To achieve this, we are going to remove
        # the utility info (and drop duplicates bc a plant-part with many
        # generators will have multiple duplicate records for each owner)
        # we are going to generate the aggregated output for a utility-less
        # "total" record and then merge back in the many utilites so each of
        # the utilities is associated with an aggergated "total" plant-part
        # record
        part_tot_no_utils = part_tot.drop(columns=["utility_id_eia"]).drop_duplicates()
        # still need to re-calc the fraction owned for the part
        part_tot_out = (
            pudl.helpers.sum_and_weighted_average_agg(
                df_in=part_tot_no_utils,
                by=self.id_cols + IDX_TO_ADD,
                sum_cols=sum_cols,
                wtavg_dict=wtavg_dict,
            )
            .pipe(pudl.helpers.convert_cols_dtypes, "eia")
            .merge(
                part_tot[self.id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD].drop_duplicates(),
                on=self.id_cols + IDX_TO_ADD,
                how="left",
                validate="1:m",
            )
        )
        part_ag = pd.concat([part_own, part_tot_out]).pipe(
            pudl.helpers.convert_cols_dtypes, "eia"
        )

        return part_ag

    def ag_fraction_owned(self, part_ag: pd.DataFrame):
        """Calculate the fraction owned for a plant-part df.

        This method takes a dataframe of records that are aggregated to the
        level of a plant-part (with certain ``id_cols``) and appends a
        fraction_owned column, which indicates the % ownership that a
        particular utility owner has for each aggreated plant-part record.

        For partial owner records (ownership_record_type == "owned"), fraction_owned is
        calcuated based on the portion of the capacity and the total capacity
        of the plant. For total owner records (ownership_record_type == "total"), the
        fraction_owned is always 1.

        This method is meant to be run after :meth:`ag_part_by_own_slice`.

        Args:
            part_ag:
        """  # noqa: D417
        # we must first get the total capacity of the full plant
        # Note: we could simply not include the ownership_record_type == "total" records
        # We are automatically assign fraction_owned == 1 to them, but it seems
        # cleaner to run the full df through this same grouby
        frac_owned = part_ag.groupby(
            by=self.id_cols + IDX_TO_ADD + ["ownership_record_type"], observed=True
        )[["capacity_mw"]].sum(min_count=1)
        # then merge the total capacity with the plant-part capacity to use to
        # calculate the fraction_owned
        part_frac = (
            pd.merge(
                part_ag,
                frac_owned,
                right_index=True,
                left_on=frac_owned.index.names,
                suffixes=("", "_total"),
            )
            .assign(
                fraction_owned=lambda x: np.where(
                    x.ownership_record_type == "owned",
                    x.capacity_mw / x.capacity_mw_total,
                    1,
                )
            )
            .drop(columns=["capacity_mw_total"])
            .pipe(pudl.helpers.convert_cols_dtypes, "eia")
        )
        return part_frac

    def add_new_plant_name(self, part_df, gens_mega):
        """Add plants names into the compiled plant part df.

        Args:
            part_df (pandas.DataFrame):  dataframe containing records associated
                with one plant part (ex: all plant's or  plant_prime_mover's).
            gens_mega (pandas.DataFrame): a table of all of the generators with
                identifying columns and data columns, sliced by ownership which
                makes "total" and "owned" records for each generator owner.
        """
        part_df = pd.merge(
            part_df,
            gens_mega[self.id_cols + ["plant_name_eia"]].drop_duplicates(),
            on=self.id_cols,
            how="left",
        ).assign(plant_name_ppe=lambda x: x.plant_name_eia)
        # we don't want the plant_id_eia to be part of the plant name, but all
        # of the other parts should have their id column in the new plant name
        if self.part_name != "plant":
            col = [x for x in self.id_cols if x != "plant_id_eia"][0]
            part_df.loc[part_df[col].notnull(), "plant_name_ppe"] = (
                part_df["plant_name_ppe"] + " " + part_df[col].astype(str)
            )
        return part_df

    def add_record_count_per_plant(self, part_df: pd.DataFrame) -> pd.DataFrame:
        """Add a record count for each set of plant part records in each plant.

        Args:
            part_df: dataframe containing records associated
                with one plant part (ex: all plant's or  plant_prime_mover's).

        Returns:
            augmented version of ``part_df`` with a new column named
            ``record_count``
        """
        group_cols = ["plant_id_eia"] + IDX_TO_ADD + IDX_OWN_TO_ADD
        # count unique records per plant
        part_df.loc[:, "record_count"] = part_df.groupby(group_cols, observed=True)[
            "record_id_eia"
        ].transform("count")
        return part_df


class TrueGranLabeler:
    """Label the plant-part table records with their true granularity.

    The coordinating function here is :meth``execute``.
    """

    def execute(self, ppl):
        """Merge the true granularity labels onto the plant part df.

        This method will add the columns ``true_gran``, ``appro_part_label``, and
        ``appro_record_id_eia`` to the plant parts list which denote whether
        each plant-part is a true or false granularity.

        First the plant part list records are matched to generators. Then
        the matched records are sorted by the order of keys in PLANT_PARTS and the
        highest granularity record for each generator is marked as the true
        granularity. The appropriate true granular part label and record id
        is then merged on to get the plant part table with true granularity labels.

        Arguments:
            ppl: (pd.DataFrame) The plant parts list
        """
        parts_to_gens = match_to_single_plant_part(
            multi_gran_df=ppl,
            ppl=ppl,
            part_name="plant_gen",
            cols_to_keep=["plant_part"],
        )[["record_id_eia_og", "record_id_eia", "plant_part_og"]].rename(
            columns={
                "record_id_eia": "gen_id",
                "record_id_eia_og": "record_id_eia",
                "plant_part_og": "plant_part",
            }
        )
        # concatenate the gen id's to get the combo of gens for each record
        combos = (
            parts_to_gens.sort_values(["gen_id"])
            .groupby(["record_id_eia"])["gen_id"]
            .apply(lambda x: ",".join(x))
            .rename("gens_combo")
        )
        parts_to_gens = parts_to_gens.merge(
            combos, how="left", left_on="record_id_eia", right_index=True
        )

        # categorical columns allow sorting by PLANT_PARTS key order
        parts_to_gens["plant_part"] = pd.Categorical(
            parts_to_gens["plant_part"], PLANT_PARTS.keys()
        )
        parts_to_gens = parts_to_gens.sort_values("plant_part")
        # get the true gran records by finding duplicate gen combos
        # this marks duplicate grans as True except for the first occurrence
        # non-duplicated granularities (unique records) are also marked False
        dupes = parts_to_gens.duplicated(subset=["gens_combo"], keep="first")
        # the False (non duplicated) granularities are now True in true_gran
        parts_to_gens.loc[:, "true_gran"] = ~(dupes)
        # drop duplicate record ids so there is one row for each record
        parts_to_gens = parts_to_gens.drop_duplicates(subset=["record_id_eia"])
        true_grans = (
            parts_to_gens[parts_to_gens.true_gran][
                ["record_id_eia", "plant_part", "gens_combo"]
            ]
            .rename(
                columns={
                    "record_id_eia": "appro_record_id_eia",
                    "plant_part": "appro_part_label",
                }
            )
            .astype({"appro_part_label": "string"})
        )
        # merge the true gran cols onto the parts to gens dataframe
        # drop cols to get a table with just record id and true gran cols
        record_id_true_gran = parts_to_gens.merge(
            true_grans, on="gens_combo", how="left", validate="m:1"
        ).drop(["plant_part", "gens_combo", "gen_id"], axis=1)

        ppl_true_gran = ppl.merge(
            record_id_true_gran, how="left", on="record_id_eia", validate="1:1"
        )

        return ppl_true_gran


class AddAttribute:
    """Base class for adding attributes to plant-part tables."""

    def __init__(
        self,
        attribute_col: str,
        part_name: str,
        assign_col_dict: dict[str, str] | None = None,
    ):
        """Initialize a attribute adder.

        Args:
            attribute_col (string): name of qualifer record that you want added.
                Must be in :py:const:`CONSISTENT_ATTRIBUTE_COLS` or a key in
                :py:const:`PRIORITY_ATTRIBUTES_DICT`
                or :py:const:`MAX_MIN_ATTRIBUTES_DICT`.
            part_name (str): the name of the part to aggregate to. Names can be
                only those in :py:const:`PLANT_PARTS`
        """  # noqa: D417
        assert attribute_col in CONSISTENT_ATTRIBUTE_COLS + list(
            PRIORITY_ATTRIBUTES_DICT.keys()
        ) + list(MAX_MIN_ATTRIBUTES_DICT.keys())
        self.attribute_col = attribute_col
        # the base columns will be the id columns, plus the other two main ids
        self.part_name = part_name
        self.id_cols = PLANT_PARTS[part_name]["id_cols"]
        self.base_cols = self.id_cols + IDX_TO_ADD
        self.assign_col_dict = assign_col_dict

    def assign_col(self, gens_mega):
        """Add a new column to gens_mega."""
        if self.assign_col_dict is not None:
            return gens_mega.assign(**self.assign_col_dict)
        else:
            return gens_mega


class AddConsistentAttributes(AddAttribute):
    """Adder of attributes records to a plant-part table."""

    def execute(self, part_df, gens_mega):
        """Get qualifier records.

        For an individual dataframe of one plant part (e.g. only
        "plant_prime_mover" plant part records), we typically have identifying
        columns and aggregated data columns. The identifying columns for a
        given plant part are only those columns which are required to uniquely
        specify a record of that type of plant part. For example, to uniquely
        specify a plant_unit record, we need both ``plant_id_eia``,
        ``unit_id_pudl``, ``report_date`` and nothing else. In other words,
        the identifying columns for a given plant part would make up a natural
        composite primary key for a table composed entirely of that type of
        plant part. Every plant part is cobbled together from generator
        records, so each record in each part_df can be thought of as a
        collection of generators.

        Identifier and qualifier columns are the same columns; whether a column
        is an identifier or a qualifier is a function of the plant part you're
        considering. All the other columns which could be identifiers in the
        context of other plant parrts (but aren't for this plant part) are
        qualifiers.

        This method takes a part_df and goes and checks whether or not the data
        we are trying to grab from the record_name column is consistent across
        every component genertor from each record.

        Args:
            part_df (pandas.DataFrame): dataframe containing records associated
                with one plant part.
            gens_mega (pandas.DataFrame): a table of all of the generators with
                identifying columns and data columns, sliced by ownership which
                makes "total" and "owned" records for each generator owner.
        """
        attribute_col = self.attribute_col
        if attribute_col in part_df.columns:
            logger.debug(f"{attribute_col} already here.. ")
            return part_df

        record_df = gens_mega.copy()
        record_df = self.assign_col(record_df)

        consistent_records = self.get_consistent_qualifiers(record_df)

        non_nulls = consistent_records[consistent_records[attribute_col].notnull()]
        logger.debug(f"merging in consistent {attribute_col}: {len(non_nulls)}")
        return part_df.merge(consistent_records, how="left")

    def get_consistent_qualifiers(self, record_df):
        """Get fully consistent qualifier records.

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
        attribute_col = self.attribute_col
        base_cols = self.base_cols
        entity_count_df = pudl.helpers.count_records(
            record_df, base_cols, "entity_occurences"
        ).pipe(pudl.helpers.convert_cols_dtypes, "eia")
        record_count_df = pudl.helpers.count_records(
            record_df, base_cols + [attribute_col], "record_occurences"
        ).pipe(pudl.helpers.convert_cols_dtypes, "eia")
        re_count = (
            record_df[base_cols + [attribute_col]]
            .merge(entity_count_df, how="left", on=base_cols)
            .merge(record_count_df, how="left", on=base_cols + [attribute_col])
        )
        # find all of the matching records..
        consistent_records = (
            re_count[re_count["entity_occurences"] == re_count["record_occurences"]]
            .drop(columns=["entity_occurences", "record_occurences"])
            .drop_duplicates()
        )
        return consistent_records


class AddPriorityAttribute(AddAttribute):
    """Add Attributes based on a priority sorting from :py:const:`PRIORITY_ATTRIBUTES`.

    This object associates one attribute from the generators that make up a plant-part
    based on a sorted list within :py:const:`PRIORITY_ATTRIBUTES`. For example, for
    "operational_status" we will grab the highest level of operational status that is
    associated with each records' component generators. The order of operational status
    is defined within the method as: 'existing', 'proposed', then 'retired'. For example
    if a plant_unit is composed of two generators, and one of them is "existing" and
    another is "retired" the entire plant_unit will be considered "existing".
    """

    def execute(self, part_df, gens_mega):
        """Add the attribute to the plant-part df based on priority.

        Args:
            part_df (pandas.DataFrame): dataframe containing records associated
                with one plant part.
            gens_mega (pandas.DataFrame): a table of all of the generators with
                identifying columns and data columns, sliced by ownership which
                makes "total" and "owned" records for each generator owner.
        """
        attribute_col = self.attribute_col
        if attribute_col in part_df.columns:
            logger.debug(f"{attribute_col} already here.. ")
            return part_df

        gens_mega = self.assign_col(gens_mega)
        logger.debug(f"getting max {attribute_col}")
        consistent_records = pudl.helpers.dedupe_on_category(
            gens_mega.copy()[self.base_cols + [attribute_col]],
            self.base_cols,
            attribute_col,
            PRIORITY_ATTRIBUTES_DICT[attribute_col],
        )
        part_df = part_df.merge(consistent_records, on=self.base_cols, how="left")
        return part_df


class AddMaxMinAttribute(AddAttribute):
    """Add Attributes based on the maximum or minimum value of a sorted attribute.

    This object adds an attribute based on the maximum or minimum of another attribute
    within a group of plant parts uniquely identified by their base ID columns.
    """

    def execute(
        self,
        part_df,
        gens_mega,
        att_dtype: str,
        keep: Literal["first", "last"] = "first",
    ):
        """Add the attribute to the plant part df based on sorting of another attribute.

        Args:
            part_df (pandas.DataFrame): dataframe containing records associated
                with one plant part.
            gens_mega (pandas.DataFrame): a table of all of the generators with
                identifying columns and data columns, sliced by ownership which
                makes "total" and "owned" records for each generator owner.
            att_dtype (string): Pandas data type of the new attribute
            keep (string): Whether to keep the first or last record in a sorted
                grouping of attributes. Passing in "first" indicates the new
                attribute is a maximum attribute.
                See :func:`pandas.drop_duplicates`.
        """
        attribute_col = self.attribute_col
        if attribute_col in part_df.columns:
            logger.debug(f"{attribute_col} already here.. ")
            return part_df

        logger.debug(f"pre count of part DataFrame: {len(part_df)}")
        gens_mega = self.assign_col(gens_mega)
        new_attribute_df = (
            gens_mega.astype({attribute_col: att_dtype})[
                self.base_cols + [attribute_col]
            ]
            .sort_values(attribute_col, ascending=False)
            .drop_duplicates(subset=self.base_cols, keep=keep)
            .dropna(subset=self.base_cols)
        )
        part_df = part_df.merge(
            new_attribute_df, how="left", on=self.base_cols, validate="m:1"
        )
        logger.debug(f"post count of part DataFrame: {len(part_df)}")
        return part_df


#################
# Data Validation
#################


def validate_run_aggregations(plant_parts_eia, gens_mega):
    """Run a test of the aggregated columns.

    This test will used the plant_parts_eia, re-run groubys and check similarity.
    """
    for part_name in PLANT_PARTS:
        logger.info(f"Begining tests for {part_name}:")
        test_merge = _test_prep_merge(part_name, plant_parts_eia, gens_mega)
        for test_col in SUM_COLS:
            # Check if test aggregation is the same as generated aggreation
            # Apply a boolean column to the test df.
            test_merge[f"test_{test_col}"] = (
                (test_merge[f"{test_col}_test"] == test_merge[f"{test_col}"])
                | (
                    test_merge[f"{test_col}_test"].isnull()
                    & test_merge[f"{test_col}"].isnull()
                )
                | (test_merge.ownership_record_type == "total")
            )
            result = list(test_merge[f"test_{test_col}"].unique())
            logger.info(f"  Results for {test_col}: {result}")
            if not all(result):
                warnings.warn(f"{test_col} done fucked up.")
                return test_merge
                # raise AssertionError(
                #    f"{test_col}'s '"
                # )


def _test_prep_merge(part_name, plant_parts_eia, gens_mega):
    """Run the test groupby and merge with the aggregations."""
    id_cols = PLANT_PARTS[part_name]["id_cols"]
    plant_cap = (
        gens_mega[gens_mega.ownership_record_type == "owned"]
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
        .groupby(by=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD, observed=True)[SUM_COLS]
        .sum(min_count=1)
        .reset_index()
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
    )
    test_merge = pd.merge(
        plant_parts_eia[plant_parts_eia.plant_part == part_name],
        plant_cap,
        on=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
        how="outer",
        indicator=True,
        suffixes=("", "_test"),
    )
    return test_merge


#########################
# Module Helper Functions
#########################


def make_id_cols_list():
    """Get a list of the id columns (primary keys) for all of the plant parts.

    Returns:
        list: a list of the ID columns for all of the plant-parts, including
        ``report_date``
    """
    return IDX_TO_ADD + pudl.helpers.dedupe_n_flatten_list_of_lists(
        [x["id_cols"] for x in PLANT_PARTS.values()]
    )


def make_parts_to_ids_dict():
    """Make dict w/ plant-part names (keys) to the main id column (values).

    All plant-parts have 1 or 2 ID columns in :py:const:`PLANT_PARTS` plant_id_eia and
    a secondary column (with the exception of the "plant" plant-part). The
    plant_id_eia column is always first, so we're going to grab the last column.

    Returns:
        dictionary: plant-part names (keys) cooresponding to the main ID column
        (value).
    """
    parts_to_ids = {}
    for part, part_dict in PLANT_PARTS.items():
        parts_to_ids[part] = part_dict["id_cols"][-1]
    return parts_to_ids


def add_record_id(part_df, id_cols, plant_part_col="plant_part", year=True):
    """Add a record id to a compiled part df.

    We need a standardized way to refer to these compiled records that contains enough
    information in the id itself that in theory we could deconstruct the id and
    determine which plant id and plant part id columns are associated with this record.
    """
    ids = deepcopy(id_cols)
    # we want the plant id first... mostly just bc it'll be easier to read
    part_df = part_df.assign(record_id_eia_temp=lambda x: x.plant_id_eia.map(str))
    ids.remove("plant_id_eia")
    for col in ids:
        part_df = part_df.assign(
            record_id_eia_temp=lambda x: x.record_id_eia_temp + "_" + x[col].astype(str)
        )
    if year:
        part_df = part_df.assign(
            record_id_eia_temp=lambda x: x.record_id_eia_temp
            + "_"
            + x.report_date.dt.year.astype(str)
        )
    part_df = part_df.assign(
        record_id_eia_temp=lambda x: x.record_id_eia_temp
        + "_"
        + x[plant_part_col]
        + "_"
        + x.ownership_record_type.astype(str)
        + "_"
        + x.utility_id_eia.astype("Int64").astype(str)
    )
    # add operational status only when records are not "operating" (i.e.
    # existing or retiring mid-year see MakeMegaGenTbl.abel_operating_gens()
    # for more details)
    non_op_mask = part_df.operational_status_pudl != "operating"
    part_df.loc[non_op_mask, "record_id_eia_temp"] = (
        part_df.loc[non_op_mask, "record_id_eia_temp"]
        + "_"
        + part_df.loc[non_op_mask, "operational_status_pudl"]
    )
    if year:
        part_df = part_df.rename(columns={"record_id_eia_temp": "record_id_eia"})
    else:
        part_df = part_df.rename(columns={"record_id_eia_temp": "plant_part_id_eia"})
    return part_df


def match_to_single_plant_part(
    multi_gran_df: pd.DataFrame,
    ppl: pd.DataFrame,
    part_name: PLANT_PARTS_LITERAL = "plant_gen",
    cols_to_keep: list[str] = [],
) -> pd.DataFrame:
    """Match data with a variety of granularities to a single plant-part.

    This method merges an input dataframe (``multi_gran_df``) containing
    data that has a heterogeneous set of plant-part granularities with a
    subset of the EIA plant-part list that has a single granularity.
    Currently this is only tested where the single granularity is generators.
    In general this will be a one-to-many merge in which values from single
    records in the input data end up associated with several records from
    the plant part list.
    First, we select a subset of the full EIA plant-part list corresponding
    to the plant part specified by the ``part_name`` argument. In theory
    this could be the plant, generator, fuel type, etc. Currently only
    generators are supported. Then, we iterate over all the possible plant
    parts, selecting the subset of records in ``multi_gran_df`` that have
    that granularity, and merge the homogeneous subset of the plant part list
    that we selected above onto that subset of the input data. Each iteration
    uses a different set of columns to merge on -- the columns which define the
    primary key for the plant part being merged. Each iteration creates a
    separate dataframe, corresponding to a particular plant part, and at
    the end they are all concatenated together and returned.

    Args:
        multi_gran_df: a data table where all records have been linked to
            EIA plant-part list but they may be heterogeneous in its
            plant-part granularities (i.e. some records could be of 'plant'
            plant-part type while others are 'plant_gen' or
            'plant_prime_mover').  All of the plant-part list columns need
            to be present in this table.
        ppl: the EIA plant-part list.
        part_name: name of the single plant part to match to. Must be a key
            in PLANT_PARTS dictionary.
        cols_to_keep: columns from the original data ``multi_gran_df`` that
            you want to show up in the output. These should not be columns
            that show up in the ``ppl``.

    Returns:
        A dataframe in which records correspond to :attr:`part_name` (in
        the current implementation: the records all correspond to EIA
        generators!). This is an intermediate table that cannot be used
        directly for analysis because the data columns from the original
        dataset are duplicated and still need to be scaled up/down.
    """
    # select only the plant-part records that we are trying to scale to
    ppl_part_df = ppl[ppl.plant_part == part_name]
    # convert the date to year start - this is necessary because the
    # depreciation data is often reported as EOY and the ppl is always SOY
    multi_gran_df.loc[:, "report_date"] = pd.to_datetime(
        multi_gran_df.report_date.dt.year, format="%Y"
    )
    out_dfs = []
    for merge_part in PLANT_PARTS:
        pk_cols = PLANT_PARTS[merge_part]["id_cols"] + IDX_TO_ADD + IDX_OWN_TO_ADD
        part_df = pd.merge(
            (
                # select just the records that correspond to merge_part
                multi_gran_df[multi_gran_df.plant_part == merge_part][
                    pk_cols + ["record_id_eia"] + cols_to_keep
                ]
            ),
            ppl_part_df,
            on=pk_cols,
            how="left",
            # this unfortunately needs to be a m:m bc sometimes the df
            # multi_gran_df has multiple record associated with the same
            # record_id_eia but are unique records and are not aggregated
            # in aggregate_duplicate_eia. For instance, the depreciation
            # data has both PUC and FERC studies.
            validate="m:m",
            suffixes=("_og", ""),
        )
        # there should be no records without a matching generator
        assert ~(part_df.record_id_eia.isnull().values.any())
        out_dfs.append(part_df)
    out_df = pd.concat(out_dfs)

    return out_df


def plant_parts_eia_distinct(plant_parts_eia: pd.DataFrame) -> pd.DataFrame:
    """Get the EIA plant_parts with only the unique granularities.

    Read in the pickled dataframe or generate it from the full PPE. Get only
    the records of the PPE that are "true granularities" and those which are not
    duplicates based on their ownership so the FERC to EIA matching model
    doesn't get confused as to which option to pick if there are many records
    with duplicate data.

    Arguments:
        plant_parts_eia: EIA plant parts table.
    """
    plant_parts_eia = plant_parts_eia.assign(
        plant_id_report_year_util_id=lambda x: x.plant_id_report_year
        + "_"
        + x.utility_id_pudl.map(str)
    ).astype({"installation_year": "float"})
    distinct_ppe = plant_parts_eia[
        (plant_parts_eia["true_gran"]) & (~plant_parts_eia["ownership_dupe"])
    ]
    if distinct_ppe.index.name != "record_id_eia":
        logger.error("Plant parts list index is not record_id_eia.")
    return distinct_ppe


def reassign_id_ownership_dupes(plant_parts_eia: pd.DataFrame) -> pd.DataFrame:
    """Reassign the record_id for the records that are labeled ownership_dupe.

    This function is used after the EIA plant-parts table is created.

    Args:
        plant_parts_eia: EIA plant parts table.
    """
    # the record_id_eia's need to be a column to mess with it and record_id_eia
    # is typically the index of plant_parts_df, so we are going to reset index
    # if record_id_eia is the index
    og_index = False
    if plant_parts_eia.index.name == "record_id_eia":
        plant_parts_eia = plant_parts_eia.reset_index()
        og_index = True
    # reassign the record id and ownership col when the record is a dupe
    plant_parts_eia = plant_parts_eia.assign(
        record_id_eia=lambda x: np.where(
            x.ownership_dupe,
            x.record_id_eia.str.replace("owned", "total"),
            x.record_id_eia,
        )
    )
    if "ownership" in plant_parts_eia.columns:
        plant_parts_eia = plant_parts_eia.assign(
            ownership=lambda x: np.where(x.ownership_dupe, "total", x.ownership)
        )
    # then we reset the index so we return the dataframe in the same structure
    # as we got it.
    if og_index:
        plant_parts_eia = plant_parts_eia.set_index("record_id_eia")
    return plant_parts_eia
