===============================================================================
PUDL ID Mapping
===============================================================================

The Status of FERC 1 and EIA IDs
--------------------------------

Many of the same utilities are reporting data to FERC and EIA, but there is no official
crosswalk file or universal ID to connect the records and obtain information from both
sources in one table. The EIA assigns their own IDs to distinguish the utilities and
plants that report to them over time. These IDs are useful for parsing EIA data, but
they are not connected to utility and plant data reported by FERC. FERC Form 1 uses a
respondent ID to keep track of utilities, but it does not have an ID system in place to
keep track of unique plants or records over time.

Why is this significant? Without plant IDs, you can't track plant characteristics over
time. Without shared plant IDs, you can't develop a holistic understanding of individual
plants based on the data they report to different entities. Linking the financial
information from FERC 1 with the operational data from EIA923 and EIA860 for instance
helps us cultivate a more complete picture of the marginal cost of electricity.

PUDL IDs
--------

To make the data from FERC 1 and EIA more usable and interchangeable, we have developed
universal ``plant_id_pudl`` and ``utility_id_pudl`` values that are the same across
datasets. The IDs are assigned via a manual mapping process that is codified in a
spreadsheet located at ``pudl/package_data/glue/pudl_id_mapping.xlsx``. This spreadsheet
contains all unique plant and utility names reported to FERC and EIA from the oldest to
the newest year of data we've collected for each data source.

In addition to mapping universal IDs onto plant and utility records, this spreadsheet
assigns universal plant and utility names to each record.

The spreadsheet has two tabs, one for mapping utilities and one for mapping plants. We
assign plant and utility PUDL IDs to each record first by giving each record a unique ID
and then by identifying and fixing records that should share an ID. See below for more
detailed instructions.

Plants, as defined by this mapping process, are considered co-located generation assets.
Records that have the same ``plant_id_eia`` should also have the same ``plant_id_pudl``.

.. warning::
    PUDL IDs should never be hard-coded into any analysis or transformation functions as
    they may come to represent different plants or utilities as new records are added
    and mapped.

PUDL IDs are not static. They identify unique plants/utilities in a given iteration of
the data, but they may not remain exactly the same over time. The spreadsheet assigns
PUDL IDs based on a record's position in the spreadsheet, so if you change the PUDL ID
of a record in the middle, it will change the PUDL ID of all those below it (unless they
are referencing a PUDL ID that already exists)

.. warning::
    No more than one person should ever update the PUDL ID mapping spreadsheet at a
    time. Two people updating PUDL IDs simultaneously will lead to discrepancies in the
    PUDL ID values.

The :mod:`pudl.glue.ferc1_eia` module is where most of the spreadsheet coordination
happens after the records are manually mapped. It also contains the functions that
determine whether plants/utilities have or haven't been mapped.


Checking for Unmapped Records
-----------------------------

With every new year of data comes the possibility of new plants and utilities. Once
you’ve integrated the new data into PUDL :doc:`(see these instructions)
<annual_updates>`, you’ll need to check for unmapped utility and plants. To do this,
run the ``find_unmapped_plants_utils.py`` script. You can add the ``--help`` flag for
more information. From the top level directory in the PUDL repository:

.. code-block:: console

    $ ./devtools/ferc1-eia-glue/find_unmapped_plants_utils.py

This script identifies plants and utilities which exist in the updated FERC 1 and EIA
datasets that do not yet appear in ``pudl_id_mapping.xlsx``. The script will output four
CSVs in the ``devtools/ferc1-eia-glue`` directory that correspond to unmapped plants and
utilities from FERC 1 and EIA.


Assigning PUDL IDs to Unmapped Records
--------------------------------------

Here comes the manually intensive part of the process! Now we must ensure that 1) every
record gets assigned a PUDL ID and 2) that records pertaining to the same plant have the
same PUDL ID.

.. warning::
    The ordering of the rows in the mapping spreadsheet is important. **YOU MUST NOT
    SORT THE PUDL ID MAPPING SPREADSHEET**, as it will change the values of many
    assigned IDs. If you need to view only a subset of the data in the sheet for ease of
    mapping you can filter it.

Mapping Plants
^^^^^^^^^^^^^^

The ``unmapped_plants_ferc1/eia.csv`` files should display basic plant information such
as the facility name, utility name, and capacity. We show capacity here so that we can
prioritize which plants to map. The larger the capacity, the more important it is to get
it mapped. Sort the records by capacity so the highest priority records at the top.

From the FERC and EIA unmapped plants spreadsheets, copy the ``plant_id_eia`` (only in
EIA), ``plant_name_ferc1/eia``, ``utility_id_ferc1/eia``, and ``utility_name_ferc1/eia``
columns and paste them at the bottom of the corresponding columns in the plants tab of
the ``pudl_id_mapping.xlsx`` spreadsheet. Next drag the auto-incrementing formula in the
``plant_id_pudl`` column and the naming formula in the ``plant_name_pudl`` column so
that all new records are automatically assigned PUDL plant names and unique PUDL IDs.
You should also drag the ``find_plant_id_eia_matches`` formula down, which we'll use in
the next step.

In previous iterations of the spreadsheet, matching FERC and EIA records were placed in
the same row with the FERC version in the FERC columns and the EIA version in the EIA
columns. This is not necessary. As long as matching FERC and EIA records (and same-plant
records within a data source) have the same PUDL ID in the ``plant_id_pudl`` column,
you’re good to go!

Linking FERC1-EIA Records
#########################

Now that all of the unmapped plants have been added to the spreadsheet and given an ID,
we need to check whether they should actually be linked to, and share PUDL IDS with,
another record. Because utilities may spell plant names differently year to year (EX:
``La Cygne`` and ``lacygne``) or report subcomponents of a single plant (EX: ``Hancock``
and ``Hancock Peaker``), it is not uncommon for multiple records to share a PUDL ID. As
mentioned above, plants with the same EIA ID should also have the same PUDL ID. The cell
formula that assigns PUDL IDs does not account for this, but there is a column,
``find_plant_id_eia_matches``, in the ``pudl_id_mapping`` spreadsheet that will look for
past instances of the same ``plant_id_eia``. If you haven't already, drag this formula
down so that it checks all the new records. If it finds a match, update the newer record
to have the same PUDL ID.

.. note::
    To save time, we’re only linking plants with a capacity of 5 MW or higher. Because
    you sorted the records by capacity, this should be easy. Just look at the unmapped
    plants csv for the first plant under 5 MW and everything below that can remain
    unlinked.

For each new record, search the entire plants_combined tab for a piece of the
plant name string (e.g. for ``chenango solar``, you could search for ``chen``,
or ``chenan``). Searching the entire plant tab helps find other records within
both FERC and EIA that may be the same or part of the same facility. Searching
for a piece can help catch misspellings in the plant name, which are more common
in the FERC records.

    * **If a record has the same plant and utility name as another record:**
        assign it the same PUDL ID as the other record **by reference** to the cell in
        which the first instance of that PUDL ID appears. **Never simply enter the PUDL
        ID as a number**, as it will not update automatically when IDs change due to
        re-mapping or other alterations. If the new plant name is similar in that it’s a
        different unit or a part of a facility that uses a different fuel type (e.g.
        ``Conemaugh (Steam)`` and ``Conemaugh (CT)``, they should still share the same
        PUDL ID. That’s because co-located fossil-fueled generators are considered parts
        of the same plant.

    * **If the plant name looks similar but there are discrepancies:**
        such as different operators (e.g. a facility ``keystone`` with operators
        ``baltimore gas and electric`` and ``atlantic gas and electric``), then it’s
        best to look at the capacity first to see if the facilities are the same. If
        that’s indeterminate, you can Google the plant to see if it has the same
        location or if there is ownership or construction history that helps determine
        if the facilities are the same or co-located.

    * **If co-located EIA plants have distinct plant IDs and no FERC 1 plant:**
        they should not be lumped under a single PUDL Plant ID, as that artificially
        reduces the granularity of data without providing any additional linkage to
        other datasets.

Mapping Utilities
^^^^^^^^^^^^^^^^^

Both FERC and EIA have utility IDs, so we’re fairly confident that they don’t require
intra-dataset mapping. For this reason, we only focus on connecting utilities between
datasets.

Linking FERC1-EIA Records
#########################

Copy the information output to the ``unmapped_utils_eia/ferc1.csv`` files and paste it
in the appropriate columns at the bottom of the ``pudl_id_mapping.xlsx``  sheet. Note
that FERC 1 utility information goes in the left-hand columns and EIA utility
information goes in the right-hand columns.

Next, you'll have to manually assign ``utility_id_pudl`` values to each row. There is no
formula you can drag down, so just find the largest ``utility_id_pudl`` and create new
values incrementing from there. To double check whether a utility has already appeared,
drag down the formulas in the ``check_utility_id_ferc1`` and ``check_utility_id_eia``
columns. If there's a match, the correct ``utility_id_pudl`` will show up in the column,
and you can create a reference to the original ``utility_id_pudl`` assignment above.

Make sure to save the file when you're done!


Testing Newly Mapped Records
----------------------------

Before you integrate these newly mapped records into the PUDL database, you'll want to
run some basic tests in the command line to make sure you've covered all of the unmapped
entities. This command assumes that you have all of the new EIA data loaded into your
live PUDL DB, and all of the new FERC 1 data loaded into your cloned FERC 1 DB:

.. code-block:: console

    $ pytest --live-dbs test/integration/glue_test.py

Integrating Newly Mapped Records into PUDL
------------------------------------------

Once you’ve successfully mapped all unmapped PUDL IDs, you’ll want to rerun the ETL!
This ensures that the newly mapped IDs get integrated into the PUDL database and output
tables that folks are using. Make sure to tell everyone else to do so as well so that
you can all use the newly mapped PUDL IDs.
