# Ingredients:

These CSV files/spreadsheet tabs were generated from the FERC Form 1 database
tables, selecting (see `id_mapping_inputs_ferc1.ipynb`)

#### FERC respondent IDs (from `f1_respondents`):
 - F1 IDs
 - Name

#### FERC Large Plants (from `f1_steam`):
  - Respondent ID
  - Respondent Name
  - Plant Name

#### FERC Small Plants (from `f1_gnrt_plant`):
  - Respondent_ID
  - respondent_name
  - Plant_name
  - Kind_of_fuel
  - capacity_rating

#### FERC Hydro and Pumped Storage (from `f1_hydro` & `f1_pumped_storage`)
  - Respondent_ID
  - respondent_name
  - Plant_name
  - Plant_kind
  - Tot_capacity

#### EIA 923 (2015) (started with these fields)
  - Plant ID
  - Plant Name
  - Operator Name
  - Operator ID
  - To get a unique set of EIA Operators and EIA plant names from 923, dumped
    these fields into a text file: `eia_plant_ids.txt`

#### EIA Operator IDs
  - Operator
  - Operator Name
  - EIA Plant Names
  - To get a unique list of Plants from EIA:
    `grep -v Plant eia_plant_ids.txt | uniq > unique_plant_ids_filtered.csv`

#### Plant ID
  - Plant Name
  - Operator Name
  - Operator ID
  - Plant State

# Utility Matching
Strategically, there are two sets of things in the world that we are trying to
match: plants, and utilities.  They each give us some information about each
other.  Two utilities that are "the same" utility, should have the same (or at
least similar) lists of plants associated with them.  Two plants that are "the
same" plant, should be associated with the same operators/owners/etc. So when
there's ambiguity in a mapping, you can check against the other type of entity
to see if there's additional information. And if there's still ambiguity after
consulting both of these lists of things, a more general search can be done on
the internet...

Find a utility in f1_respondents tab that you want to match. Select keyword
(utility name or a part of the utility name) from FERC respondent_name and
search EIA Operator Name for potential matches. There are three general
possibilities:

## Zero Utilities Match
- Could be that FERC utility doesn't have any plants, doesn't generate
  any electricity, and so isn't reporting to EIA. Actually, this is most
  FERC plants. OKAY.
- Could be that this utility *had* plants, but they're all dead now. OKAY
- Could be that the utility reports to FERC and EIA using very different
  names. NOT OKAY
- In all cases, try to verify the lack of matches:
  - Verify that this FERC utility actually has plants associated with it
    in FERC.
     - If it *doesn't* have plants, make a note of it and compare the list of
     utilities currently in the database with the ones being added. If the
     utility is not currently in the database, add the utility's
     respondent ID and utility name to the utilities_output tab of the
     mapping_eia923_ferc1 sheet, and you're good!
       - POSSIBLE FAIL: EIA has many more plants than FERC listed. Could
          be that utility names are different enough between FERC & EIA
          that it doesn't come up in simple keyword search, and because
          we don't have a set of FERC plants to work from, we'll never
          link these two utilities with each other, or the EIA plants with
          the FERC utility... No clear way to fix this.
     - If it *does* have plants:
        - Try and look up some of the plants associated with the FERC
          respondent in the EIA plants, to see what utility they are
          associated with. Here, the ONLY piece of information we have to
          work with is the plant name, which is in no way kept unique.
        - If a given plant name exists with different associated
          utilities in FERC & EIA data, ask the internet whether those
          two utilities are actually the same company.
        - If obviously the same (e.g. subsidiary/holding company) then
          map the utilities to each other. We could get this mapping
          "wrong" because utility relationships are complicated.
        - If obviously not (e.g. they're in different states) then note
          no EIA operator for the f1_respondent you're trying to map.

## Exactly One Utility Matches
- Again, check the list of plants associated with your f1_respondent and
  the candidate EIA operator that matches.  Are they the same?
  - FERC & EIA plant lists are exactly the same! Amazing!
    - Map the utilities to each other.
    - Map the plants (see below)
  - EIA plant list is a superset of FERC plant list.
    - EIA just has more plants...
    - Check other kinds of FERC plants (small, hydro, etc.)
    - Search for individual EIA plants in the FERC plant list to see if
      there are other names for the utility that owns those plants
      hiding out in the FERC data.
      - If so, then maybe there's a 2-to-1 (or N-to-1) mapping of FERC
        respondents to an EIA operator (this does happen)
      - Most EIA & FERC plants match, but a few EIA plants seem to be
        associated with some other utility... the utilities probably match.
      - Go into the plant mapping process on these plants, and keep an
        eye out for additional utility names associated with this respondent
        (might be an unusually named subsidiary, result of a merger, etc.)
      - If you find one, you'll need to add another mapping between the
        FERC respondent and those EIA operators.

## Multiple Utilities Match
- For example... AEP -- it's a huge conglomerate utility holding mess...
  and the boundaries of a "utility" are actually less well defined than
  the boundaries of a "plant" and we're not trying to address this
  philosophical question. For our purposes we're mostly treating
  entities that have the same or similar collections of plants (assets)
  as the same utility.
- Two major outcomes:
  - Either the multiple EIA IDs actually correspond to the same FERC
    respondent, in which case they both get mapped (see processes in
    the exactly one match case above).
  - Or, they're actually different, and you figure out which one to map
    to the FERC respondent, based on their plants (again, see exactly
    one match process, above).

## Ingesting Utilities Without Plants

- When ingesting a new year's worth of FERC Form 1 data,

# Plant Mapping
Most plant mapping will have taken place during the utility mapping processes
above, because comparing the lists of plants associated with given FERC and EIA
IDs is such an integral part of the utility mapping. This is the process of
actually mapping them.

- Filter for a given FERC respondent_name in the f1_plants tab
- Filter for matching/similar EIA Operator Name in EIA plants tab
- If there’s a one-to-one mapping between EIA Plant IDs and the FERC Plant Names
  - Add “EIA Plant ID,” “EIA Plant Name,” “EIA Operator ID,” and “EIA Operator
    Name” to the f1_plants table.
- If there’s any FERC plant that does not show up in the list of EIA plants for
  the EIA Operator matching the FERC respondent you're searching for...
  - then go look for the plant itself in the full list of eia_plant_names by
  filtering them for f1_plant_name
  - If there is no match, note “No EIA ID”
  - If there *is* a match, then map the plants to each other, and note that
    that the f1_respondent and eia_operator differ.
- If there’s more than one EIA plant IDs associated with a single FERC plant
  name (this happens sometimes when EIA reports individual units as "plants"):
  - Duplicate the FERC plant entry, and associate both (or all) of the EIA
    plant information with each instance of the FERC plant.
- Similarly, if there’s more than one (FERC plant name + respondent_ID)
  associated with a given EIA plant id:
  - Add identical EIA info associated with each of the FERC plants.
  - This often happens when the same plant is reported by multiple FERC
    respondents.
  - May also happen when FERC is reporting "units" as "plants" for some reason.
  - Note “n f1_plant_name’s” (where N is the number of FERC plants associated
    with a given EIA Plant ID)

# Outcomes:
  - Each PUDL utility_id will be associated with at between zero and many
    FERC respondents and EIA operators.
  - Each PUDL plant_id will have between zero and many FERC and EIA plants
    associated with it.
  - Really what we've generated is a list of associations, not direct
    correspondences.  We have a database that can give us an educated guess
    about which plants are related to each other, which utilities are related
    to each other, which plants are associated with what utilities.
  - Building real "Utility" and "Plant" objects that have well defined physical
    attributes will require curating these associations on a plant by plant
    and utility by utility basis, except in the simplest cases (which are
    thankfully fairly common).

# Potential Error Checking Strategies:

  - Compare the locations (lat/lon) of various plants. For PUDL plants that are
    less than some small distances from each other (e.g. 1 mile) check to see
    if they are associated with each other in the database.
  - All of the above processes started from FERC respondents and went to EIA
    operators, because there were many fewer FERC respondents and plants.
    However, there are cases in which there may be EIA operators and plants that
    ought to be associated with FERC respondents and plants, which may not have
    been found because there are some kinds of utilities that don't report to
    FERC, but do report to EIA (e.g. munis, co-ops, like TriState)
    - Can we enumerate a case in which we would fail to associate an EIA plant
      with a FERC plant/utility, when that sould have happened?
    - There are almost certainly some EIA plants, especially plants with
      multiple owners, that are not mapped to the other operators or
      respondents.
    - i.e. If a plant has multiple fields in FERC or EIA associated with the
      larger utilities, then the smaller operators may have been lost because
      the plants were sorted based on FERC respondent_ids.

- As of 1/30/2017, all of the utility IDs were assigned without mapping holding
  companies or multiple names for the same company.
