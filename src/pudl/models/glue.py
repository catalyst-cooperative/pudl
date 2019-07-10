"""
Database models that pertain to the entire PUDL project.

These tables include many lists of static values, as well as the "glue" that
is required to relate information from different data sources to each other.
"""

from sqlalchemy import Column, Enum, ForeignKey, Integer, String

import pudl.constants
import pudl.models.entities

###########################################################################
# Tables and Enum objects representing static lists. E.g. US states.
###########################################################################

us_states_territories = Enum(*pudl.constants.us_states.keys(),
                             name="us_states_territories")
us_states_lower48 = Enum(*pudl.constants.cems_states.keys(),
                         name="us_states_lower48")
us_states_canada_prov_terr = Enum(
    *pudl.constants.us_states.keys(),
    *pudl.constants.canada_prov_terr.keys(),
    name='us_states_canada_prov_terr'
)


class FERCAccount(pudl.models.entities.PUDLBase):
    """Static list of all the FERC account numbers and descriptions."""

    __tablename__ = 'ferc_accounts'
    __table_args__ = ({'comment': "Account numbers from the FERC Uniform System of Accounts for Electric Plant, which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101)."})
    ferc_account_id = Column(
        String,
        primary_key=True,
        comment="Account number, from FERC's Uniform System of Accounts for Electric Plant. Also includes higher level labeled categories."
    )
    description = Column(
        String,
        nullable=False,
        comment="Long description of the FERC Account."
    )


class FERCDepreciationLine(pudl.models.entities.PUDLBase):
    """Static list of all the FERC account numbers and descriptions."""

    __tablename__ = 'ferc_depreciation_lines'
    __table_args__ = ({"comment": "PUDL assigned FERC Form 1 line identifiers and long descriptions from FERC Form 1 page 219, Accumulated Provision for Depreciation of Electric Utility Plant (Account 108)."})
    line_id = Column(
        String,
        primary_key=True,
        comment="A human readable string uniquely identifying the FERC depreciation account. Used in lieu of the actual line number, as those numbers are not guaranteed to be consistent from year to year."
    )
    description = Column(
        String,
        nullable=False,
        comment="Description of the FERC depreciation account, as listed on FERC Form 1, Page 219."
    )


###########################################################################
# "Glue" tables relating names & IDs from different data sources
###########################################################################

class UtilityFERC1(pudl.models.entities.PUDLBase):
    """
    Mapping between FERC Respondent ID and PUDL Utility ID. This mapping is
    manually generated each year when new FERC Form 1 data is integrated into
    PUDL, if any new FERC respondents have appeared.

    This table is read in from a spreadsheet stored in the PUDL repository:

    results/id_mapping/mapping_eia923_ferc1.xlsx
    """

    __tablename__ = 'utilities_ferc'
    __table_args__ = ({"comment": "This table maps the manually assigned PUDL utility ID to a FERC respondent ID, enabling a connection between the FERC and EIA data sets. It also stores the utility name associated with the FERC respondent ID. Those values originate in the f1_respondent_id table in FERC's FoxPro database, which is stored in a file called F1_1.DBF. This table is generated from a spreadsheet stored in the PUDL repository: results/id_mapping/mapping_eia923_ferc1.xlsx"})
    utility_id_ferc1 = Column(
        Integer,
        primary_key=True,
        comment="FERC assigned respondent_id, identifying the reporting entity. Stable from year to year."
    )
    utility_name_ferc1 = Column(
        String,
        nullable=False,
        comment="Name of the responding utility, as it is reported in FERC Form 1. For human readability only."
    )
    utility_id_pudl = Column(
        Integer,
        ForeignKey('utilities.utility_id_pudl'),
        nullable=False,
        comment="A manually assigned PUDL utility ID. Should probably be constant over time."
    )


class PlantFERC1(pudl.models.entities.PUDLBase):
    """
    A mapping between manually signed PUDL plant IDs, and pairs of utility IDs and plant names. Because FERC does not assign plant IDs, without doing a lot of work the best we can get for a unique identifier is that combination. However, because plant names are not constant from year to year (they are freeform strings which change depending on the whim of the person filling out the form) there may be many combinations of utility ID and plant name representing the same plant in different years. Furthermore, the same plant may be reported by different utilities in the same year due to shared ownership. This table allows all of those different combinations -- which ultimately map to a single collection of infrastructure on the ground -- to a single PUDL plant ID.

    This table is read in from a spreadsheet stored in the PUDL repository:

    results/id_mapping/mapping_eia923_ferc1.xlsx
    """

    __tablename__ = 'plants_ferc'
    __table_args__ = ({"comment": ""})
    utility_id_ferc1 = Column(
        Integer,
        ForeignKey('utilities_ferc.utility_id_ferc1'),
        primary_key=True,
        comment="FERC assigned respondent_id, identifying the reporting entity. Stable from year to year."
    )
    plant_name = Column(
        String,
        primary_key=True,
        nullable=False,
        comment="Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant."
    )
    plant_id_pudl = Column(
        Integer,
        ForeignKey('plants.plant_id_pudl'),
        nullable=False,
        comment="A manually assigned PUDL plant ID. Should probably be constant over time."
    )


class UtilityEIA923(pudl.models.entities.PUDLBase):
    """
    An EIA operator, typically a utility company.

    EIA does assign unique IDs to each operator, as well as supplying a name.
    """

    __tablename__ = 'utilities_eia'
    utility_id_eia = Column(Integer, primary_key=True)
    utility_name = Column(String, nullable=False)
    utility_id_pudl = Column(Integer, ForeignKey(
        'utilities.utility_id_pudl'), nullable=False)


class PlantEIA923(pudl.models.entities.PUDLBase):
    """
    A plant listed in the EIA 923 form.

    A single plant typically has only a single operator.  However, plants may
    have multiple owners, and so the same plant may show up under multiple FERC
    respondents (utilities).
    """

    __tablename__ = 'plants_eia'
    plant_id_eia = Column(Integer, primary_key=True)
    plant_name = Column(String, nullable=False)
    plant_id_pudl = Column(Integer, ForeignKey(
        'plants.plant_id_pudl'), nullable=False)


class Utility(pudl.models.entities.PUDLBase):
    """
    A power plant owning or operating entity, that reports in at least one of the datasets we have integrated.

    This is the home table for PUDL assigned utility IDs. These IDs are
    manually generated each year when new FERC and EIA reporting is integrated,
    and any newly identified utilities are added to the list with a new ID.

    This table is read in from a spreadsheet stored in the PUDL repository:

    results/id_mapping/mapping_eia923_ferc1.xlsx
    """

    __tablename__ = 'utilities'
    __table_args__ = ({"comment": "Home table for PUDL assigned utility IDs. These IDs are manually generated each year when new FERC and EIA reporting is integrated, and any newly found utilities are added to the list with a new ID. Each ID maps to a power plant owning or operating entity which is reported in at least one FERC or EIA data set. This table is read in from a spreadsheet stored in the PUDL repository: results/id_mapping/mapping_eia923_ferc1.xlsx"})
    utility_id_pudl = Column(
        Integer,
        primary_key=True,
        comment="A manually assigned PUDL utility ID. Should probably be constant over time."
    )
    name = Column(
        String,
        nullable=False,
        comment="Utility name, chosen arbitrarily from the several possible utility names available in the utility matching process. Included for human readability only."
    )


class Plant(pudl.models.entities.PUDLBase):
    """
    A co-located collection of electricity generating infrastructure.

    Plants are enumerated based on their appearing in at least one public data
    source, like the FERC Form 1, or EIA Form 923 reporting.  However, they
    may not appear in all data sources.  Additionally, plants may in some
    cases be broken down into smaller units in one data source than another.

    This is the home table for PUDL assigned plant IDs. These IDs are manually
    generated each year when new FERC and EIA reporting is integrated, and any
    newly identified plants are added to the list with a new ID.

    This table is read in from a spreadsheet stored in the PUDL repository:

    results/id_mapping/mapping_eia923_ferc1.xlsx
    """

    __tablename__ = 'plants'
    __table_args__ = ({"comment": "Home table for PUDL assigned plant IDs. These IDs are manually generated each year when new FERC and EIA reporting is integrated, and any newly identified plants are added to the list with a new ID. Each ID maps to a power plant which is reported in at least one FERC or EIA data set. This table is read in from a spreadsheet stored in the PUDL repository: results/id_mapping/mapping_eia923_ferc1.xlsx"})
    plant_id_pudl = Column(
        Integer,
        primary_key=True,
        comment="A manually assigned PUDL plant ID. Should probably be constant over time."
    )
    name = Column(
        String,
        comment="Plant name, chosen arbitrarily from the several possible plant names available in the plant matching process. Included for human readability only."
    )


class UtilityPlantAssn(pudl.models.entities.PUDLBase):
    """Enumerates existence of relationships between plants and utilities."""

    __tablename__ = 'utility_plant_assn'
    utility_id_pudl = Column(Integer, ForeignKey(
        'utilities.utility_id_pudl'), primary_key=True)
    plant_id_pudl = Column(Integer, ForeignKey(
        'plants.plant_id_pudl'), primary_key=True)
