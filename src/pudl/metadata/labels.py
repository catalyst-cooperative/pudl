"""Descriptive labels for coded field values."""
from typing import Dict

ENTITY_TYPES: Dict[str, str] = {
    'M': 'Municipal',
    'C': 'Cooperative',
    'R': 'Retail Power Marketer',
    'I': 'Investor Owned',
    'P': 'Political Subdivision',
    'T': 'Transmission',
    'S': 'State',
    'W': 'Wholesale Power Marketer',
    'F': 'Federal',
    'A': 'Municipal Mktg Authority',
    'G': 'Community Choice Aggregator',
    'D': 'Nonutility DSM Administrator',
    'B': 'Behind the Meter',
    'Q': 'Independent Power Producer',
    'IND': 'Industrial',
    'COM': 'Commercial',
    'PR': 'Private',  # Added by AES for OD table (Arbitrary moniker)
    'PO': 'Power Marketer',  # Added by AES for OD table
    'U': 'Unknown',  # Added by AES for OD table
    'O': 'Other',  # Added by AES for OD table
}
"""
Descriptive labels for EIA entity type codes.
"""

ESTIMATED_OR_ACTUAL: Dict[str, str] = {'E': 'Estimated', 'A': 'Actual'}
"""
Descriptive labels for EIA estimated or actual codes.
"""

MOMENTARY_INTERRUPTIONS: Dict[str, str] = {
    'L': 'Less than 1 minute',
    'F': 'Less than or equal to 5 minutes',
    'O': 'Other',
}
"""
Descriptive labels for EIA momentary interruption codes.
"""
