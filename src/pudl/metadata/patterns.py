"""Reusable regex pattern constraints for fields in the PUDL metadata."""

BORROWER_ID_RUS = r"^[A-Z]{2}\d{4}$"
"""Regex pattern for borrower IDs in the RUS dataset, state code followed by a 4-digit number."""

EXHIBIT21_VERSION_SEC10K = r"^21\.*\d*$"
"""Regex pattern for Exhibit 21 version numbers in SEC 10-K filings."""

FISCAL_YEAR_END_MMDD_SEC10K = r"^(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1\d|2\d|3[01])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)$"
"""Regex pattern for fiscal year end dates in MMDD format for SEC 10-K filings."""

HTTP_URL = r"^https?://.+"
"""Regex pattern for HTTP and HTTPS URLs."""

INDUSTRY_ID_SIC = r"^\d{4}$"
"""Regex pattern for 4-digit Standard Industrial Classification (SIC) codes."""

INSTALL_DECADE_PHMSAGAS = r"(\d{4}s|unknown_decade|pre_1940|total_decades)"
"""Regex pattern for installation decades in the PHMSA GAS dataset."""

STATE_ID_FIPS = r"^\d{2}$"
"""Regex pattern for 2-digit FIPS state codes."""

TAXPAYER_ID = r"\d{2}-\d{7}"
"""Regex pattern for 9-digit company taxpayer ID numbers (TINs)."""

YEAR_QUARTER = r"\d{4}q[1-4]"
"""Regex pattern for year and quarter in the format YYYYqX."""

ZIP4 = r"\d{4}"
"""Regex pattern for 4-digit ZIP+4 codes."""

ZIP5 = r"\d{5}"
"""Regex pattern for 5-digit ZIP codes."""
