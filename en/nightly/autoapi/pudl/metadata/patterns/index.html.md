# pudl.metadata.patterns

Reusable regex pattern constraints for fields in the PUDL metadata.

## Attributes

| [`BORROWER_ID_RUS`](#pudl.metadata.patterns.BORROWER_ID_RUS)                         | Regex pattern for borrower IDs in the RUS dataset, state code followed by a 4-digit number.   |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| [`EXHIBIT21_VERSION_SEC10K`](#pudl.metadata.patterns.EXHIBIT21_VERSION_SEC10K)       | Regex pattern for Exhibit 21 version numbers in SEC 10-K filings.                             |
| [`FISCAL_YEAR_END_MMDD_SEC10K`](#pudl.metadata.patterns.FISCAL_YEAR_END_MMDD_SEC10K) | Regex pattern for fiscal year end dates in MMDD format for SEC 10-K filings.                  |
| [`HTTP_URL`](#pudl.metadata.patterns.HTTP_URL)                                       | Regex pattern for HTTP and HTTPS URLs.                                                        |
| [`INDUSTRY_ID_SIC`](#pudl.metadata.patterns.INDUSTRY_ID_SIC)                         | Regex pattern for 4-digit Standard Industrial Classification (SIC) codes.                     |
| [`INSTALL_DECADE_PHMSAGAS`](#pudl.metadata.patterns.INSTALL_DECADE_PHMSAGAS)         | Regex pattern for installation decades in the PHMSA GAS dataset.                              |
| [`STATE_ID_FIPS`](#pudl.metadata.patterns.STATE_ID_FIPS)                             | Regex pattern for 2-digit FIPS state codes.                                                   |
| [`TAXPAYER_ID`](#pudl.metadata.patterns.TAXPAYER_ID)                                 | Regex pattern for 9-digit company taxpayer ID numbers (TINs).                                 |
| [`YEAR_QUARTER`](#pudl.metadata.patterns.YEAR_QUARTER)                               | Regex pattern for year and quarter in the format YYYYqX.                                      |
| [`ZIP4`](#pudl.metadata.patterns.ZIP4)                                               | Regex pattern for 4-digit ZIP+4 codes.                                                        |
| [`ZIP5`](#pudl.metadata.patterns.ZIP5)                                               | Regex pattern for 5-digit ZIP codes.                                                          |

## Module Contents

### pudl.metadata.patterns.BORROWER_ID_RUS *= '^[A-Z]{2}\\\\d{4}$'*

Regex pattern for borrower IDs in the RUS dataset, state code followed by a 4-digit number.

### pudl.metadata.patterns.EXHIBIT21_VERSION_SEC10K *= '^21\\\\.\*\\\\d\*$'*

Regex pattern for Exhibit 21 version numbers in SEC 10-K filings.

### pudl.metadata.patterns.FISCAL_YEAR_END_MMDD_SEC10K *= '^(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1\\\\d|2\\\\d|3[01])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)$'*

Regex pattern for fiscal year end dates in MMDD format for SEC 10-K filings.

### pudl.metadata.patterns.HTTP_URL *= '^https?://.+'*

Regex pattern for HTTP and HTTPS URLs.

### pudl.metadata.patterns.INDUSTRY_ID_SIC *= '^\\\\d{4}$'*

Regex pattern for 4-digit Standard Industrial Classification (SIC) codes.

### pudl.metadata.patterns.INSTALL_DECADE_PHMSAGAS *= '(\\\\d{4}s|unknown_decade|pre_1940|total_decades)'*

Regex pattern for installation decades in the PHMSA GAS dataset.

### pudl.metadata.patterns.STATE_ID_FIPS *= '^\\\\d{2}$'*

Regex pattern for 2-digit FIPS state codes.

### pudl.metadata.patterns.TAXPAYER_ID *= '\\\\d{2}-\\\\d{7}'*

Regex pattern for 9-digit company taxpayer ID numbers (TINs).

### pudl.metadata.patterns.YEAR_QUARTER *= '\\\\d{4}q[1-4]'*

Regex pattern for year and quarter in the format YYYYqX.

### pudl.metadata.patterns.ZIP4 *= '\\\\d{4}'*

Regex pattern for 4-digit ZIP+4 codes.

### pudl.metadata.patterns.ZIP5 *= '\\\\d{5}'*

Regex pattern for 5-digit ZIP codes.
