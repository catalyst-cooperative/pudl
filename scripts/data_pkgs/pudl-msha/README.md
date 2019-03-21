This data package contains a subset of the open data published by the [US
Mining Health and Safety Administration (MSHA)](https://www.msha.gov). It
focuses primarily on MSHA data that is related to the production of coal, and
thus also to the US electricity system.

This data package was created by [Catalyst Cooperative](https://catalyst.coop)
as part of the [Public Utility Data Liberation (PUDL)
project](https://github.com/catalyst-cooperative/pudl). It can be downloaded
from [DataHub](https://datahub.io/zaneselvans/pudl-msha).

## Data
This data package contains a collection of public data compiled and published
by the [US Mining Safety and Health Administration
(MSHA)](https://www.msha.gov/), which is part of the [US Department of
Labor](https://www.dol.gov).

The data is primarily related to US mines, their operators, and their
historical employment and production. It contains a subset of the
information published by MSHA, selected because it is relevant to coal
production and the US electricity system, including:
* the Mines Data Set
* the Mine Controller and Operator History
* the Mine Employment and Production (Quarterly) Data Set

The original data can be downloaded directly from [MSHA's open data
page](https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp).

## Preparation
The data in this package has been minimally altered from the original version
published by MSHA.

The following alterations have been made to the data:
* Tabular files have been re-formatted to use commas to separate values rather
  than the pipe (`|`) character.
* The character encoding of the data files has been converted from `iso-8859-1`
  to `utf-8`.
* Columns stored as strings and using `Y` and `N` to indicate Boolean `True`
  and `False` values have been converted to Boolean types, and now use the
  strings `True` and `False`.
* Where appropriate, the types of some other columns have been changed from
  `string` to `integer` to reflect the nature of the data they contain.

The scripts and other inputs used to prepare the data package can be obtained
from the [PUDL repository on
GitHub](https://github.com/catalyst-cooperative/pudl/).

## License
The data contained in this package is a US Government Work and is not subject
to copyright within the US. The data package was created by [Catalyst
Cooperative](https://catalyst.coop) as part of the [Public Utility Data
Liberation project](https://github.com/catalyst-cooperative/pudl) and is
released under a [CC0-1.0 Public Domain
Dedication](https://creativecommons.org/publicdomain/zero/1.0/). The software
used in the compilation of the data package is released under the [MIT
License](https://opensource.org/licenses/MIT).
