import numpy as np
import pandas as pd

eia923_dirname = "data/eia/form923"
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
def get_eia923(years=[2015,]):
    """Retrieve data from EIA Form 923 for analysis.

    For now this pulls from the published Excel spreadsheets. The same data
    may also be available in a more machine readable form via the EIA's bulk
    JSON download facility, but those files will require parsing.

    """

