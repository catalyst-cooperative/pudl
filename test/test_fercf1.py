# This is just a placeholder for the moment... we will need to create
# unit tests as we go along.

from .context import pudl
import pytest

def test_f1_slurp():
    """
    Create a fresh FERC Form 1 DB in postgres, and attempt to access it.
    """
    from sqlalchemy import create_engine
    import pandas as pd
    pudl.fercf1.f1_slurp()
    f1_engine = create_engine('postgresql://catalyst@localhost:5432/ferc_f1')
    with f1_engine.connect() as con:
        df = pd.read_sql('SELECT respondent_id, respondent_name FROM f1_respondent_id', con)
