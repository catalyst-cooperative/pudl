"""Clean up the data from FERC Form 1.

The data from FERC Form 1 is notoriously bad. The small generators table, for example,
contains rows that represent different types of information: headers, values, and notes.
It's like someone took a paper form, copied it into Microsoft Excel, and called it a
day. Needless to say, it needs a lot of help before it can be used programatically for
bulk analysis.

This module is indented as a hub for some of the more drastic cleaning measures
required to make the data from FERC Form 1 useful.
"""
# import pandas as pd

########################################################################################
# *** S M A L L  G E N E R A T O R S  T A B L E ***
########################################################################################
