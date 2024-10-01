===============================================================================
Development Troubleshooting
===============================================================================

Having trouble working on a ticket or with a new asset? Look below for 
recommendations on how to handle specific issues that might pop up. (Note that
this list is evolving over time.)

-------------------------------------------------------------------------------
"I'm only seeing one year of data from a raw asset"
-------------------------------------------------------------------------------

Let's say you're working on transforming an asset that's currently only in its
raw, extracted form. You've materialized one of the "...__all_dfs" assets in 
Dagster but you're still only seeing one or two years of data.

In this case, you need to materialize the asset group and not just the 
particular "...__all_dfs" asset that you're working with.