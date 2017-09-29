"""Test the output module."""

frc_out = outputs.frc_eia923_df(pudl_engine)
gens_out = outputs.gens_eia860_df(pudl_engine)
gf_out = outputs.gf_eia923_df(pudl_engine)
pu_eia = outputs.plants_utils_eia_df(pudl_engine)
pu_ferc = outputs.plants_utils_ferc_df(pudl_engine)
steam_out = outputs.plants_steam_ferc1_df(pudl_engine)
fuel_out = outputs.fuel_ferc1_df(pudl_engine)
