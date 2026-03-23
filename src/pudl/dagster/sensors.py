"""Dagster sensors for PUDL."""

from pudl.deploy import ferceqr

default_sensors = [ferceqr.ferceqr_sensor]

__all__ = ["default_sensors"]
