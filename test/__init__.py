"""
Testing infrastructure for PUDL.

Right now this "package" really only exists to create a test specific logger.

"""
# Create a parent logger for all test loggers to inherit from
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
