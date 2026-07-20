#!/usr/bin/python3
"""Print out install requirements from setup.py for use with pip install."""

from setuptools._distutils.core import run_setup

setup = run_setup("setup.py")
for dep in setup.install_requires:
    print(dep)
