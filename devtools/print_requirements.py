#!/usr/bin/python3
"""Print out install requirements from setup.py for use with pip install."""
import distutils.core

setup = distutils.core.run_setup("setup.py")
for dep in setup.install_requires:
    print(dep)
