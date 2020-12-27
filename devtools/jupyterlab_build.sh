#!/bin/bash

jupyter labextension install --no-build --log-level=INFO \
    @axlair/jupyterlab_vim \
    @jupyterlab/toc \
    @jupyter-widgets/jupyterlab-manager

jupyter labextension update --all

# Rebuild the jupyterlab application with the extensions:
jupyter lab build

# Show us what all we've got installed...
jupyter labextension list
jupyter serverextension list
