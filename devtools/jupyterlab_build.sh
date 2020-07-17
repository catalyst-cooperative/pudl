#!/bin/bash

jupyter labextension install --no-build --log-level=INFO \
    @axlair/jupyterlab_vim \
    @jupyterlab/toc \
    @jupyterlab/debugger \
    jupyterlab-flake8 \
    @jupyter-widgets/jupyterlab-manager \
    dask-labextension

jupyter serverextension enable dask_labextension

# Rebuild the jupyterlab application with the extensions:
jupyter lab build

# Show us what all we've got installed...
jupyter labextension list
jupyter serverextension list
