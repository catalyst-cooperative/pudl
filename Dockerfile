FROM continuumio/miniconda3:latest

# TODO: miniconda can install python3.7 inside the envirnoment so there
# may not be need to using python3.7 base python env outside of conda.

WORKDIR /pudl

# Create pudl-dev environment
COPY devtools/environment.yml .
RUN conda env create -f environment.yml

# TODO(rousik): either install --editable pudl from source (/pudl/src) or from PyPI

# TODO(rousik): configure volume under /pudl/workspace, set it up, download raw data
# and set up databases (everything prior to running ETL)

# COPY . /pudl
# RUN conda env create -f devtools/environment.yml

# TODO: Install remaining dependencies inside this env
SHELL ["conda", "run", "-n", "pudl-dev", "/bin/bash", "-c"]

# Copy source code into the container and install into the conda env
WORKDIR /pudl/src
COPY . /pudl/src
RUN pip install --editable /pudl/src

RUN mkdir /pudl/workspace
RUN pudl_setup --clobber /pudl/workspace

# TODO(rousik): turn /pudl/workspace into the volume that can be reused
# TODO(rousik): run pudl_datastore to download zenodo stuffs locally
# Note that we need to mount the volume with the zenodo files but will need
# to make a copy of it to ensure that our results can be extracted to a separate
# volume, or who knows where.

# VOLUME ["/pudl/workspace"]

# RUN pip install --editable ./
# RUN pudl_setup --clobber /pudl/
