# First, build a base image with miniconda, pudl-dev environment
# and all dependencies installed.
FROM continuumio/miniconda3:latest AS pudl-dev-base
WORKDIR /pudl

COPY devtools/environment.yml .
RUN conda env create -f environment.yml

RUN echo "source activate pudl-dev" > ~/.bashrc
# ENV PATH /opt/conda/envs/env/bin:$PATH
SHELL ["conda", "run", "-n", "pudl-dev", "/bin/bash", "-c"]

# Next, install pudl source code, install it into conda env and
# prepare workspace.
FROM pudl-dev-base AS pudl-dev
# TODO(rousik): ideally, we would git clone --depth=1 to drop the unnecessary git history.
# Perhaps make a temp copy of /pudl/src, clone that and drop the old one?
WORKDIR /pudl/src
COPY . /pudl/src
RUN pip install --editable /pudl/src

# RUN mkdir /pudl/workspace
RUN mkdir /pudl/inputs
RUN mkdir /pudl/outputs
RUN pudl_setup --pudl_in /pudl/inputs --pudl_out /pudl/outputs

# Mark /pudl/inputs, /pudl/outputs and /pudl/src as mountable volumes
VOLUME /pudl/src
VOLUME /pudl/inputs
VOLUME /pudl/outputs
