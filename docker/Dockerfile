FROM mambaorg/micromamba:1.5.3

USER root

SHELL [ "/bin/bash", "-exo", "pipefail", "-c" ]

# Install some linux packages
# awscli requires unzip, less, groff and mandoc
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y git jq unzip less groff mandoc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure gsutil authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Switch back to being non-root user and get into the home directory
USER $MAMBA_USER
ENV CONTAINER_HOME=/home/$MAMBA_USER
WORKDIR ${CONTAINER_HOME}

ENV CONDA_PREFIX=${CONTAINER_HOME}/env
ENV PUDL_REPO=${CONTAINER_HOME}/pudl
ENV CONDA_RUN="micromamba run --prefix ${CONDA_PREFIX}"

ENV CONTAINER_PUDL_WORKSPACE=${CONTAINER_HOME}/pudl_work
ENV PUDL_INPUT=${CONTAINER_PUDL_WORKSPACE}/input
ENV PUDL_OUTPUT=${CONTAINER_PUDL_WORKSPACE}/output
ENV DAGSTER_HOME=${CONTAINER_PUDL_WORKSPACE}/dagster_home

RUN mkdir -p ${PUDL_INPUT} ${PUDL_OUTPUT} ${DAGSTER_HOME}

# Copy dagster configuration file
COPY docker/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

# Create a conda environment based on the specification in the repo
COPY environments/conda-lock.yml environments/conda-lock.yml
RUN micromamba create --prefix ${CONDA_PREFIX} --yes --file environments/conda-lock.yml && \
    micromamba clean -afy
# Copy the cloned pudl repository into the user's home directory
COPY --chown=${MAMBA_USER}:${MAMBA_USER} . ${CONTAINER_HOME}

# TODO(rousik): The following is a workaround for sudden breakage where conda
# can't find libraries contained within the environment. It's unclear why!
ENV LD_LIBRARY_PATH=${CONDA_PREFIX}/lib
# We need information from .git to get version with setuptools_scm so we mount that
# directory without copying it into the image.
RUN --mount=type=bind,source=.git,target=${PUDL_REPO}/.git \
    ${CONDA_RUN} pip install --no-cache-dir --no-deps --editable . && \
    # Run the PUDL setup script so we know where to read and write data
    ${CONDA_RUN} pudl_setup

USER root

# Install awscli2
RUN ${CONDA_RUN} curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

# Install flyctl
RUN ${CONDA_RUN} curl -L https://fly.io/install.sh -o "install_fly.sh"
RUN ${CONDA_RUN} sh install_fly.sh
ENV PATH="${CONTAINER_HOME}/.fly/bin:$PATH"

USER $MAMBA_USER

# Run the unit tests:
CMD ["micromamba", "run", "--prefix", "${CONDA_PREFIX}", "--attach", "''", "pytest", "test/unit"]
