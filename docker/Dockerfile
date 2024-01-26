FROM mambaorg/micromamba:1.5.6

ENV PGDATA=${CONTAINER_HOME}/pgdata

USER root

SHELL [ "/bin/bash", "-exo", "pipefail", "-c" ]

# Install some linux packages
# awscli requires unzip, less, groff and mandoc
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y git jq unzip less groff mandoc postgresql && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure gsutil authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Add mamba user to postgres group
# hadolint ignore=DL3059
RUN usermod -aG postgres "$MAMBA_USER"

# Create new cluster for Dagster usage that's owned by $MAMBA_USER.
# When the PG major version changes we'll have to update this from 15 to 16
# hadolint ignore=DL3059
RUN pg_createcluster 15 dagster -u "$MAMBA_USER" -- -A trust

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

RUN mkdir -p ${PUDL_INPUT} ${PUDL_OUTPUT} ${DAGSTER_HOME} ${PUDL_REPO}

# Copy dagster configuration file
COPY docker/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

# Copy conda-lock.yml in so we can build the conda environment and cache that layer in
# the Docker image before installing PUDL.
COPY environments/conda-lock.yml ${PUDL_REPO}/environments/conda-lock.yml
# Create a conda environment based on the specification in the repo
RUN micromamba create --prefix ${CONDA_PREFIX} --yes --file ${PUDL_REPO}/environments/conda-lock.yml && \
    micromamba clean -afy

# Copy the rest of the cloned PUDL repo into the image.
# This includes the .git directory, so it is a whole repo
COPY --chown=${MAMBA_USER}:${MAMBA_USER} . ${PUDL_REPO}

# TODO(rousik): The following is a workaround for sudden breakage where conda
# can't find libraries contained within the environment. It's unclear why!
ENV LD_LIBRARY_PATH=${CONDA_PREFIX}/lib
RUN ${CONDA_RUN} pip install --no-cache-dir --no-deps --editable ${PUDL_REPO}

# Install awscli2
# Change back to root because the install script needs access to /usr/local/aws-cli
# curl commands run within conda environment because curl is installed by conda.
USER root
RUN ${CONDA_RUN} bash -c 'curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && ./aws/install'
USER $MAMBA_USER

# Install flyctl
# hadolint ignore=DL3059
RUN ${CONDA_RUN} bash -c 'curl -L https://fly.io/install.sh | sh'
ENV PATH="${CONTAINER_HOME}/.fly/bin:$PATH"

WORKDIR ${PUDL_REPO}
# Run the unit tests:
CMD ["micromamba", "run", "--prefix", "${CONDA_PREFIX}", "--attach", "''", "pytest", "test/unit"]
