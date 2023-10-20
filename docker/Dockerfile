FROM mambaorg/micromamba:1.5.1

USER root

# Install curl and js
# awscli requires unzip, less, groff and mandoc
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y git curl jq unzip less groff mandoc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure gsutil authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Install awscli2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

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

# Create a conda environment based on the specification in the repo
COPY environments/conda-lock.yml environments/conda-lock.yml
RUN micromamba create --prefix ${CONDA_PREFIX} --yes --category dev docs test datasette --file environments/conda-lock.yml && \
    micromamba clean -afy

# Copy the cloned pudl repository into the user's home directory
COPY --chown=${MAMBA_USER}:${MAMBA_USER} . ${CONTAINER_HOME}

# TODO(rousik): The following is a workaround for sudden breakage where conda
# can't find libraries contained within the environment. It's unclear why!
ENV LD_LIBRARY_PATH=${CONDA_PREFIX}/lib
# We need information from .git to get version with setuptools_scm so we mount that
# directory without copying it into the image.
RUN --mount=type=bind,source=.git,target=${PUDL_REPO}/.git \
    ${CONDA_RUN} pip install --no-cache-dir --editable . && \
    # Run the PUDL setup script so we know where to read and write data
    ${CONDA_RUN} pudl_setup

# Run the unit tests:
CMD ["conda", "run", "--no-capture-output", "--prefix", "${CONDA_PREFIX}", "pytest", "test/unit"]
