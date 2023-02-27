FROM condaforge/mambaforge:4.12.0-0

# Install curl and js
# awscli requires unzip, less, groff and mandoc
# hadolint ignore=DL3008
RUN apt-get update && apt-get install --no-install-recommends -y curl jq unzip less groff mandoc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure gsutil authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Install awscli2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && ./aws/install

# Create a non-root user inside the container
# hadolint ignore=DL3059
RUN useradd -Ums /bin/bash catalyst

ENV CONTAINER_HOME=/home/catalyst

# Copy the cloned pudl repository into the user's home directory
COPY --chown=catalyst:catalyst . ${CONTAINER_HOME}

# Switch to being the catalyst user and go into the copied repo
USER catalyst
WORKDIR ${CONTAINER_HOME}

ENV CONDA_PREFIX=${CONTAINER_HOME}/env
ENV PUDL_REPO=${CONTAINER_HOME}/pudl
ENV CONDA_RUN="conda run --no-capture-output --prefix ${CONDA_PREFIX}"
ENV PYTHON_VERSION="3.10"
ENV CONTAINER_PUDL_IN=${CONTAINER_HOME}/pudl_in
ENV CONTAINER_PUDL_OUT=${CONTAINER_HOME}/pudl_out
ENV DAGSTER_HOME=${CONTAINER_HOME}/DAGSTER_HOME

# Create a conda environment based on the specification in the repo
# We need information from .git to get version with setuptools_scm so we mount that
# directory without copying it into the image.
RUN --mount=type=bind,source=.git,target=${PUDL_REPO}/.git \
    # mamba create can't read environment.yml, and mamba env isn't installed yet,
    # so we create a bare py3.10 env first:
    mamba create --copy --prefix ${CONDA_PREFIX} --yes python=${PYTHON_VERSION} && \
    # Then we can use mamba env update, which can parse the environment.yml file:
    mamba env update --prefix ${CONDA_PREFIX} --file test/test-environment.yml && \
    conda clean -afy && \
    ${CONDA_RUN} pip install --no-cache-dir -e './[dev,doc,test,datasette]' && \
    # Create data input/output directories
    mkdir ${CONTAINER_PUDL_IN} && mkdir ${CONTAINER_PUDL_OUT} && \
    # Run the PUDL setup script so we know where to read and write data
    ${CONDA_RUN} pudl_setup \
    --pudl_in ${CONTAINER_PUDL_IN} \
    --pudl_out ${CONTAINER_PUDL_OUT} && \
    mkdir -p DAGSTER_HOME

# Run the unit tests:
CMD ["conda", "run", "--no-capture-output", "--prefix", "${CONDA_PREFIX}", "pytest", "test/unit"]
