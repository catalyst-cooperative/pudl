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

# Switch to being the catalyst user and go into the copied repo
USER catalyst
WORKDIR ${CONTAINER_HOME}

ENV CONDA_PREFIX=${CONTAINER_HOME}/env
ENV PUDL_REPO=${CONTAINER_HOME}/pudl
ENV CONDA_RUN="conda run --no-capture-output --prefix ${CONDA_PREFIX}"
ENV PYTHON_VERSION="3.10"

ENV CONTAINER_PUDL_WORKSPACE=${CONTAINER_HOME}/pudl_work
ENV PUDL_INPUT=${CONTAINER_PUDL_WORKSPACE}/data
ENV PUDL_OUTPUT=${CONTAINER_PUDL_WORKSPACE}/output
ENV DAGSTER_HOME=${CONTAINER_PUDL_WORKSPACE}/dagster_home

# Create data input/output directories
RUN mkdir -p ${PUDL_INPUT} ${PUDL_OUTPUT} ${DAGSTER_HOME}

# Create a conda environment based on the specification in the repo
COPY test/test-environment.yml test/test-environment.yml
RUN mamba create --copy --prefix ${CONDA_PREFIX} --yes python=${PYTHON_VERSION} && \
    # Then we can use mamba env update, which can parse the environment.yml file:
    mamba env update --prefix ${CONDA_PREFIX} --file test/test-environment.yml && \
    conda clean -afy


# Copy the cloned pudl repository into the user's home directory
COPY --chown=catalyst:catalyst . ${CONTAINER_HOME}

# We need information from .git to get version with setuptools_scm so we mount that
# directory without copying it into the image.
RUN --mount=type=bind,source=.git,target=${PUDL_REPO}/.git \
    ${CONDA_RUN} pip install --no-cache-dir -e './[dev,doc,test,datasette]' && \
    # Run the PUDL setup script so we know where to read and write data
    ${CONDA_RUN} pudl_setup


# Run the unit tests:
CMD ["conda", "run", "--no-capture-output", "--prefix", "${CONDA_PREFIX}", "pytest", "test/unit"]
