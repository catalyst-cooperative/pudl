FROM ghcr.io/prefix-dev/pixi:0.69.0-noble

# The pixi base image uses 'ubuntu' as the non-root user (UID 1000)
ENV PIXI_USER=ubuntu
ENV CONTAINER_HOME=/home/$PIXI_USER

USER root

SHELL [ "/bin/bash", "-exo", "pipefail", "-c" ]

# Install some linux packages
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y git inotify-tools ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure gcloud authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Switch back to being non-root user and get into the home directory
USER $PIXI_USER
WORKDIR ${CONTAINER_HOME}

ENV PUDL_ROOT_PATH=${CONTAINER_HOME}/pudl

ENV CONTAINER_PUDL_WORKSPACE=${CONTAINER_HOME}/pudl_work
ENV PUDL_INPUT=${CONTAINER_PUDL_WORKSPACE}/input
ENV PUDL_OUTPUT=${CONTAINER_PUDL_WORKSPACE}/output
ENV DAGSTER_HOME=${CONTAINER_PUDL_WORKSPACE}/dagster_home

RUN mkdir -p ${PUDL_INPUT} ${PUDL_OUTPUT} ${DAGSTER_HOME} ${PUDL_ROOT_PATH}

# Copy job-specific dagster configuration files; each batch script selects
# the appropriate one at startup by copying it to $DAGSTER_HOME/dagster.yaml.
COPY builds/dagster-pudl.yaml ${DAGSTER_HOME}/dagster-pudl.yaml
COPY builds/dagster-ferceqr.yaml ${DAGSTER_HOME}/dagster-ferceqr.yaml

# Copy the entire PUDL repo into the image.
# We can't install the PUDL package without .git/ because the version is dynamically
# determined using hatch-vcs, which needs access to the git tags.
COPY --chown=${PIXI_USER}:${PIXI_USER} . ${PUDL_ROOT_PATH}
WORKDIR ${PUDL_ROOT_PATH}

# Install the pixi environment (includes the editable local package)
RUN pixi install --locked

# Run the unit tests:
CMD ["pixi", "run", "-v", "pytest-unit"]
