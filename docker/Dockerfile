FROM ghcr.io/prefix-dev/pixi:0.62.2-noble

# The pixi base image uses 'ubuntu' as the non-root user (UID 1000)
ENV PIXI_USER=ubuntu
ENV CONTAINER_HOME=/home/$PIXI_USER
ENV PGDATA=${CONTAINER_HOME}/pgdata

USER root

SHELL [ "/bin/bash", "-exo", "pipefail", "-c" ]

# Install some linux packages
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y git postgresql && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure gcloud authentication
# hadolint ignore=DL3059
RUN printf '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# Add pixi user to postgres group
# hadolint ignore=DL3059
RUN usermod -aG postgres "$PIXI_USER"

# We use an environment variable to set the Postgres version because it is also used in
# the nightly build script and this makes it easier to ensure they are all the same.
# Remember to bump the Postgres version. Postgres 17 was released in September, 2024.
ENV PG_VERSION=16
# Create new cluster for Dagster usage that's owned by $PIXI_USER.
# hadolint ignore=DL3059
RUN pg_createcluster ${PG_VERSION} dagster -u "$PIXI_USER" -- -A trust

# Switch back to being non-root user and get into the home directory
USER $PIXI_USER
WORKDIR ${CONTAINER_HOME}

ENV PUDL_REPO=${CONTAINER_HOME}/pudl

ENV CONTAINER_PUDL_WORKSPACE=${CONTAINER_HOME}/pudl_work
ENV PUDL_INPUT=${CONTAINER_PUDL_WORKSPACE}/input
ENV PUDL_OUTPUT=${CONTAINER_PUDL_WORKSPACE}/output
ENV DAGSTER_HOME=${CONTAINER_PUDL_WORKSPACE}/dagster_home

RUN mkdir -p ${PUDL_INPUT} ${PUDL_OUTPUT} ${DAGSTER_HOME} ${PUDL_REPO}

# Copy dagster configuration file
COPY docker/dagster.yaml ${DAGSTER_HOME}/dagster.yaml

# Copy the entire PUDL repo into the image.
# We can't install the PUDL package without .git/ because the version is dynamically
# determined using hatch-vcs, which needs access to the git tags.
COPY --chown=${PIXI_USER}:${PIXI_USER} . ${PUDL_REPO}
WORKDIR ${PUDL_REPO}

# Install the pixi environment (includes the editable local package)
RUN pixi install --locked

# Run the unit tests:
CMD ["pixi", "run", "-v", "pytest-unit"]
