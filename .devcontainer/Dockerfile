FROM ghcr.io/prefix-dev/pixi:0.71.0-noble

# Reuse the nightly build's Pixi base image, but keep the devcontainer lean:
# the repository is bind-mounted at runtime and Dagster falls back to its
# default SQLite instance storage under DAGSTER_HOME.
USER root

SHELL ["/bin/bash", "-exo", "pipefail", "-c"]

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        ca-certificates \
        gnupg \
        software-properties-common && \
    add-apt-repository ppa:git-core/ppa && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
        git \
        openssh-client \
        sudo \
        bubblewrap \
        socat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo "ubuntu ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu && \
    chmod 0440 /etc/sudoers.d/ubuntu && \
    chmod u+s /usr/bin/bwrap && \
    mkdir -p /workspaces && \
    chown -R ubuntu:ubuntu /workspaces

USER ubuntu
WORKDIR /workspaces


# Activate the Pixi shell automatically in interactive bash sessions once the
# environment has been installed by the post-create hook.
# hadolint ignore=SC2016
RUN echo 'eval "$(pixi shell-hook --shell bash -e dev 2>/dev/null || true)"' >> ~/.bashrc && \
    pixi global install taplo=0.9.3 && \
    echo 'export PATH="$HOME/.pixi/bin:$PATH"' >> ~/.bashrc
