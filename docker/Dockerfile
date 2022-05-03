FROM condaforge/mambaforge:4.12.0-0

# Create a non-root user inside the container
RUN useradd -Ums /bin/bash catalyst

# Copy the cloned pudl repository into the user's home directory
COPY --chown=catalyst:catalyst . /home/catalyst/pudl

# Switch to being the catalyst user and go into the copied repo
USER catalyst
WORKDIR /home/catalyst/pudl

# Create a conda environment based on the specification in the repo
# We need information from .git to get version with setuptools_scm so we mount that
# directory without copying it into the image.
RUN --mount=type=bind,source=.git,target=/home/catalyst/pudl/.git \
        # mamba create can't read environment.yml, and mamba env isn't installed yet,
        # so we create a bare py3.10 env first:
        mamba create --name pudl-etl --yes python=3.10 && \
    # Then we can use mamba env update, which can parse the environment.yml file:
    mamba env update --name pudl-etl --file devtools/ci-environment.yml && \
    conda clean -afy && \
    conda run --name pudl-etl pip install --no-cache-dir -e './[dev,doc,test]' && \
    # Create data input/output directories
    mkdir /home/catalyst/pudl_in && mkdir /home/catalyst/pudl_out && \
    # Run the PUDL setup script so we know where to read and write data
    conda run --name pudl-etl \
        pudl_setup --pudl_in /home/catalyst/pudl_in --pudl_out /home/catalyst/pudl_out

# Run the unit tests:
CMD ["conda", "run", "--name", "pudl-etl", "pytest", "test/unit"]
