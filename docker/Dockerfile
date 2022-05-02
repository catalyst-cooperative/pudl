FROM condaforge/mambaforge:4.12.0-0

# Create a non-root user inside the container
RUN useradd -Ums /bin/bash catalyst

# Copy the cloned pudl repository into the user's home directory
COPY --chown=catalyst:catalyst . /home/catalyst/pudl
# We need information from .git to get version with setuptools_scm
#COPY --chown=catalyst:catalyst .git/ /home/catalyst/pudl/.git/

# Switch to being the catalyst user and go into the copied repo
USER catalyst
WORKDIR /home/catalyst/pudl

# Create a conda environment based on the specification in the repo
RUN --mount=type=bind,source=.git,target=/home/catalyst/pudl/.git \
    mamba create --name pudl-etl --yes --file docker/requirements-pudl-etl.txt && \
    conda clean -afy && \
    ls -la && \
    find .git -type f && \
    conda run --name pudl-etl pip install --no-cache-dir -e './[dev,doc,test]'

# Run the unit tests:
CMD ["conda", "run", "--name", "pudl-etl", "pytest", "test/unit"]
