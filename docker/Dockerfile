FROM condaforge/mambaforge:4.12.0-0 AS hello-docker

# Create a non-root user inside the container
RUN useradd -ms /bin/bash catalyst
USER catalyst
WORKDIR /home/catalyst/pudl

# Outside the container we've already cloned the repository. Copy it to /pudl
COPY . /home/catalyst/pudl

# Create a conda environment based on the specification in the repo
RUN mamba create --name pudl-etl --file ./docker/requirements-pudl-etl.txt --yes && \
    conda clean -afy && \
    mamba list && \
    ls -la && \
    conda run --name pudl-etl python -m pip install '/home/catalyst/pudl[dev,doc,test]'

# Run the unit tests:
CMD ["conda", "run", "--name", "pudl-etl", "pytest", "test/unit"]
