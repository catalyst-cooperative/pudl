FROM condaforge/mambaforge:latest AS hello-docker

# Create a non-root user inside the container
RUN useradd -ms /bin/bash catalyst

# Outside the container we've already cloned the repository. Copy it to /pudl
COPY . /home/catalyst/pudl

# Create a conda environment based on the specification in the repo
RUN mamba create --yes --file /home/catalyst/pudl/docker/requirements-pudl-etl.txt --name pudl-etl && conda clean -afy

# Install the catalystcoop.pudl package from /pudl using pip
RUN conda run --name pudl-etl python -m pip install '/home/catalyst/pudl[dev,doc,test]'

# Run the unit tests:
WORKDIR /home/catalyst/pudl
CMD ["conda", "run", "--name", "pudl-etl", "pytest", "test/unit"]
