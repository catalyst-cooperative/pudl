FROM condaforge/mambaforge:4.12.0-0

# Create a non-root user inside the container
RUN useradd -Ums /bin/bash catalyst

# Copy the cloned pudl repository into the user's home directory
COPY . /home/catalyst/pudl
# We need information from .git to get version with setuptools_scm
COPY .git /home/catalyst/pudl/.git
# This is another way to do it, but I don't understand it yet
#RUN --mount=source=.git,target=.git,type=bind && \
#    pip install --no-cache-dir -e .

# Make sure the user owns everything in their home directory
RUN chown -R catalyst:catalyst /home/catalyst

# Switch to being the catalyst user and go into the copied repo
USER catalyst
WORKDIR /home/catalyst/pudl

# Create a conda environment based on the specification in the repo
RUN mamba create --name pudl-etl --yes --file docker/requirements-pudl-etl.txt && \
    conda clean -afy && \
    ls -la && \
    find .git -type f && \
    conda run --name pudl-etl pip install './[dev,doc,test]'

# Run the unit tests:
CMD ["conda", "run", "--name", "pudl-etl", "pytest", "test/unit"]
