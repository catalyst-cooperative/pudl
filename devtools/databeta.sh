#!/bin/sh
# A script to compile a Dockerized data release based on a user's local PUDL
# data environment.

# Name of the directory to create the data release archive in
RELEASE_DIR=pudl-v0.5.0-2021-11-14
# The PUDL working directory where we'll find the data to archive:
PUDL_IN=$HOME/code/catalyst/pudl-work
# Reference to an existing Docker image to pull
DOCKER_TAG="2021.11.14"

echo "Started:" `date`
# Start with a clean slate:
rm -rf $RELEASE_DIR
mkdir -p $RELEASE_DIR
# The release container / environment is based on the pudl-examples repo:
git clone --depth 1 git@github.com:catalyst-cooperative/pudl-examples.git $RELEASE_DIR
rm -rf $RELEASE_DIR/.git*
# These directories are where the data will go. They're integrated with the
# Docker container that's defined in the pudl-examples repo:
mkdir -p $RELEASE_DIR/pudl_data
mkdir -p $RELEASE_DIR/user_data

# Make sure we have the specified version of the Docker container:
docker pull catalystcoop/pudl-jupyter:$DOCKER_TAG
# Freeze the version of the Docker container:
cat $RELEASE_DIR/docker-compose.yml | sed -e "s/pudl-jupyter:latest/pudl-jupyter:$DOCKER_TAG/" > $RELEASE_DIR/new-docker-compose.yml
mv $RELEASE_DIR/new-docker-compose.yml $RELEASE_DIR/docker-compose.yml
# Set up a skeleton PUDL environment in the release dir:
pudl_setup $RELEASE_DIR/pudl_data

# These are probably outdated now... see if they fail.
rm -rf $RELEASE_DIR/pudl_data/environment.yml
rm -rf $RELEASE_DIR/pudl_data/notebook

# Copy over all of the pre-processed data
echo "Copying SQLite databases..."
cp -v $PUDL_IN/sqlite/ferc1.sqlite $RELEASE_DIR/pudl_data/sqlite/
cp -v $PUDL_IN/sqlite/pudl.sqlite $RELEASE_DIR/pudl_data/sqlite/
cp -v $PUDL_IN/sqlite/censusdp1tract.sqlite $RELEASE_DIR/pudl_data/sqlite/
echo "Copying Parquet datasets..."
cp -r $PUDL_IN/parquet/epacems $RELEASE_DIR/pudl_data/parquet/

# Save the Docker image as a tarball so it can be archived with the data:
docker save catalystcoop/pudl-jupyter:$DOCKER_TAG -o $RELEASE_DIR/pudl-jupyter.tar

# List the high-level contents of the archive so we can see what it contains:
find $RELEASE_DIR -maxdepth 3

# Create the archive
tar -czf $RELEASE_DIR.tgz $RELEASE_DIR

echo "Finished:" `date`
