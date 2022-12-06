#!/bin/sh
# A script to compile a Dockerized data release based on PUDL nightly build outputs.

# Positional arguments:

# First command line argument is the PUDL nightly build tag / ref. This indicates what
# build outputs to use. E.g. "dev" or "v2022.11.30"
PUDL_REF=$1

# Docker tag to use in the archive, e.g. "latest" or "2022.11.30". Will be used to
# pull the docker image using catalystcoop/pudl-jupyter:$DOCKER_TAG
DOCKER_TAG=$2

# Path to a local directory where the archive will be assembled. Should be in a place
# with at least 20GB of disk space.
# E.g. "/tmp/pudl-v2022.11.30"
RELEASE_DIR=$3

# Construct the GCS URL:
GCS_ROOT="gs://intake.catalyst.coop"
GCS_URL="$GCS_ROOT/$PUDL_REF"

# Construct the Docker image name
DOCKER_REPO="catalystcoop"
DOCKER_NAME="pudl-jupyter"
DOCKER_IMAGE="$DOCKER_REPO/$DOCKER_NAME:$DOCKER_TAG"

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
docker pull $DOCKER_IMAGE
# Freeze the version of the Docker container:
cat $RELEASE_DIR/docker-compose.yml | sed -e "s/$DOCKER_NAME:latest/$DOCKER_NAME:$DOCKER_TAG/" > $RELEASE_DIR/new-docker-compose.yml
mv $RELEASE_DIR/new-docker-compose.yml $RELEASE_DIR/docker-compose.yml
# Set up a skeleton PUDL environment in the release dir:
pudl_setup $RELEASE_DIR/pudl_data

# These are probably outdated now... see if they fail.
rm -rf $RELEASE_DIR/pudl_data/environment.yml
rm -rf $RELEASE_DIR/pudl_data/notebook
rm -rf $RELEASE_DIR/pudl_data/settings

# Copy over all of the pre-processed data
echo "Copying SQLite databases..."
mkdir -p $RELEASE_DIR/pudl_data/sqlite/
gsutil -m cp "$GCS_URL/*.sqlite" "$GCS_URL/ferc*_xbrl_*.json" $RELEASE_DIR/pudl_data/sqlite/

echo "Copying Parquet datasets..."
mkdir -p $RELEASE_DIR/pudl_data/parquet/epacems
gsutil -m cp -r "$GCS_URL/hourly_emissions_epacems/*" $RELEASE_DIR/pudl_data/parquet/epacems

# Save the Docker image as a tarball so it can be archived with the data:
echo "Saving Docker image: $DOCKER_IMAGE"
docker save $DOCKER_IMAGE -o $RELEASE_DIR/pudl-jupyter.tar

# List the high-level contents of the archive so we can see what it contains:
echo "Archive contents:"
find $RELEASE_DIR -maxdepth 3

# Create the archive
echo "Creating the archive tarball..."
tar -czf $RELEASE_DIR.tgz $RELEASE_DIR

echo "Finished:" `date`
