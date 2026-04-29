#! /bin/bash
# This script is called by the Dockerfile in this directory, and is used to clean up
# old Docker images on the VM, which otherwise accumulate and take up disk space.
# Delete old catalystcoop/pudl-etl images
docker rmi -f "$(docker images -q catalystcoop/pudl-etl)"
