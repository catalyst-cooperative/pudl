#! /bin/bash
# Delete old catalystcoop/pudl-etl images
docker rmi -f $(docker images -q catalystcoop/pudl-etl)
