#!/bin/sh
# A script to compile a Dockerized data release:
DATABETA=databeta-2021-03-30
PUDL_IN=$HOME/code/catalyst/pudl-work
DOCKER_TAG="2021.03.27"

date
rm -rf $DATABETA
mkdir -p $DATABETA
git clone --depth 1 git@github.com:catalyst-cooperative/pudl-examples.git $DATABETA
rm -rf $DATABETA/.git*
mkdir -p $DATABETA/pudl_data
mkdir -p $DATABETA/user_data
cat $DATABETA/docker-compose.yml | sed -e "s/pudl-jupyter:latest/pudl-jupyter:$DOCKER_TAG/" > $DATABETA/new-docker-compose.yml
mv $DATABETA/new-docker-compose.yml $DATABETA/docker-compose.yml
pudl_setup $DATABETA/pudl_data
rm -rf $DATABETA/pudl_data/environment.yml
rm -rf $DATABETA/pudl_data/notebook
cp -v $PUDL_IN/sqlite/ferc1.sqlite $DATABETA/pudl_data/sqlite/
cp -v $PUDL_IN/sqlite/pudl.sqlite $DATABETA/pudl_data/sqlite/
cp -v $PUDL_IN/sqlite/censusdp1tract.sqlite $DATABETA/pudl_data/sqlite/
echo "Copying EPA CEMS..."
cp -r $PUDL_IN/parquet/epacems $DATABETA/pudl_data/parquet/
docker save catalystcoop/pudl-jupyter:$DOCKER_TAG -o $DATABETA/pudl-jupyter.tar
find $DATABETA -maxdepth 3
tar -czf $DATABETA.tgz $DATABETA
date
