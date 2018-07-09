#!/bin/bash

# Copied almost totally from Pandas
# https://github.com/pandas-dev/pandas/blob/master/ci/install_travis.sh

echo
echo "[install_travis]"

home_dir=$(pwd)
echo
echo "[home_dir]: $home_dir"

# install miniconda
MINICONDA_DIR="$HOME/miniconda3"

echo
echo "[Using clean Miniconda install]"

if [ -d "$MINICONDA_DIR" ]; then
    rm -rf "$MINICONDA_DIR"
fi

# install miniconda
if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    time wget http://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -q -O miniconda.sh || exit 1
else
    time wget http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -q -O miniconda.sh || exit 1
fi
time bash miniconda.sh -b -p "$MINICONDA_DIR" || exit 1

echo
echo "[show conda]"
which conda

echo
echo "[update conda]"
conda config --set ssl_verify false || exit 1
conda config --set quiet true --set always_yes true --set changeps1 false || exit 1
conda update -q conda

# Useful for debugging any issues with conda
conda info -a || exit 1
echo "[create env]"

# create our environment
time conda env create -q --file=environment.yml || exit 1
conda activate pudl

# build and install
#echo "[running setup.py develop]"
#python setup.py develop  || exit 1

echo
echo "[show environment]"
conda list

echo
echo "[done]"
exit 0
