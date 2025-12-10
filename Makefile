gcs_cache_path := --gcs-cache-path=gs://zenodo-cache.catalyst.coop
covargs := --append
pytest_args := --durations 20 ${gcs_cache_path} --cov-fail-under=0
etl_fast_yml := src/pudl/package_data/settings/etl_fast.yml
etl_full_yml := src/pudl/package_data/settings/etl_full.yml
current_branch := $(shell git rev-parse --abbrev-ref HEAD)
current_tag := $(shell git tag --points-at HEAD)
# docs_install_version will be:
#  - latest if we're on main
#  - nightly if we're on nightly
#  - stable if we're on stable
#  - [tag] if we're on a tag like v20*
# & otherwise empty
docs_install_version := $(subst main,latest,$(strip $(filter main nightly stable,${current_branch}) $(filter v20%, ${current_tag})))

# We use mamba locally, but micromamba in CI, so choose the right binary:
ifdef GITHUB_ACTIONS
  mamba := micromamba
else
  mamba := mamba
endif

# Tell make to look in the environments and output directory for targets and sources.
VPATH = environments:${PUDL_OUTPUT}

.PHONY: jlab
jlab:
	jupyter lab --no-browser

########################################################################################
# Conda lockfile generation and environment creation
########################################################################################

# Remove pre-existing conda lockfile and rendered environment files
.PHONY: conda-clean
conda-clean:
	rm -f environments/conda-*lock.yml

# Regenerate the conda lockfile and render platform specific conda environments.
conda-lock.yml: pyproject.toml
	${mamba} run --name base ${mamba} install --quiet --yes "conda-lock>=3" prettier
	${mamba} run --name base conda-lock \
		--${mamba} \
		--file=pyproject.toml \
		--lockfile=environments/conda-lock.yml
	cd environments && ${mamba} run --name base conda-lock render \
		--kind env \
		--dev-dependencies \
		conda-lock.yml
	${mamba} run --name base prettier --write environments/*.yml

# Create the pudl-dev conda environment based on the universal lockfile
.PHONY: pudl-dev
pudl-dev:
	${mamba} run --name base ${mamba} install --quiet --yes "conda-lock>=3"
# Only attempt to remove the pudl-dev environment if it already exists.
	if ${mamba} env list | grep -q pudl-dev; then \
		${mamba} env remove --quiet --yes --name pudl-dev; \
	fi
	${mamba} run --name base conda-lock install \
		--name pudl-dev \
		--${mamba} \
		--dev environments/conda-lock.yml
	echo "To activate the fresh environment run: mamba activate pudl-dev"

.PHONY: install-pudl
install-pudl: pudl-dev
	${mamba} run --name pudl-dev pip install --no-cache-dir --no-deps --editable .
	echo "To activate the fresh environment run: mamba activate pudl-dev"

########################################################################################
# Build documentation for local use or testing
########################################################################################
.PHONY: docs-clean
docs-clean:
	rm -rf docs/_build
	rm -rf docs/autoapi
	rm -f docs/data_dictionaries/pudl_db.rst
	rm -f docs/data_dictionaries/codes_and_labels.rst
	rm -rf docs/data_dictionaries/code_csvs
	rm -f docs/data_sources/census*.rst
	rm -f docs/data_sources/eia*.rst
	rm -f docs/data_sources/epacems*.rst
	rm -f docs/data_sources/epacamd*.rst
	rm -f docs/data_sources/ferc*.rst
	rm -f docs/data_sources/gridpathratoolkit*.rst
	rm -f docs/data_sources/nrelatb*.rst
	rm -f docs/data_sources/phmsagas*.rst
	rm -f docs/data_sources/sec10k*.rst
	rm -f docs/data_sources/vcerare*.rst

# Note that there's some PUDL code which only gets run when we generate the docs, so
# we want to generate coverage from the docs build.
.PHONY: docs-build
docs-build: docs-clean
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build --jobs auto -v -W -b html docs docs/_build/html

.PHONY: docs-install
docs-install: docs-build
	mkdir -p docs/_install/en
	cp docs/latest_redirect.html docs/_install/index.html
ifdef docs_install_version
	rm -rf docs/_install/en/${docs_install_version}
	cp -r docs/_build/html docs/_install/en/${docs_install_version}
endif

########################################################################################
# Running the Full ETL
# NOTE: these commands will clobber your existing databases, and may take an hour or
# more to run.
########################################################################################

# Extract all FERC DBF and XBRL data to SQLite.
.PHONY: ferc
ferc:
	rm -f ${PUDL_OUTPUT}/ferc*.sqlite
	rm -f ${PUDL_OUTPUT}/ferc*_xbrl_datapackage.json
	rm -f ${PUDL_OUTPUT}/ferc*_xbrl_taxonomy_metadata.json
	coverage run ${covargs} -- src/pudl/ferc_to_sqlite/cli.py ${gcs_cache_path} ${etl_full_yml}

# Remove the existing PUDL DB if it exists.
# Create a new empty DB using alembic.
# Run the full PUDL ETL.
.PHONY: pudl
pudl:
	rm -f ${PUDL_OUTPUT}/pudl.sqlite
	alembic upgrade head
	coverage run ${covargs} -- src/pudl/etl/cli.py ${gcs_cache_path} ${etl_full_yml}

########################################################################################
# Targets that are coordinated by pytest -- mostly they're actual tests.
########################################################################################
.PHONY: pytest-unit
pytest-unit:
	pytest ${pytest_args} --doctest-modules src/pudl test/unit

.PHONY: pytest-integration
pytest-integration:
	pytest ${pytest_args} --etl-settings ${etl_fast_yml} test/integration

.PHONY: coverage-erase
coverage-erase:
	coverage erase

.PHONY: pytest-ci
pytest-ci: pytest-unit pytest-integration

.PHONY: pytest-coverage
pytest-coverage: coverage-erase docs-build pytest-ci
	coverage report

.PHONY: pytest-integration-full
pytest-integration-full:
	pytest ${pytest_args} -n 4 --no-cov --live-dbs --etl-settings ${etl_full_yml} test/integration

# Run the full ETL, generating new FERC & PUDL SQLite DBs and EPA CEMS Parquet files.
# Then run the full integration tests and data validations on all years of data.
# NOTE: This will clobber your existing databases and takes hours to run!!!
# Backgrounding the data validation and integration tests and using wait allows them to
# run in parallel.
.PHONY: nuke
nuke: coverage-erase docs-build pytest-unit ferc pudl
	pytest ${pytest_args} -n 4 --live-dbs --etl-settings ${etl_full_yml} test/integration
	coverage report

# Check that designated Jupyter notebooks can be run against the current DB
.PHONY: pytest-jupyter
pytest-jupyter:
	pytest --live-dbs test/integration/jupyter_notebooks_test.py

# Build the FERC 1 and PUDL DBs, ignoring foreign key constraints.
# Identify any plant or utility IDs in the DBs that haven't yet been mapped
# NOTE: This probably needs to be turned into a script of some kind not a test.
# In particular, building these DBs without checking FK constraints, in a location
# where they aren't going to clobber existing user DBs
unmapped-ids:
	pytest \
		--save-unmapped-ids \
		--ignore-foreign-key-constraints \
		--etl-settings ${etl_full_yml} \
		test/integration/glue_test.py
