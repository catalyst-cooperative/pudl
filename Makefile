covargs := --append --source=src/pudl
gcs_cache_path := --gcs-cache-path=gs://zenodo-cache.catalyst.coop
pytest_covargs := --cov-append --cov=src/pudl --cov-report=xml
coverage_report := coverage report --sort=cover
pytest_args := --durations 20 ${pytest_covargs} ${gcs_cache_path}
etl_fast_yml := src/pudl/package_data/settings/etl_fast.yml
etl_full_yml := src/pudl/package_data/settings/etl_full.yml
pip_install_pudl := pip install --no-deps --editable ./

########################################################################################
# Start up the Dagster UI
########################################################################################
.PHONY: dagster
dagster:
	dagster dev -m pudl.etl -m pudl.ferc_to_sqlite

########################################################################################
# Conda lockfile generation and environment creation
########################################################################################
ifdef GITHUB_ACTION
  mamba := micromamba
else
  mamba := mamba
endif

# Tell make to look in the environments directory for targets and sources.
VPATH = environments

# Remove pre-existing conda lockfile and rendered environment files
.PHONY: conda-clean
conda-clean:
	rm -f environments/conda-*lock.yml

# Regenerate the conda lockfile and render platform specific conda environments.
conda-lock.yml: pyproject.toml
	conda-lock \
		--${mamba} \
		--file=pyproject.toml \
		--lockfile=environments/conda-lock.yml
	cd environments && conda-lock render \
		--kind env \
		--dev-dependencies \
		--extras docs \
		--extras datasette \
		conda-lock.yml
	prettier --write environments/*.yml

# Create the pudl-dev conda environment based on the universal lockfile
.PHONY: pudl-dev
pudl-dev: conda-lock.yml
	conda-lock install \
		--name pudl-dev \
		--${mamba} \
		--dev \
		--extras docs \
		--extras datasette \
		environments/conda-lock.yml

.PHONY: install-pudl
install-pudl: pudl-dev
	${mamba} run --name pudl-dev pip install --no-deps --editable .


########################################################################################
# Build documentation (for local use)
########################################################################################
.PHONY: docs-clean
docs-clean:
	rm -rf docs/_build
	rm -f coverage.xml

.PHONY: docs-build
docs-build: docs-clean
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	coverage xml

########################################################################################
# Common pytest cases
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

.PHONY: pytest-coverage
pytest-coverage: coverage-erase docs-build pytest-unit pytest-integration
	${coverage_report}

.PHONY: pytest-integration-full
pytest-integration-full:
	pytest ${pytest_args} --etl-settings ${etl_full_yml} test/integration

.PHONY: pytest-validate
pytest-validate:
	pytest --live-dbs test/validate
	pudl_check_fks

# Extract all FERC DBF and XBRL data to SQLite.
# NOTE: This will clobber your existing databases.
.PHONY: ferc-to-sqlite
ferc-to-sqlite:
	coverage run ${covargs} -- \
		src/pudl/ferc_to_sqlite/cli.py \
		--clobber \
		${gcs_cache_path} \
		${etl_full_yml}

# Run the full PUDL ETL
# NOTE: This will clobber your existing databases.
.PHONY: pudl-etl-full
pudl-etl-full: ferc-to-sqlite
	coverage run ${covargs} -- \
		src/pudl/cli/etl.py \
		--clobber \
		${gcs_cache_path} \
		${etl_full_yml}

# Run the full ETL, generating new FERC & PUDL SQLite DBs and EPA CEMS Parquet files.
# Then run the full integration tests and data validations on all years of data.
# NOTE: This will clobber your existing databases and takes hours to run!!!
# Backgrounding the data validation and integration tests and using wait allows them to
# run in parallel.
.PHONY: nuke
nuke: coverage-erase docs-build pytest-unit pytest-integration pudl-etl-full
	pudl_check_fks
	pytest ${pytest_args} --live-dbs --etl-settings ${etl_full_yml} test/integration & \
	pytest ${pytest_args} --live-dbs test/validate & \
	wait
	${coverage_report}

########################################################################################
# Some miscellaneous test cases
########################################################################################

# Check that designated Jupyter notebooks can be run against the current DB
.PHONY: pytest-jupyter
pytest-jupyter:
	pytest --live-dbs test/integration/jupyter_notebooks_test.py

# Compare actual and expected number of rows in many tables:
.PHONY: pytest-minmax-rows
pytest-minmax-rows:
	pytest --live-dbs test/validate -k minmax_rows

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
