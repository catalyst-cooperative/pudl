covargs := --append --source=src/pudl
gcs_cache_path := --gcs-cache-path=gs://zenodo-cache.catalyst.coop
pytest_covargs := --cov-append --cov=src/pudl --cov-report=xml
pytest_args := --durations 0 ${pytest_covargs} ${gcs_cache_path}
coverage_erase := coverage erase
coverage_report := coverage report --sort=cover
etl_fast_yml := src/pudl/package_data/settings/etl_fast.yml
etl_full_yml := src/pudl/package_data/settings/etl_full.yml
pip_install_pudl := pip install --no-deps --editable ./

########################################################################################
# Start up the Dagster UI
########################################################################################
dagster:
	dagster dev -m pudl.etl -m pudl.ferc_to_sqlite

########################################################################################
# Conda lockfile generation
########################################################################################

ifdef GITHUB_ACTION
  mamba := micromamba
else
  mamba := mamba
endif

# Regenerate the conda lockfile and render platform specific conda environments.
conda-lock:
	rm -f environments/conda-*lock.yml
	conda-lock \
		--${mamba} \
		--file=pyproject.toml \
		--lockfile=environments/conda-lock.yml
	(cd environments && conda-lock render \
		--kind env \
		--dev-dependencies \
		--extras docs \
		--extras datasette \
		conda-lock.yml)
	prettier --write environments/*.yml

########################################################################################
# Build documentation (for local use)
########################################################################################
docs-clean:
	rm -rf docs/_build

docs-build: docs-clean
	doc8 docs/ README.rst
	sphinx-build -W -b html docs docs/_build/html

########################################################################################
# Generic pytest commands for local use, without test coverage
########################################################################################
pytest-unit:
	pytest --doctest-modules src/pudl test/unit

pytest-integration:
	pytest test/integration

pytest-validate:
	pytest --live-dbs test/validate
	pudl_check_fks

########################################################################################
# More complex pytest commands for local use that collect test coverage
########################################################################################
# Run unit & integration tests on 1-2 years of data and collect test coverage data.
local-pytest-ci: docs-clean
	${coverage_erase}
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	pytest ${pytest_args} --doctest-modules src/pudl test/unit
	pytest ${pytest_args} --etl-settings ${etl_fast_yml} test/integration
	${coverage_report}

# Run unit & integration tests on ALL years of data and collect test coverage data.
# NOTE: This will take 1+ hours to run and the PUDL DB will not be retained.
local-pytest-ci-all-years: docs-clean
	${coverage_erase}
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	pytest ${pytest_args} --doctest-modules src/pudl test/unit
	pytest ${pytest_args} --etl-settings ${etl_full_yml} test/integration
	${coverage_report}

# Run the full ETL, generating new FERC & PUDL SQLite DBs and EPA CEMS Parquet files.
# Then run the full integration tests and data validations on all years of data.
# NOTE: This will clobber your existing databases and takes hours to run!!!
# Backgrounding the data validation and integration tests and using wait allows them to
# run in parallel.
nuke: docs-clean
	${coverage_erase}
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	pytest ${pytest_args} --doctest-modules src/pudl test/unit
	pytest ${pytest_args} \
		--etl-settings ${etl_fast_yml} \
		test/integration
	rm -f tox-nuke.log
	coverage run ${covargs} -- \
		src/pudl/convert/ferc_to_sqlite.py \
		--logfile tox-nuke.log \
		--clobber \
		${gcs_cache_path} \
		${etl_full_yml}
	coverage run ${covargs} -- \
		src/pudl/cli/etl.py \
		--logfile tox-nuke.log \
		--clobber \
		${gcs_cache_path} \
		${etl_full_yml}
	pudl_check_fks
	pytest ${pytest_args} --live-dbs --etl-settings ${etl_full_yml} test/integration & \
	pytest ${pytest_args} --live-dbs test/validate & \
	wait
	${coverage_report}

########################################################################################
# Some miscellaneous test cases
########################################################################################

# Check that designated Jupyter notebooks can be run against the current DB
pytest-jupyter:
	pytest --live-dbs test/integration/jupyter_notebooks_test.py

# Compare actual and expected number of rows in many tables:
pytest-minmax-rows:
	pytest --live-dbs \
		test/validate/epacamd_eia_test.py::test_minmax_rows \
		test/validate/ferc1_test.py::test_minmax_rows \
		test/validate/eia_test.py::test_minmax_rows \
		test/validate/mcoe_test.py::test_minmax_rows_mcoe

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

########################################################################################
# The github- prefixed targets are meant to be run by GitHub Actions
########################################################################################

github-docs-build: docs-clean
	${pip_install_pudl}
	${coverage_erase}
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	${coverage_report}
	coverage xml

github-pytest-unit:
	${pip_install_pudl}
	${coverage_erase}
	pytest ${pytest_args} --doctest-modules src/pudl test/unit
	${coverage_report}

github-pytest-integration:
	${pip_install_pudl}
	${coverage_erase}
	pytest ${pytest_args} test/integration
	${coverage_report}
