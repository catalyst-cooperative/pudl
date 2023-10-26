covargs := --append --source=src/pudl
pytest_args := --durations 0 --cov-append --cov=src/pudl --cov-report=xml
coverage_erase := coverage erase
coverage_report := coverage report --sort=cover
etl_fast_yml := src/pudl/package_data/settings/etl_fast.yml
etl_full_yml := src/pudl/package_data/settings/etl_full.yml
pip_install_pudl := pip install --no-deps --editable ./

docs-clean:
	rm -rf docs/_build

docs-build: docs-clean
	doc8 docs/ README.rst
	sphinx-build -W -b html docs docs/_build/html

pytest-unit:
	pytest --doctest-modules src/pudl test/unit

pytest-integration:
	pytest test/integration

pytest-jupyter:
	pytest --live-dbs test/integration/jupyter_notebooks_test.py

pytest-minmax-rows:
	pytest --live-dbs \
		test/validate/epacamd_eia_test.py::test_minmax_rows \
		test/validate/ferc1_test.py::test_minmax_rows \
		test/validate/eia_test.py::test_minmax_rows \
		test/validate/mcoe_test.py::test_minmax_rows_mcoe

pytest-validate:
	pytest --live-dbs test/validate

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
	pytest ${pytest_args} --gcs-cache-path=gs://zenodo-cache.catalyst.coop test/integration
	${coverage_report}

# Run the full ETL, generating new FERC & PUDL SQLite DBs and EPA CEMS Parquet files.
# Then run the full integration tests and data validations on all years of data.
# NOTE: This will clobber your existing databases and takes hours to run!!!
nuke: docs-clean
	${coverage_erase}
	doc8 docs/ README.rst
	coverage run ${covargs} -- ${CONDA_PREFIX}/bin/sphinx-build -W -b html docs docs/_build/html
	pytest ${pytest_args} --doctest-modules src/pudl test/unit
	pytest ${pytest_args} --etl-settings ${etl_fast_yml} test/integration
	rm -f tox-nuke.log
	coverage run ${covargs} -- src/pudl/convert/ferc_to_sqlite.py --logfile tox-nuke.log --clobber ${etl_full_yml}
	coverage run ${covargs} -- src/pudl/cli/etl.py --logfile tox-nuke.log --clobber ${etl_full_yml}
	pytest ${pytest_args} --live-dbs --etl-settings ${etl_full_yml} test/integration
	pytest ${pytest_args} --live-dbs test/validate
	${coverage_report}

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
