FROM apache/superset:7bb7fc0
# Switching to root to install the required packages
USER root

COPY --chown=superset superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

RUN pip install --no-cache-dir duckdb==1.0.0 duckdb-engine==0.13.0 psycopg2-binary==2.9.9 Authlib==1.3.1
# Switching back to using the `superset` user
USER superset
