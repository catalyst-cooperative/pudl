# hadolint ignore=DL3006
FROM apache/superset
# hadolint ignore=DL3002
USER root

COPY --chown=superset superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# add to requirements file
COPY --chown=superset requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt && \
    rm /app/requirements.txt
# Switching back to using the `superset` user
USER superset
