FROM python:3.11.0-slim-bullseye

ENV DATABASES 'pudl.sqlite ferc1.sqlite'
ENV DATASETTE_PORT 8080

RUN apt-get update \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && pip install -U \
    datasette==0.64.8 \
    datasette-cluster-map==0.18.2 \
    datasette-vega==0.6.2 \
    datasette-block-robots==1.1 \
    --no-cache-dir

COPY . /app
WORKDIR /app

EXPOSE ${DATASETTE_PORT}
ENV DATASETTE_SECRET '{datasette_secret}'
CMD ["./run.sh"]
