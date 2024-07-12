docker exec -it pudl-superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

docker exec -it pudl-superset superset db upgrade
docker exec -it pudl-superset superset init

docker exec -it pudl-superset superset fab import-roles --path /app/roles.json
