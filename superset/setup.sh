# Description: This script was used to setup the superset instance for the first time.

# Create and admin user
docker exec -it pudl-superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

# Initialize the database and run migrations
docker exec -it pudl-superset superset db upgrade
docker exec -it pudl-superset superset init

# Import custom roles that include a new role that combines permissions of Gamma and sql_user roles
# docker exec -it pudl-superset superset fab import-roles --path /app/roles.json
