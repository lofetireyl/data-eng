# How to start the docker pipeline

Navigate to the root folder and execute 
  ```bash
  docker compose up -d
  ```
If some of the services are not healthy for any reason, try starting up the services individually like this:

BUILD ORDER:
  ```bash
docker compose up -d --force-recreate --no-deps zookeeper kafka postgres adminer

docker compose up -d --force-recreate --no-deps producer

docker compose logs -f producer

docker compose up -d --force-recreate --no-deps spark streamlit

docker compose logs -f spark

docker compose stop producer

docker compose up -d producer
  ```

If nothing works try to clean previous installed volumes and execute from the root folder (be careful with these commands, as they clean all containers that are NOT running at the oment of execution):


  ```bash
docker compose down -v
docker compose up -d
  ```
  OR
  ```bash
docker compose down
docker builder prune -a
docker system prune -a --volumes
  ```