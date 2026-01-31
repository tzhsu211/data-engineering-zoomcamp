# data-engineering-zoomcamp
data-engineering-zoomcamp

### build docker
in the folder with "Dockerfile", 
```
docker build -t test:v001 .
```
build up the docker based on the dockerfile with tag -t test and version v01 and finally mention the location '.' 



#### postgres docker:
```
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network pg-network \
  --name pgdatabase \
  postgres:18
```
--> uv run pgcli -h localhost  -p 5432 -u root -d ny_taxi

```
uv run ingest_data.py \
--year=2021 \
--month=1 \
--target-table=yellow_taxi_data_2021_1

```

* and the order of the docker command line matters. 
all -e, -v, network need to be placed before tag or it will be seem as the arg of the image.
```
docker run -it --rm \
--network pg-network \
test:v01 \
--pg-host=pgdatabase \
--target-table=yellow_taxi_trips_2021_1 \
--month=1 \
--year=2021
```

```
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-v pgadmin_data:/var/lib/pgadmin \
-p 8085:80 \
--network pg-network \
--name pgadmin \
dpage/pgadmin4
```

