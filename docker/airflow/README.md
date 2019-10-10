# Airflow Base Docker Image
This folder contains Dockerfile, all necessary Airflow and PostgreSQL config files  for a docker container running Airflow 1.10.5 on PostgreSQL 10.10 as Metadata Store. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/airflow

## Build/Pull and Run Image:

Build Airflow Base Image:
```
docker build . -t marcelmittelstaedt/airflow:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/airflow
```

Run Image:
```
docker run -dit --name airflow_container -p 8080:8080 marcelmittelstaedt/airflow:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop airflow_container
```

Start Container:
```
docker start airflow_container
```

# Start Airflow and PostgreSQL within Container:

PostgreSQL and Airflow starts automatically on each start/restart of conatainer.

Airflow UI: Airflow UI: http://X.X.X.X:8080


