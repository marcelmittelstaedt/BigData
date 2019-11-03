# Airflow Base Docker Image
This folder contains Dockerfile, all necessary Airflow and PostgreSQL config files for a docker container running Airflow 1.10.5 on PostgreSQL 10.10 as Metadata Store and using LocalExecutors. Also added and configured Hadoop, Hive and Spark Binaries, so airflow will be able to execute hadoop, hive and spark commands on other Hadoop/Hive/Spark docker container. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/airflow

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
docker run -dit --name airflow -p 8080:8080 --net bigdatanet marcelmittelstaedt/airflow:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop airflow
```

Start Container:
```
docker start airflow
```

# Start Airflow and PostgreSQL within Container:

PostgreSQL and Airflow starts automatically on each start/restart of container.

Airflow UI: http://X.X.X.X:8080


