# Pentaho Data Integration Base Docker Image
This folder contains Dockerfile, all necessary config files for a docker container running Pentaho Data Integration 8.0. Also added and configured Hadoop, Hive and Spark Binaries, so airflow will be able to execute hadoop and hive commands (beeline) on other Hadoop/Hive/Spark docker container. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/pentaho

## Build/Pull and Run Image:

Build Airflow Base Image:
```
docker build . -t marcelmittelstaedt/pentaho:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/pentaho
```

Run Image:
```
docker run -dit --name pentaho --net bigdatanet marcelmittelstaedt/pentaho:latest 
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop pentaho
```

Start Container:
```
docker start pentaho
```


