# Hadoop, Hive and HiveServer Base Docker Image
This folder contains Dockerfile, all necessary Hadoop FS, Yarn and Hive config files and docker Entrypoint startup script for a docker container running a pseudo-distributed single-node Hadoop Cluster including Hive and HiveServer2. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/hiveserver_base

## Build/Pull and Run Image:

Build Hadoop Base Image:
```
docker build . -t marcelmittelstaedt/hiveserver_base:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/hiveserver_base
```

Run Image:
```
docker run -dit --name hiveserver_base_container -p 8088:8088 -p 9870:9870 -p 10000:10000 marcelmittelstaedt/hiveserver_base:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop hiveserver_base_container
```

Start Container:
```
docker start hiveserver_base_container
```

# Start and Stop Hadoop and Hive within Container:
Switch to hadoop user:
```
sudo su hadoop
cd
```

Start Hadoop FS and Yarn:
```
start-all.sh
```

Start HiveServer2:
```
hiveserver2
```

Connect To HiveServer2 via JDBC (e.g. using DBeaver: https://dbeaver.io/):
```
jdbc:hive2://35.235.41.203:10000/default
```

Stop Hadoop FS and Yarn:
```
stop-all.sh
```
