# Hive and Hadoop Base Docker Image
This folder contains Dockerfile, all necessary Hadoop FS, Yarn and Hive config files and docker Entrypoint startup script for a docker container running a pseudo-distributed single-node Hadoop Cluster including Hive. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/hive_base

## Build/Pull and Run Image:

Build Hadoop Base Image:
```
docker build . -t marcelmittelstaedt/hive_base:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/hive_base
```

Run Image:
```
docker run -dit --name hive_base_container -p 8088:8088 -p 9870:9870 marcelmittelstaedt/hive_base:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop hive_base_container
```

Start Container:
```
docker start hive_base_container
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

Start Hive CLI:
```
hive
```

Stop Hadoop FS and Yarn:
```
stop-all.sh
```
