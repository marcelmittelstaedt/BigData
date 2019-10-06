# Hadoop Base Docker Image
This folder contains Dockerfile and all necessary Hadoop FS and Yarn config files for a docker container running a pseudo-distributed single-node Hadoop Cluster. You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/hadoop_base/tags

## Build/Pull and Run Image:

Build Hadoop Base Image:
```
docker build . -t marcelmittelstaedt/hadoop_base:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/hadoop_base
```

Run Image:
```
docker run -dit --name hadoop_base_container -p 8088:8088 -p 9870:9870 marcelmittelstaedt/hadoop_base:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop hadoop_base_container
```

Start Container:
```
docker start hadoop_base_container
```

# Start and Stop Hadoop within Container:
Switch to hadoop user:
```
sudo su hadoop
cd
```

Start Hadoop FS and Yarn:
```
sudo su hadoop
cd
start-all.sh
```

Stop Hadoop FS and Yarn:
```
stop-all.sh
```
