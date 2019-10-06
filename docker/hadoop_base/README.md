Build Hadoop Base Image:
```
docker build . -t marcelmittelstaedt/hadoop_base:latest
```

Run Image:
```
sudo docker run -dit --name hadoop_base_container -p 8088:8088 -p 9870:9870 marcelmittelstaedt/hadoop_base:latest
```

Stop Container:
```
docker stop hadoop_base_container
```

Start Container:
```
docker start hadoop_container
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
