# Spark Base Docker Image
This folder contains Dockerfile, all necessary Hadoop FS, Yarn, Hive and Spark config files and docker Entrypoint startup script for a docker container running a pseudo-distributed single-node Hadoop and Spark Cluster including Jupyter Notebooks (as well as Hive and HiveServer2). You can build it on your own or pull it from https://hub.docker.com/r/marcelmittelstaedt/spark_base

## Build/Pull and Run Image:

Build Hadoop Base Image:
```
docker build . -t marcelmittelstaedt/spark_base:latest
```

...or pull Image:
```
docker pull marcelmittelstaedt/spark_base
```

Run Image:
```
docker run -dit --name spark_base_container -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 10000:10000 -p 4040:4040 -p 8042:8042 -p 8888:8888 marcelmittelstaedt/spark_base:latest
```

# Start and Stop Docker Container:
Stop Container:
```
docker stop spark_base_container
```

Start Container:
```
docker start spark_base_container
```

# Start and Stop Hadoop, Spark and Jupyter within Container:
Switch to hadoop user:
```
sudo su hadoop
cd
```

Start Hadoop FS and Yarn:
```
start-all.sh
```

Start Spark Shell (with Yarn Master):
```
spark-shell --master yarn
```

Start PySpark Shell (with Yarn Master):
```
pyspark --master yarn
```

Start Jupyter Notebook:
```
jupyter notebook
````

Stop Hadoop FS and Yarn:
```
stop-all.sh
```
