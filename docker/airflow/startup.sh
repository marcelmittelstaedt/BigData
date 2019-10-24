#!/bin/bash
# https://docs.docker.com/config/containers/multi-service_container/

# Setup PostgreSQL and Init Airflow 
CONTAINER_ALREADY_INITIALIZED="CONTAINER_ALREADY_INITIALIZED"
if [ ! -e $CONTAINER_ALREADY_INITIALIZED ]; then
    touch $CONTAINER_ALREADY_INITIALIZED
    echo "First start of container."
   	
    	# Format HDFS
	echo "Start PostgreSQL."
	sudo service postgresql start
	status=$?
	if [ $status -ne 0 ]; then
  	  echo "Failed to start PostgreSQL: $status"
  	  exit $status
	fi

	# Create PostgreSQL Metadat for Airflow
	sudo -u postgres psql -U postgres -c 'CREATE DATABASE airflow;'
	sudo -u postgres psql -U postgres -c "CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';"
	sudo -u postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow to airflow;"

	# Initialize Airflow
        echo "Initialize Airflow."
        sudo -u airflow -H sh -c "/home/airflow/.local/bin/airflow initdb"
	status=$?
        if [ $status -ne 0 ]; then
          echo "Failed to initialize Airflow: $status"
          exit $status
        fi

else
    echo "Not first start of Container, no PostgreSQL or Airflow setup necessary."
fi

# Start PostgreSQL 
sudo service postgresql restart

# Start Airflow
sudo -u airflow -H sh -c "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/;export JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre; export HADOOP_USER_NAME=hadoop; export SPARK_HOME=/home/airflow/spark; export HADOOP_HOME=/home/airflow/hadoop; export PYSPARK_PYTHON=python3; PATH=$PATH:/home/airflow/.local/bin:/home/airflow/hive/bin:/home/airflow/hadoop:/home/airflow/hadoop/bin:/home/airflow/hadoop/sbin:/usr/lib/jvm/java-8-openjdk-amd64:/usr/lib/jvm/java-8-openjdk-amd64/jre exec /home/airflow/.local/bin/airflow scheduler > /home/airflow/airflow_scheduler.log &"
sudo -u airflow -H sh -c "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/;export JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre; export HADOOP_USER_NAME=hadoop; export SPARK_HOME=/home/airflow/spark; export HADOOP_HOME=/home/airflow/hadoop; export PYSPARK_PYTHON=python3; PATH=$PATH:/home/airflow/.local/bin:/home/airflow/hive/bin:/home/airflow/hadoop:/home/airflow/hadoop/bin:/home/airflow/hadoop/sbin:/usr/lib/jvm/java-8-openjdk-amd64:/usr/lib/jvm/java-8-openjdk-amd64/jre exec /home/airflow/.local/bin/airflow webserver -p 8080 --pid /home/airflow/airflow/airflow-webserver.pid > /home/airflow/airflow_webservice.log &"

# Start Hadoop Cluster
#sudo -u hadoop -H sh -c "echo Switched to User:; whoami; cd; /home/hadoop/hadoop/sbin/start-all.sh"
#status=$?
#if [ $status -ne 0 ]; then
#  echo "Failed to start Hadoop DFS and Yarn: $status"
#  exit $status
#fi

while sleep 60; do
	#ps aux | grep "hadoop" | grep -q -v grep
  	#PROCESS_1_STATUS=$?  
	#if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    	#	echo "One of the processes has already exited."
    	#	exit 1
  	#fi
	sleep 1
done
