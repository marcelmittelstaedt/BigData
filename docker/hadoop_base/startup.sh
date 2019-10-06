#!/bin/bash
# https://docs.docker.com/config/containers/multi-service_container/

# Start SSH
service ssh restart
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start SSH for Hadoop: $status"
  exit $status
fi

# Fromat HDFS
CONTAINER_ALREADY_INITIALIZED="CONTAINER_ALREADY_INITIALIZED"
if [ ! -e $CONTAINER_ALREADY_INITIALIZED ]; then
    touch $CONTAINER_ALREADY_INITIALIZED
    echo "First start of container, will format HDFS."
   
	sudo -u hadoop -H sh -c "echo Switched to User:; whoami; cd; /home/hadoop/hadoop/bin/hdfs namenode -format"
	status=$?
	if [ $status -ne 0 ]; then
  	  echo "Failed to format HDFS: $status"
  	  exit $status
	fi
else
    echo "Not first start of Container, no HDFS format necessary."
fi

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
