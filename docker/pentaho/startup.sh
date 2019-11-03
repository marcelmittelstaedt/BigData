#!/bin/bash
# https://docs.docker.com/config/containers/multi-service_container/

# Setup PostgreSQL and Init Airflow 
CONTAINER_ALREADY_INITIALIZED="CONTAINER_ALREADY_INITIALIZED"
if [ ! -e $CONTAINER_ALREADY_INITIALIZED ]; then
    touch $CONTAINER_ALREADY_INITIALIZED
    echo "First start of container."
   	
	# Initialize Pentaho
        echo "Initialize Pentaho."

	# Pull Code from Git
	sudo -u pentaho -H sh -c "git clone https://github.com/marcelmittelstaedt/BigData.git /home/pentaho/BigData"

	# Link Directories to Git Repo files
	sudo -u pentaho -H sh -c "ln -s /home/pentaho/BigData/exercises/winter_semester_2019-2020/06_pentaho/pdi_jobs/ /home/pentaho/pdi_jobs"

else
    echo "Not first start of Container, no Pentaho setup necessary."
fi

echo "Container Startup finished."


while sleep 60; do
	#ps aux | grep "hadoop" | grep -q -v grep
  	#PROCESS_1_STATUS=$?  
	#if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    	#	echo "One of the processes has already exited."
    	#	exit 1
  	#fi
	sleep 1
done
