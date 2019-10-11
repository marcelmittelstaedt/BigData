#!/bin/bash

SCRIPTNAME="wget_file.sh"
REMOTE_URL=$1
REMOTE_FILENAME=$2
LOCAL_DIRECTORY=$3
LOCAL_FILENAME=$4  
WGET_LOGFILE=$5


function print_script_usage {
	echo "[`date +'%F-%T'`][$SCRIPTNAME] $1"
	echo "[`date +'%F-%T'`][$SCRIPTNAME] Run script like:"
	echo "[`date +'%F-%T'`][$SCRIPTNAME] ./wget_file.sh REMOTE_URL REMOTE_FILENAME LOCAL_DIRECTORY LOCAL_FILENAME WGET_LOGFILE"
	echo "[`date +'%F-%T'`][$SCRIPTNAME] For instance:"
	echo "[`date +'%F-%T'`][$SCRIPTNAME] ./wget_file.sh https://datasets.imdbws.com/ name.basics.tsv.gz /home/hadoop/pdi/download name.basics.tsv.gz /home/hadoop/pdi/log/wget.log"
	exit 1
}

function print_error_message {
	echo "[`date +'%F-%T'`][$SCRIPTNAME] ERROR: $1"
	exit 1
}

function print_info_message {
	echo "[`date +'%F-%T'`][$SCRIPTNAME] INFO: $1"
}

# Check Input Parameters
if [ -z "$REMOTE_URL" ] || [ -z "$REMOTE_FILENAME" ] || [ -z "$LOCAL_DIRECTORY" ] || [ -z "$LOCAL_FILENAME" ] || [ -z "$WGET_LOGFILE" ]; then 
	print_script_usage "Missing parameters!"
fi

# Check Input Parameters
if [ "$#" -ne 5 ]; then
	print_script_usage "Too many parameters!"
fi

# Check $LOCAL_DIRECTORY
if [ ! -d "$LOCAL_DIRECTORY" ]; then
  	print_error_message "Directory "$LOCAL_DIRECTORY" does not exist!"
fi

#Check if $REMOTE_URL is reachable
curl --output /dev/null --silent --head --fail $REMOTE_URL
if [ $? -ne 0 ]; then
	print_error_message "REMOTE_URL not reachable: "$REMOTE_URL""
fi

# Create WGET Parameters
WGET_URL="$REMOTE_URL/$REMOTE_FILENAME"
WGET_LOCAL="$LOCAL_DIRECTORY/$LOCAL_FILENAME"

print_info_message "Downloading File: $WGET_URL To $WGET_LOCAL"

# Execute WGET Command
wget $WGET_URL -O $WGET_LOCAL 2>>$WGET_LOGFILE
if [ $? -ne 0 ]; then
	print_error_message "Execution of wget failed: wget "$WGET_URL" -O "$WGET_LOCAL""
fi

print_info_message "Script "$SCRIPTNAME" finished successfully."
exit 0
