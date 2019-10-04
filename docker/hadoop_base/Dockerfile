FROM ubuntu:18.04

MAINTAINER Marcel Mittelstaedt <marcel.mittelstaedt@datawhizz.io>

# Install All Necessary APT Packages
RUN apt-get update 
RUN apt-get install -y openjdk-8-jdk sudo ssh pdsh openssh-client openssh-server vim curl htop && apt-get clean

# Add Hadoop User
RUN useradd -ms /bin/bash hadoop

# Switch To Hadoop User
USER hadoop
WORKDIR /home/hadoop

# Create SSH Files for Hadoop
RUN mkdir /home/hadoop/.ssh
RUN ssh-keygen -t rsa -N "" -f /home/hadoop/.ssh/id_rsa
RUN cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys
RUN chmod 0600 /home/hadoop/.ssh/authorized_keys

# Download and Install Hadoop
RUN wget https://www-eu.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz
RUN tar -xzf hadoop-3.1.2.tar.gz
RUN rm hadoop-3.1.2.tar.gz
RUN mv hadoop-3.1.2 hadoop

# Install Config Files
COPY .bashrc .bashrc
COPY hadoop-env.sh hadoop/etc/hadoop/hadoop-env.sh
COPY core-site.xml hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml hadoop/etc/hadoop/mapred-site.xml
COPY yarn-site.xml hadoop/etc/hadoop/yarn-site.xml

# Switch back to Root User
USER root
WORKDIR /
ENTRYPOINT service ssh restart && bash

# Expose Ports
# Hadoop Task Tracker Web-Interface
EXPOSE 8088  

# Hadoop Name Node Web-Interface 
EXPOSE 9870




